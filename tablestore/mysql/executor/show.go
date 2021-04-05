package executor

import (
	"bytes"
	"context"
	"fmt"
	"github.com/pingcap/errors"
	"sort"
	"strconv"
	"strings"

	"github.com/cznic/mathutil"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/charset"
	parser_model "github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/collate"
	"github.com/pingcap/tidb/util/format"
	"github.com/pingcap/tidb/util/stringutil"
	"github.com/zhihu/zetta/pkg/model"
	"github.com/zhihu/zetta/tablestore/infoschema"
	"github.com/zhihu/zetta/tablestore/mysql/expression"
	"github.com/zhihu/zetta/tablestore/mysql/planner"
	"github.com/zhihu/zetta/tablestore/mysql/sctx"
	"github.com/zhihu/zetta/tablestore/table"
)

type ShowExec struct {
	baseExecutor
	Tp     ast.ShowStmtType
	is     infoschema.InfoSchema
	Plan   *planner.LogicalShow
	result *chunk.Chunk
	cursor int

	GlobalScope bool
}

func (s *ShowExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.GrowAndReset(s.maxChunkSize)
	if s.result == nil {
		s.result = NewFirstChunk(s)
		err := s.fetchShowInfo(ctx)
		if err != nil {
			return err
		}
		iter := chunk.NewIterator4Chunk(s.result)
		for colIdx := 0; colIdx < s.Schema().Len(); colIdx++ {
			retType := s.Schema().Columns[colIdx].RetType
			if !types.IsTypeVarchar(retType.Tp) {
				continue
			}
			for row := iter.Begin(); row != iter.End(); row = iter.Next() {
				if valLen := len(row.GetString(colIdx)); retType.Flen < valLen {
					retType.Flen = valLen
				}
			}
		}
	}
	if s.cursor >= s.result.NumRows() {
		return nil
	}
	numCurBatch := mathutil.Min(req.Capacity(), s.result.NumRows()-s.cursor)
	req.Append(s.result, s.cursor, s.cursor+numCurBatch)
	s.cursor += numCurBatch
	return nil
}

func (s *ShowExec) fetchShowInfo(ctx context.Context) error {
	switch s.Tp {
	case ast.ShowCharset:
		return s.fetchShowCharset()
	case ast.ShowCollation:
		return s.fetchShowCollation()
	case ast.ShowDatabases:
		return s.fetchShowDatabases(ctx)
	case ast.ShowTables:
		return s.fetchShowTables(ctx)
	case ast.ShowColumns:
		return s.fetchShowColumns(ctx)
	case ast.ShowCreateTable:
		return s.fetchShowCreateTable(ctx)
	case ast.ShowVariables:
		return s.fetchShowVariables()
	case ast.ShowStatus:
		return s.fetchShowStatus()
	default:
		return errors.New("Unsupported show type")
	}
	return nil
}

func (e *ShowExec) fetchShowStatus() error {
	sessionVars := e.ctx.GetSessionVars()
	statusVars, err := variable.GetStatusVars(sessionVars)
	if err != nil {
		return errors.Trace(err)
	}
	for status, v := range statusVars {
		if e.GlobalScope && v.Scope == variable.ScopeSession {
			continue
		}
		switch v.Value.(type) {
		case []interface{}, nil:
			v.Value = fmt.Sprintf("%v", v.Value)
		}
		value, err := types.ToString(v.Value)
		if err != nil {
			return errors.Trace(err)
		}
		e.appendRow([]interface{}{status, value})
	}
	return nil
}

func (e *ShowExec) fetchShowCollation() error {
	collations := collate.GetSupportedCollations()
	for _, v := range collations {
		isDefault := ""
		if v.IsDefault {
			isDefault = "Yes"
		}
		e.appendRow([]interface{}{
			v.Name,
			v.CharsetName,
			v.ID,
			isDefault,
			"Yes",
			1,
		})
	}
	return nil
}

// fetchShowCharset gets all charset information and fill them into e.rows.
// See http://dev.mysql.com/doc/refman/5.7/en/show-character-set.html
func (e *ShowExec) fetchShowCharset() error {
	descs := charset.GetSupportedCharsets()
	for _, desc := range descs {
		e.appendRow([]interface{}{
			desc.Name,
			desc.Desc,
			desc.DefaultCollation,
			desc.Maxlen,
		})
	}
	return nil
}

func (e *ShowExec) fetchShowVariables() (err error) {
	var (
		value         string
		ok            bool
		sessionVars   = e.ctx.GetSessionVars()
		unreachedVars = make([]string, 0, len(variable.SysVars))
	)
	//e.appendRow([]interface{}{"sql_mode", "STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION"})
	for _, v := range variable.SysVars {
		if !e.GlobalScope {
			// For a session scope variable,
			// 1. try to fetch value from SessionVars.Systems;
			// 2. if this variable is session-only, fetch value from SysVars
			//		otherwise, fetch the value from table `mysql.Global_Variables`.
			value, ok, err = variable.GetSessionOnlySysVars(sessionVars, v.Name)
		} else {
			// If the scope of a system variable is ScopeNone,
			// it's a read-only variable, so we return the default value of it.
			// Otherwise, we have to fetch the values from table `mysql.Global_Variables` for global variable names.
			value, ok, err = variable.GetScopeNoneSystemVar(v.Name)
		}
		if err != nil {
			return errors.Trace(err)
		}
		if !ok {
			unreachedVars = append(unreachedVars, v.Name)
			continue
		}
		e.appendRow([]interface{}{v.Name, value})
	}
	if len(unreachedVars) != 0 {
		systemVars, err := sessionVars.GlobalVarsAccessor.GetAllSysVars()
		if err != nil {
			return errors.Trace(err)
		}
		for _, varName := range unreachedVars {
			varValue, ok := systemVars[varName]
			if !ok {
				varValue = variable.SysVars[varName].Value
			}
			e.appendRow([]interface{}{varName, varValue})
		}
	}
	return nil
}

func (e *ShowExec) fetchShowCreateTable(ctx context.Context) error {
	tb, err := e.getTable()
	if err != nil {
		return err
	}

	tableInfo := tb.Meta()
	var buf bytes.Buffer
	// TODO: let the result more like MySQL.
	if err = ConstructResultOfShowCreateTable(e.ctx, tableInfo, &buf); err != nil {
		return err
	}

	e.appendRow([]interface{}{tableInfo.TableName, buf.String()})
	return nil
}

func getDefaultCollate(charsetName string) string {
	for _, c := range charset.GetSupportedCharsets() {
		if strings.EqualFold(c.Name, charsetName) {
			return c.DefaultCollation
		}
	}
	return ""
}

// ConstructResultOfShowCreateTable constructs the result for show create table.
func ConstructResultOfShowCreateTable(ctx sctx.Context, tableInfo *model.TableMeta, buf *bytes.Buffer) (err error) {
	tblCharset := mysql.DefaultCharset
	tblCollate := getDefaultCollate(tblCharset)

	sqlMode := ctx.GetSessionVars().SQLMode
	fmt.Fprintf(buf, "CREATE TABLE %s (\n", stringutil.Escape(tableInfo.TableName, sqlMode))
	var pkCol *parser_model.ColumnInfo
	needAddComma := false
	for i, col := range tableInfo.Columns {
		if needAddComma {
			buf.WriteString(",\n")
		}
		fmt.Fprintf(buf, "  %s %s", stringutil.Escape(col.ColumnMeta.Name, sqlMode), col.GetTypeDesc())
		/*
			if col.Charset != "binary" {
				if col.Charset != tblCharset {
					fmt.Fprintf(buf, " CHARACTER SET %s", col.Charset)
				}
				if col.Collate != tblCollate {
					fmt.Fprintf(buf, " COLLATE %s", col.Collate)
				} else {
					defcol, err := charset.GetDefaultCollation(col.Charset)
					if err == nil && defcol != col.Collate {
						fmt.Fprintf(buf, " COLLATE %s", col.Collate)
					}
				}
			}
		*/

		if mysql.HasAutoIncrementFlag(col.Flag) {
			buf.WriteString(" NOT NULL AUTO_INCREMENT")
		} else {
			if mysql.HasNotNullFlag(col.Flag) {
				buf.WriteString(" NOT NULL")
			}
		}
		if tableInfo.PKIsHandle && tableInfo.ContainsAutoRandomBits() && tableInfo.GetPkName().L == col.ColumnMeta.Name {
			buf.WriteString(fmt.Sprintf(" /*T![auto_rand] AUTO_RANDOM(%d) */", tableInfo.AutoRandomBits))
		}
		if len(col.Comment) > 0 {
			buf.WriteString(fmt.Sprintf(" COMMENT '%s'", format.OutputFormat(col.Comment)))
		}
		if i != len(tableInfo.Cols())-1 {
			needAddComma = true
		}
		if tableInfo.PKIsHandle && mysql.HasPriKeyFlag(col.Flag) {
			pkCol = &col.ColumnInfo
		}
	}

	if pkCol != nil {
		// If PKIsHanle, pk info is not in tb.Indices(). We should handle it here.
		buf.WriteString(",\n")
		fmt.Fprintf(buf, "  PRIMARY KEY (%s)", stringutil.Escape(pkCol.Name.O, sqlMode))
	}

	publicIndices := make([]*parser_model.IndexInfo, 0, len(tableInfo.Indices))
	for _, idx := range tableInfo.TableInfo.Indices {
		if idx.State == parser_model.StatePublic {
			publicIndices = append(publicIndices, idx)
		}
	}
	if len(publicIndices) > 0 {
		buf.WriteString(",\n")
	}

	for i, idxInfo := range publicIndices {
		if idxInfo.Primary {
			buf.WriteString("  PRIMARY KEY ")
		} else if idxInfo.Unique {
			fmt.Fprintf(buf, "  UNIQUE KEY %s ", stringutil.Escape(idxInfo.Name.O, sqlMode))
		} else {
			fmt.Fprintf(buf, "  KEY %s ", stringutil.Escape(idxInfo.Name.O, sqlMode))
		}

		cols := make([]string, 0, len(idxInfo.Columns))
		var colInfo string
		for _, c := range idxInfo.Columns {
			if tableInfo.Columns[c.Offset].Hidden {
				colInfo = fmt.Sprintf("(%s)", tableInfo.Columns[c.Offset].GeneratedExprString)
			} else {
				colInfo = stringutil.Escape(c.Name.O, sqlMode)
				if c.Length != types.UnspecifiedLength {
					colInfo = fmt.Sprintf("%s(%s)", colInfo, strconv.Itoa(c.Length))
				}
			}
			cols = append(cols, colInfo)
		}
		fmt.Fprintf(buf, "(%s)", strings.Join(cols, ","))
		if idxInfo.Invisible {
			fmt.Fprintf(buf, ` /*!80000 INVISIBLE */`)
		}
		if i != len(publicIndices)-1 {
			buf.WriteString(",\n")
		}
	}

	buf.WriteString("\n")

	buf.WriteString(") ENGINE=InnoDB")
	// We need to explicitly set the default charset and collation
	// to make it work on MySQL server which has default collate utf8_general_ci.
	if len(tblCollate) == 0 || tblCollate == "binary" {
		// If we can not find default collate for the given charset,
		// or the collate is 'binary'(MySQL-5.7 compatibility, see #15633 for details),
		// do not show the collate part.
		fmt.Fprintf(buf, " DEFAULT CHARSET=%s", tblCharset)
	} else {
		fmt.Fprintf(buf, " DEFAULT CHARSET=%s COLLATE=%s", tblCharset, tblCollate)
	}

	// Displayed if the compression typed is set.
	if len(tableInfo.Compression) != 0 {
		fmt.Fprintf(buf, " COMPRESSION='%s'", tableInfo.Compression)
	}

	if tableInfo.AutoIdCache != 0 {
		fmt.Fprintf(buf, " /*T![auto_id_cache] AUTO_ID_CACHE=%d */", tableInfo.AutoIdCache)
	}

	if tableInfo.AutoRandID != 0 {
		fmt.Fprintf(buf, " /*T![auto_rand_base] AUTO_RANDOM_BASE=%d */", tableInfo.AutoRandID)
	}

	if tableInfo.ShardRowIDBits > 0 {
		fmt.Fprintf(buf, "/*!90000 SHARD_ROW_ID_BITS=%d ", tableInfo.ShardRowIDBits)
		if tableInfo.PreSplitRegions > 0 {
			fmt.Fprintf(buf, "PRE_SPLIT_REGIONS=%d ", tableInfo.PreSplitRegions)
		}
		buf.WriteString("*/")
	}

	if len(tableInfo.Comment) > 0 {
		fmt.Fprintf(buf, " COMMENT='%s'", format.OutputFormat(tableInfo.Comment))
	}
	return nil
}

func (s *ShowExec) getTable() (table.Table, error) {
	tbName := s.Plan.Table.Name.L
	dbName := s.Plan.DBName
	tbl, err := s.is.GetTableByName(dbName, tbName)
	return tbl, err
}

func (s *ShowExec) fetchShowColumns(ctx context.Context) error {
	tbl, err := s.getTable()
	if err != nil {
		return err
	}
	cols := tbl.Cols()
	for _, col := range cols {
		desc := table.NewColDesc(tbl, col)
		s.appendRow([]interface{}{
			desc.Field,
			desc.Type,
			desc.Key,
		})
	}
	for _, cf := range tbl.ColumnFamilies() {
		desc := table.NewCFDesc(cf)
		s.appendRow([]interface{}{
			desc.Field,
			desc.Type,
			"null",
		})
	}
	return nil
}

func (s *ShowExec) fetchShowTables(ctx context.Context) error {
	currentDB := s.Plan.DBName
	if currentDB == "" {
		currentDB = s.ctx.GetSessionVars().CurrentDB
	}
	if currentDB == "" {
		return planner.ErrNoDB
	}
	tableMetas := s.is.ListTablesByDatabase(currentDB)
	tbNames := []string{}
	for _, tb := range tableMetas {
		tbNames = append(tbNames, tb.TableName)
	}
	sort.Strings(tbNames)
	for _, name := range tbNames {
		s.appendRow([]interface{}{
			name,
		})
	}
	return nil
}

func (s *ShowExec) fetchShowDatabases(ctx context.Context) error {
	dbs := s.is.ListDatabases()
	dbNames := []string{}
	for _, db := range dbs {
		dbNames = append(dbNames, db.Database)
	}
	sort.Strings(dbNames)
	for _, name := range dbNames {
		s.appendRow([]interface{}{
			name,
		})
	}
	return nil
}

func (s *ShowExec) Schema() *expression.Schema {
	return s.Plan.Schema()
}

func (s *ShowExec) appendRow(row []interface{}) {
	for i, col := range row {
		if col == nil {
			s.result.AppendNull(i)
			continue
		}
		switch x := col.(type) {
		case nil:
			s.result.AppendNull(i)
		case int:
			s.result.AppendInt64(i, int64(x))
		case int64:
			s.result.AppendInt64(i, x)
		case uint64:
			s.result.AppendUint64(i, x)
		case float64:
			s.result.AppendFloat64(i, x)
		case float32:
			s.result.AppendFloat32(i, x)
		case string:
			s.result.AppendString(i, x)
		case []byte:
			s.result.AppendBytes(i, x)
		case types.BinaryLiteral:
			s.result.AppendBytes(i, x)
		case *types.MyDecimal:
			s.result.AppendMyDecimal(i, x)
		case types.Time:
			s.result.AppendTime(i, x)
		case json.BinaryJSON:
			s.result.AppendJSON(i, x)
		case types.Duration:
			s.result.AppendDuration(i, x)
		case types.Enum:
			s.result.AppendEnum(i, x)
		case types.Set:
			s.result.AppendSet(i, x)
		default:
			s.result.AppendNull(i)
		}
	}
}

type FakerExec struct {
	baseExecutor
}
