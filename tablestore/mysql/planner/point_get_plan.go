package planner

import (
	"errors"

	"github.com/pingcap/parser/ast"
	parser_model "github.com/pingcap/parser/model"
	"github.com/pingcap/parser/opcode"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	driver "github.com/pingcap/tidb/types/parser_driver"
	"github.com/pingcap/tidb/util/plancodec"
	"github.com/zhihu/zetta/pkg/model"
	"github.com/zhihu/zetta/tablestore/domain"
	"github.com/zhihu/zetta/tablestore/mysql/expression"
	"github.com/zhihu/zetta/tablestore/mysql/sctx"
	"github.com/zhihu/zetta/tablestore/table"
	"github.com/zhihu/zetta/tablestore/util"
)

func TryFastPlan(ctx sctx.Context, node ast.Node) Plan {
	switch x := node.(type) {
	case *ast.SelectStmt:
		if p := tryWhereIn2BatchPointGet(ctx, x, false); p != nil {
			return p
		}
		if p := trySimpleScanPlan(ctx, x); p != nil {
			return p
		}
	case *ast.DeleteStmt:
		if p := tryWhereInPlanDelete(ctx, x); p != nil {
			return p
		}
	}
	return nil
}

func trySimpleScanPlan(ctx sctx.Context, selStmt *ast.SelectStmt) *SimpleScanPlan {
	if selStmt.GroupBy != nil || selStmt.Having != nil || len(selStmt.WindowSpecs) > 0 {
		return nil
	}
	return newSimpleScanPlan(ctx, selStmt)
}

func newSimpleScanPlan(ctx sctx.Context, selStmt *ast.SelectStmt) *SimpleScanPlan {
	var (
		limitCount uint64
		err        error
	)
	if selStmt.Limit != nil {
		limitCount, _, err = extractLimitCountOffset(ctx, selStmt.Limit)
		if err != nil {
			return nil
		}
	}

	if selStmt.OrderBy != nil && len(selStmt.OrderBy.Items) > 1 {
		return nil
	}
	if selStmt.Where == nil {
		return nil
	}

	tbl, schema, names := getTblSchemaNames(ctx, selStmt)
	if tbl == nil {
		return nil
	}

	var (
		//orderColName string
		indexMeta *model.IndexMeta
		isPrimary bool
	)

	if selStmt.OrderBy != nil && len(selStmt.OrderBy.Items) > 0 {
		orderByItem := selStmt.OrderBy.Items[0]
		_, indexMeta, isPrimary = getOrderByItemIndex(tbl, orderByItem)
		if indexMeta == nil && !isPrimary {
			return nil
		}
	}

	/*
		whereColName, lower, upper := extractWhere(tbl, selStmt.Where)
		if (lower == nil && upper == nil) || (orderColName != "" && orderColName != whereColName) {
			return nil
		}
	*/
	lowers, uppers, indexMeta, err := checkWhere(tbl, selStmt.Where)
	if err != nil {
		return nil
	}

	ssp := SimpleScanPlan{
		ctx:       ctx,
		IndexMeta: indexMeta,
		Table:     tbl,
		Upper:     uppers,
		Lower:     lowers,

		Limit: int64(limitCount),
	}.Init(ctx, schema, names)
	return ssp
}

//Check if all cols in where conditions belong to one same index.
func checkWhere(tbl table.Table, where ast.ExprNode) (lowers []*util.Bound, uppers []*util.Bound, idx *model.IndexMeta, err error) {
	var hasLeftPrefix bool

	conditions := splitWhere(where)
	colOffset, idxMeta, lower, upper, err := extractSingleCond(tbl, conditions[0])
	if err != nil {
		return nil, nil, idxMeta, err
	}
	var idxColNums int
	if idxMeta == nil {
		idxColNums = len(tbl.Meta().PrimaryKey)
	} else {
		idxColNums = len(idxMeta.DefinedColumns)
	}

	lowers = make([]*util.Bound, idxColNums)
	uppers = make([]*util.Bound, idxColNums)
	lowers[colOffset] = lower
	uppers[colOffset] = upper

	var idxMeta1 *model.IndexMeta
	for _, cond := range conditions {
		if colOffset == 0 {
			hasLeftPrefix = true
		}
		colOffset, idxMeta1, lower, upper, err = extractSingleCond(tbl, cond)
		if err != nil {
			return nil, nil, idxMeta, err
		}
		if idxMeta1 != idxMeta {
			return nil, nil, nil, errors.New("where conditions must belong to only one index")
		}
		if lowers[colOffset] == nil {
			lowers[colOffset] = lower
		}
		if uppers[colOffset] == nil {
			uppers[colOffset] = upper
		}
	}
	if !hasLeftPrefix {
		return nil, nil, nil, errors.New("no left prefix column found")
	}
	for i := range lowers {
		if lowers[i] != nil && uppers[i] != nil {
			c, err := lowers[i].Value.CompareDatum(nil, uppers[i].Value)
			if err != nil {
				return lowers, uppers, idxMeta, err
			}
			if c != 0 && i != idxColNums-1 {
				return lowers, uppers, idxMeta, errors.New("not all cols in condition are indexed")
			}
		}
		if lowers[i] == nil && uppers[i] == nil {
			return lowers, uppers, idxMeta, errors.New("not all cols in condition are indexed")
		}
		if (lowers[i] == nil || uppers[i] == nil) && i != idxColNums-1 {
			return lowers, uppers, idxMeta, errors.New("not all cols in condition are indexed")
		}

	}
	return lowers, uppers, idxMeta, nil
}

func extractSingleCond(tbl table.Table, cond ast.ExprNode) (colOffset int, idxMeta *model.IndexMeta, lower *util.Bound, upper *util.Bound, err error) {
	var (
		value   types.Datum
		colName string
		colMeta *model.ColumnMeta
	)
	lower = &util.Bound{}
	upper = &util.Bound{}
	colOffset = -1

	switch x := cond.(type) {
	case *ast.BinaryOperationExpr:
		switch x.Op {
		case opcode.LT:
			lower = nil
		case opcode.LE:
			lower = nil
			upper.Included = true
		case opcode.GT:
			upper = nil
		case opcode.GE:
			upper = nil
			lower.Included = true
		case opcode.EQ:
			upper.Included = true
			lower.Included = true
		default:
			err = errors.New("unsupported operand")
			return
		}

		if col, ok := x.L.(*ast.ColumnNameExpr); !ok {
			err = errors.New("must be a column name at left")
			return
		} else {
			colName = col.Name.Name.L
		}

		if colMeta = tbl.Meta().FindColumnByName(colName); colMeta == nil {
			err = errors.New("no column info found")
			return
		}
		if colOffset, idxMeta = tbl.Meta().GetIndexByColumnName(colName); colOffset == -1 {
			err = errors.New("no index info found")
			return
		}

		if d, ok := x.R.(*driver.ValueExpr); !ok {
			err = errors.New("must be a constant at right")
			return
		} else {
			value, err = d.Datum.ConvertTo(&stmtctx.StatementContext{}, &colMeta.FieldType)
			if err != nil {
				return
			}
			if lower != nil {
				lower.Value = &value
			}
			if upper != nil {
				upper.Value = &value
			}
			return
		}
	default:
		err = errors.New("unsupported expr")
		return
	}
}

//Only support simple conditions like where _id > 1 or where _id < 1.
func extractWhere(tbl table.Table, where ast.ExprNode) (colName string, lower *util.Bound, upper *util.Bound) {
	var (
		err   error
		value types.Datum
	)
	lower = &util.Bound{}
	upper = &util.Bound{}
	switch x := where.(type) {
	case *ast.BinaryOperationExpr:
		switch x.Op {
		case opcode.LT:
			lower = nil
		case opcode.LE:
			lower = nil
			upper.Included = true
		case opcode.GT:
			upper = nil
		case opcode.GE:
			upper = nil
			upper.Included = true
		case opcode.And:
			colNamel, lowerl, upperl := extractWhere(tbl, x.L)
			colNamer, lowerr, upperr := extractWhere(tbl, x.L)
			if colNamel != colNamer {
				return colName, nil, nil
			}
			if lowerl == nil {
				lower = lowerr
			} else {
				lower = lowerl
			}

			if upperl == nil {
				upper = upperr
			} else {
				upper = upperl
			}
			return
		default:
			return colName, nil, nil
		}

		if col, ok := x.L.(*ast.ColumnNameExpr); !ok {
			return colName, nil, nil
		} else {
			colName = col.Name.Name.L
		}
		if colMeta := tbl.Meta().FindColumnByName(colName); colMeta == nil {
			return colName, nil, nil
		} else {
			if d, ok := x.R.(*driver.ValueExpr); !ok {
				return colName, nil, nil
			} else {
				value, err = d.Datum.ConvertTo(&stmtctx.StatementContext{}, &colMeta.FieldType)
				if err != nil {
					return colName, nil, nil
				}
				if lower != nil {
					lower.Value = &value
				}
				if upper != nil {
					upper.Value = &value
				}
				return
			}
		}
	default:
		return colName, nil, nil
	}
}

func getSimpleColumnValue(tbl table.Table, expr ast.ExprNode) *types.Datum {
	var (
		col   *ast.ColumnNameExpr
		ok    bool
		value types.Datum
		err   error
	)
	switch x := expr.(type) {
	case *ast.BinaryOperationExpr:
		if col, ok = x.L.(*ast.ColumnNameExpr); !ok {
			return nil
		}
		if colMeta := tbl.Meta().FindColumnByName(col.Name.Name.L); colMeta == nil {
			return nil
		} else {
			if d, ok := x.R.(*driver.ValueExpr); !ok {
				return nil
			} else {
				value, err = d.Datum.ConvertTo(&stmtctx.StatementContext{}, &colMeta.FieldType)
				if err != nil {
					return nil
				}
			}
		}
	default:
		return nil
	}
	return &value
}

func getOrderByItemIndex(tbl table.Table, orderItem *ast.ByItem) (string, *model.IndexMeta, bool) {
	tbName := tbl.Meta().TableName
	var colName string
	switch x := orderItem.Expr.(type) {
	case *ast.ColumnNameExpr:
		if name := x.Name.Table.L; name != "" && name != tbName {
			return colName, nil, false
		}
		colName = x.Name.Name.L
	default:
		return colName, nil, false
	}
	pkeys := tbl.Meta().PrimaryKey
	//Only support the left prefix.
	for i, pkey := range pkeys {
		if colName == pkey && i == 0 {
			return colName, nil, true
		}
	}
	_, indexMeta := tbl.Meta().GetIndexByColumnName(colName)
	if indexMeta == nil {
		return colName, nil, false
	}
	//Only support the left prefix.
	if indexMeta.DefinedColumns[0] != colName {
		return colName, nil, false
	}
	return colName, indexMeta, false
}

func getTblSchemaNames(ctx sctx.Context, selStmt *ast.SelectStmt) (table.Table, *expression.Schema, []*types.FieldName) {
	tblName, _ := getSingleTableNameAndAlias(selStmt.From)
	if tblName == nil {
		return nil, nil, nil
	}
	dbName := tblName.Schema.L
	if dbName == "" {
		dbName = ctx.GetSessionVars().CurrentDB
	}
	tbName := tblName.Name.L
	is := domain.GetOnlyDomain().InfoSchema()
	tbl, err := is.GetTableByName(dbName, tbName)
	if err != nil {
		return nil, nil, nil
	}
	for _, col := range tbl.Meta().Columns {
		if col.State != parser_model.StatePublic && col.State != parser_model.StateNone {
			return nil, nil, nil
		}
	}
	schema, names := buildSchemaFromFields(tblName.Schema, tbl.Meta(), tblName.Name, selStmt.Fields.Fields)
	if schema == nil {
		return nil, nil, nil
	}
	return tbl, schema, names
}

func tryWhereInPlanDelete(ctx sctx.Context, delStmt *ast.DeleteStmt) *Delete {
	selStmt := &ast.SelectStmt{}
	selStmt.Where = delStmt.Where
	selStmt.From = delStmt.TableRefs
	selectField := &ast.SelectField{
		WildCard: &ast.WildCardField{},
	}
	selStmt.Fields = &ast.FieldList{
		Fields: []*ast.SelectField{selectField},
	}

	tblName, _ := getSingleTableNameAndAlias(selStmt.From)
	if tblName == nil {
		return nil
	}
	dbName := tblName.Schema.L
	if dbName == "" {
		dbName = ctx.GetSessionVars().CurrentDB
	}
	tbName := tblName.Name.L
	is := domain.GetOnlyDomain().InfoSchema()
	tbl, err := is.GetTableByName(dbName, tbName)
	if err != nil {
		return nil
	}

	pointGetPlan := tryWhereIn2BatchPointGet(ctx, selStmt, false)
	if pointGetPlan == nil {
		return nil
	}
	return Delete{
		Table:              tbl,
		SimplePointGetPlan: pointGetPlan,
	}.Init(ctx)
}

func tryWhereIn2BatchPointGet(ctx sctx.Context, selStmt *ast.SelectStmt, del bool) *BatchPointGetPlan {
	if selStmt.OrderBy != nil || selStmt.GroupBy != nil ||
		selStmt.Having != nil || len(selStmt.WindowSpecs) > 0 {
		return nil
	}

	tblName, _ := getSingleTableNameAndAlias(selStmt.From)
	if tblName == nil {
		return nil
	}
	dbName := tblName.Schema.L
	if dbName == "" {
		dbName = ctx.GetSessionVars().CurrentDB
	}
	tbName := tblName.Name.L
	is := domain.GetOnlyDomain().InfoSchema()
	tbl, err := is.GetTableByName(dbName, tbName)
	if err != nil {
		return nil
	}

	exprs := splitWhere(selStmt.Where)
	var (
		in  *ast.PatternInExpr
		dts []types.Datum
	)
	for _, expr := range exprs {
		hasIn, ok := expr.(*ast.PatternInExpr)
		if ok && !hasIn.Not && len(hasIn.List) >= 1 {
			in = hasIn
			continue
		}
		if value := getSimpleColumnValue(tbl, expr); value != nil {
			dts = append(dts, *value)
		}
	}

	if in == nil {
		return nil
	}

	for _, col := range tbl.Meta().Columns {
		if col.State != parser_model.StatePublic && col.State != parser_model.StateNone {
			return nil
		}
	}
	schema, names := buildSchemaFromFields(tblName.Schema, tbl.Meta(), tblName.Name, selStmt.Fields.Fields)
	if schema == nil {
		return nil
	}

	//Do not support multi col names.
	var whereColName string
	// SELECT * FROM t WHERE (key) in ((1), (2))
	colExpr := in.Expr
	if p, ok := colExpr.(*ast.ParenthesesExpr); ok {
		colExpr = p.Expr
	}
	switch colName := colExpr.(type) {
	case *ast.ColumnNameExpr:
		if name := colName.Name.Table.L; name != "" && name != tbName {
			return nil
		}
		whereColName = colName.Name.Name.L
	default:
		return nil
	}

	p := newBatchPointGetPlan(ctx, in, dts, tbl, schema, names, whereColName, del)
	return p
}

func newBatchPointGetPlan(ctx sctx.Context, patternInExpr *ast.PatternInExpr, dts []types.Datum,
	tbl table.Table, schema *expression.Schema, names []*types.FieldName, whereColName string, del bool) *BatchPointGetPlan {
	indexValues := make([][]types.Datum, len(patternInExpr.List))
	_, indexMeta := tbl.Meta().GetIndexByColumnName(whereColName)
	colMeta := tbl.Meta().FindColumnByName(whereColName)
	for i, item := range patternInExpr.List {
		// SELECT * FROM t WHERE (key) in ((1), (2))
		if p, ok := item.(*ast.ParenthesesExpr); ok {
			item = p.Expr
		}
		var (
			values []types.Datum
			d      types.Datum
			err    error
		)
		switch x := item.(type) {
		case *driver.ValueExpr:
			//if indexMeta != nil {
			d, err = x.Datum.ConvertTo(&stmtctx.StatementContext{}, &colMeta.FieldType)
			if err != nil {
				return nil
			}
			//}
			values = []types.Datum{d}
		default:
			return nil
		}
		indexValues[i] = append(values, dts...)
	}
	p := BatchPointGetPlan{
		IndexMeta:   indexMeta,
		Table:       tbl,
		ctx:         ctx,
		IndexValues: indexValues,
		Delete:      del,
	}.Init(ctx, schema, names)
	return p
}

// getSingleTableNameAndAlias return the ast node of queried table name and the alias string.
// `tblName` is `nil` if there are multiple tables in the query.
// `tblAlias` will be the real table name if there is no table alias in the query.
func getSingleTableNameAndAlias(tableRefs *ast.TableRefsClause) (tblName *ast.TableName, tblAlias model.CIStr) {
	if tableRefs == nil || tableRefs.TableRefs == nil || tableRefs.TableRefs.Right != nil {
		return nil, tblAlias
	}
	tblSrc, ok := tableRefs.TableRefs.Left.(*ast.TableSource)
	if !ok {
		return nil, tblAlias
	}
	tblName, ok = tblSrc.Source.(*ast.TableName)
	if !ok {
		return nil, tblAlias
	}
	return tblName, tblAlias
}

func colInfoToColumn(col *model.ColumnMeta, idx int) *expression.Column {
	return &expression.Column{
		RetType:  &col.FieldType,
		ID:       col.Id,
		UniqueID: int64(col.Offset),
		Index:    idx,
		OrigName: col.ColumnMeta.Name,
	}
}

func buildSchemaFromFields(
	dbName parser_model.CIStr,
	tbl *model.TableMeta,
	tblName parser_model.CIStr,
	fields []*ast.SelectField,
) (
	*expression.Schema,
	[]*types.FieldName,
) {
	columns := make([]*expression.Column, 0, len(tbl.Columns)+1)
	names := make([]*types.FieldName, 0, len(tbl.Columns)+1)
	if len(fields) > 0 {
		for _, field := range fields {
			if field.WildCard != nil {
				if field.WildCard.Table.L != "" && field.WildCard.Table.L != tblName.L {
					return nil, nil
				}
				for _, col := range tbl.Columns {
					names = append(names, &types.FieldName{
						DBName:      dbName,
						OrigTblName: parser_model.NewCIStr(tbl.TableName),
						TblName:     tblName,
						ColName:     parser_model.NewCIStr(col.ColumnMeta.Name),
					})
					columns = append(columns, colInfoToColumn(col, len(columns)))
				}
				continue
			}
			colNameExpr, ok := field.Expr.(*ast.ColumnNameExpr)
			if !ok {
				return nil, nil
			}
			if colNameExpr.Name.Table.L != "" && colNameExpr.Name.Table.L != tblName.L {
				return nil, nil
			}
			//col := findCol(tbl, colNameExpr.Name)
			col := tbl.FindColumnByName(colNameExpr.Name.Name.L)
			if col == nil {
				return nil, nil
			}
			//asName := col.Name
			//if field.AsName.L != "" {
			//	asName = field.AsName
			//}
			names = append(names, &types.FieldName{
				DBName:      dbName,
				OrigTblName: parser_model.NewCIStr(tbl.TableName),
				TblName:     tblName,
				ColName:     parser_model.NewCIStr(col.ColumnMeta.Name),
			})
			columns = append(columns, colInfoToColumn(col, len(columns)))
		}
		return expression.NewSchema(columns...), names
	}
	// fields len is 0 for update and delete.
	/*
		for _, col := range tbl.Columns {
			names = append(names, &types.FieldName{
				DBName:      dbName,
				OrigTblName: tbl.Name,
				TblName:     tblName,
				ColName:     col.Name,
			})
			column := colInfoToColumn(col, len(columns))
			columns = append(columns, column)
		}
		schema := expression.NewSchema(columns...)
	*/
	return nil, names
}

type BatchPointGetPlan struct {
	baseSchemaProducer

	ctx         sctx.Context
	Table       table.Table
	IndexMeta   *model.IndexMeta
	IndexValues [][]types.Datum

	Delete bool
}

func (p BatchPointGetPlan) Init(ctx sctx.Context, schema *expression.Schema, names []*types.FieldName) *BatchPointGetPlan {
	p.basePlan = newBasePlan(ctx, plancodec.TypeBatchPointGet)
	p.schema = schema
	p.names = names
	return &p
}

type bound struct {
	Less     bool
	Desc     bool
	Included bool
	Value    *types.Datum
}

type SimpleScanPlan struct {
	baseSchemaProducer

	ctx       sctx.Context
	Table     table.Table
	IndexMeta *model.IndexMeta

	Lower []*util.Bound
	Upper []*util.Bound

	Limit int64
}

func (p SimpleScanPlan) Init(ctx sctx.Context, schema *expression.Schema, names []*types.FieldName) *SimpleScanPlan {
	p.basePlan = newBasePlan(ctx, plancodec.TypeTableScan)
	p.schema = schema
	p.names = names
	return &p
}
