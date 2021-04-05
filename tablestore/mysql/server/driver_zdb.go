package server

import (
	"context"
	"crypto/tls"

	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/zhihu/zetta/tablestore/mysql/sqlexec"
)

type ZDBDriver struct {
	store kv.Storage
}

func NewZDBDriver(store kv.Storage) *ZDBDriver {
	return &ZDBDriver{
		store: store,
	}
}

// OpenCtx implements IDriver.
func (qd *ZDBDriver) OpenCtx(connID uint64, capability uint32, collation uint8, dbname string, tlsState *tls.ConnectionState) (QueryCtx, error) {
	se, err := CreateSession(qd.store)
	if err != nil {
		return nil, err
	}
	err = se.SetCollation(int(collation))
	if err != nil {
		return nil, err
	}
	se.SetConnectionID(connID)
	zc := &ZDBContext{
		session:   se,
		currentDB: dbname,
	}
	return zc, nil
}

type ZDBContext struct {
	baseContext
	session   Session
	currentDB string
}

func (tc *ZDBContext) Status() uint16 {
	return tc.session.Status()
}

func (tc *ZDBContext) GetHistorySQL() string {
	return tc.session.GetHistorySQL()
}

func (tc *ZDBContext) AffectedRows() uint64 {
	return tc.session.AffectedRows()
}

// CommitTxn implements QueryCtx CommitTxn method.
func (tc *ZDBContext) CommitTxn(ctx context.Context) error {
	return tc.session.CommitTxn(ctx)
}

// RollbackTxn implements QueryCtx RollbackTxn method.
func (tc *ZDBContext) RollbackTxn() {
	tc.session.RollbackTxn(context.TODO())
}

// CurrentDB implements QueryCtx CurrentDB method.
func (tc *ZDBContext) CurrentDB() string {
	return tc.currentDB
}

// ExecuteStmt implements QueryCtx interface.
func (tc *ZDBContext) ExecuteStmt(ctx context.Context, stmt ast.StmtNode) (ResultSet, error) {
	rs, err := tc.session.ExecuteStmt(ctx, stmt)
	if err != nil {
		return nil, err
	}
	if rs == nil {
		return nil, nil
	}
	return &zdbResultSet{
		recordSet: rs,
	}, nil
}

// Parse implements QueryCtx interface.
func (tc *ZDBContext) Parse(ctx context.Context, sql string) ([]ast.StmtNode, error) {
	return tc.session.Parse(ctx, sql)
}

// SetClientCapability implements QueryCtx SetClientCapability method.
func (tc *ZDBContext) SetClientCapability(flags uint32) {
}

// Close implements QueryCtx Close method.
func (tc *ZDBContext) Close() error {
	tc.session.Close()
	return nil
}

// FieldList implements QueryCtx FieldList method.
func (tc *ZDBContext) FieldList(table string) (columns []*ColumnInfo, err error) {
	fields, err := tc.session.FieldList(table)
	if err != nil {
		return nil, err
	}
	columns = make([]*ColumnInfo, 0, len(fields))
	//for _, f := range fields {
	//	columns = append(columns, convertColumnInfo(f))
	//}
	return columns, nil
}

func (tc *ZDBContext) GetSessionVars() *variable.SessionVars {
	return tc.session.GetSessionVars()
}

type zdbResultSet struct {
	recordSet sqlexec.RecordSet
	columns   []*ColumnInfo
	rows      []chunk.Row
}

func (z *zdbResultSet) NewChunk() *chunk.Chunk {
	return z.recordSet.NewChunk()
}

func (z *zdbResultSet) Next(ctx context.Context, req *chunk.Chunk) error {
	err := z.recordSet.Next(ctx, req)
	if err != nil {
		return err
	}
	numRows := req.NumRows()
	if numRows == 0 {
		return nil
	}
	return nil
}

func (z *zdbResultSet) StoreFetchedRows(rows []chunk.Row) {
	z.rows = rows
}

func (z *zdbResultSet) GetFetchedRows() []chunk.Row {
	if z.rows == nil {
		z.rows = make([]chunk.Row, 0, 1024)
	}
	return z.rows
}

func (z *zdbResultSet) Close() error {
	if z.recordSet != nil {
		return z.recordSet.Close()
	}
	return nil
}

/*
func (z *zdbResultSet) Columns() []*ColumnInfo {
	if z.columns != nil {
		return z.columns
	}
	schema := z.e.Schema()
	outputNames := z.eb.OutputNames
	for i := 0; i < schema.Len(); i++ {
		column := schema.Columns[i]
		fmt.Println("column name:", column.String(), column.OrigName)
		col := &ColumnInfo{
			Name:    outputNames[i].ColName.O,
			OrgName: outputNames[i].OrigColName.O,
			Table:   outputNames[i].TblName.O,
			Schema:  outputNames[i].DBName.O,
			Flag:    uint16(column.RetType.Flag),
			Charset: uint16(mysql.CharsetNameToID(column.RetType.Charset)),
			Type:    column.RetType.Tp,
		}
		z.columns = append(z.columns, col)
	}
	return z.columns
}
*/

func (z *zdbResultSet) Columns() []*ColumnInfo {
	if z.columns != nil {
		return z.columns
	}
	if z.columns == nil {
		fields := z.recordSet.Fields()
		for _, v := range fields {
			z.columns = append(z.columns, convertColumnInfo(v))
		}
	}
	return z.columns
}

func convertColumnInfo(fld *ast.ResultField) (ci *ColumnInfo) {
	ci = &ColumnInfo{
		Name:    fld.ColumnAsName.O,
		OrgName: fld.Column.Name.O,
		Table:   fld.TableAsName.O,
		Schema:  fld.DBName.O,
		Flag:    uint16(fld.Column.Flag),
		Charset: uint16(mysql.CharsetNameToID(fld.Column.Charset)),
		Type:    fld.Column.Tp,
	}

	if fld.Table != nil {
		ci.OrgTable = fld.Table.Name.O
	}
	if fld.Column.Flen == types.UnspecifiedLength {
		ci.ColumnLength = 0
	} else {
		ci.ColumnLength = uint32(fld.Column.Flen)
	}
	if fld.Column.Tp == mysql.TypeNewDecimal {
		// Consider the negative sign.
		ci.ColumnLength++
		if fld.Column.Decimal > int(types.DefaultFsp) {
			// Consider the decimal point.
			ci.ColumnLength++
		}
	}

	if fld.Column.Decimal == types.UnspecifiedLength {
		if fld.Column.Tp == mysql.TypeDuration {
			ci.Decimal = uint8(types.DefaultFsp)
		} else {
			ci.Decimal = mysql.NotFixedDec
		}
	} else {
		ci.Decimal = uint8(fld.Column.Decimal)
	}

	// Keep things compatible for old clients.
	// Refer to mysql-server/sql/protocol.cc send_result_set_metadata()
	if ci.Type == mysql.TypeVarchar {
		ci.Type = mysql.TypeVarString
	}
	return
}
