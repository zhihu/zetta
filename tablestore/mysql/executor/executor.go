package executor

import (
	"bytes"
	"context"
	"fmt"
	"runtime"
	"strings"

	"github.com/cznic/mathutil"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/stringutil"
	"github.com/zhihu/zetta/pkg/codec"
	"github.com/zhihu/zetta/pkg/model"
	"github.com/zhihu/zetta/pkg/tablecodec"
	"github.com/zhihu/zetta/tablestore/infoschema"
	"github.com/zhihu/zetta/tablestore/mysql/executor/aggfuncs"
	"github.com/zhihu/zetta/tablestore/mysql/expression"
	"github.com/zhihu/zetta/tablestore/mysql/sctx"
	"github.com/zhihu/zetta/tablestore/table"
	"github.com/zhihu/zetta/tablestore/table/tables"
	"github.com/zhihu/zetta/tablestore/util"
	"github.com/zhihu/zetta/tablestore/zstore"
	"go.uber.org/zap"
)

// Executor is the physical implementation of a algebra operator.
//
// In TiDB, all algebra operators are implemented as iterators, i.e., they
// support a simple Open-Next-Close protocol. See this paper for more details:
//
// "Volcano-An Extensible and Parallel Query Evaluation System"
//
// Different from Volcano's execution model, a "Next" function call in TiDB will
// return a batch of rows, other than a single row in Volcano.
// NOTE: Executors must call "chk.Reset()" before appending their results to it.
type Executor interface {
	base() *baseExecutor
	Open(context.Context) error
	Next(ctx context.Context, req *chunk.Chunk) error
	Close() error
	Schema() *expression.Schema
}

var (
	_ Executor = &TableScanExec{}
	_ Executor = &TableDualExec{}
	_ Executor = &SimpleExec{}
	_ Executor = &BatchPointGetExecutor{}
	_ Executor = &SimpleScanExecutor{}
)

// Next is a wrapper function on e.Next(), it handles some common codes.
func Next(ctx context.Context, e Executor, req *chunk.Chunk) error {
	return e.Next(ctx, req)
}

type baseExecutor struct {
	ctx           sctx.Context
	id            fmt.Stringer
	schema        *expression.Schema // output schema
	initCap       int
	maxChunkSize  int
	children      []Executor
	retFieldTypes []*types.FieldType
}

// base returns the baseExecutor of an executor, don't override this method!
func (e *baseExecutor) base() *baseExecutor {
	return e
}

// Open initializes children recursively and "childrenResults" according to children's schemas.
func (e *baseExecutor) Open(ctx context.Context) error {
	for _, child := range e.children {
		err := child.Open(ctx)
		if err != nil {
			return err
		}
	}
	return nil
}

// Close closes all executors and release all resources.
func (e *baseExecutor) Close() error {
	var firstErr error
	for _, src := range e.children {
		if err := src.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// Schema returns the current baseExecutor's schema. If it is nil, then create and return a new one.
func (e *baseExecutor) Schema() *expression.Schema {
	if e.schema == nil {
		return expression.NewSchema()
	}
	return e.schema
}

// Next fills multiple rows into a chunk.
func (e *baseExecutor) Next(ctx context.Context, req *chunk.Chunk) error {
	return nil
}

func newBaseExecutor(ctx sctx.Context, schema *expression.Schema, id fmt.Stringer, children ...Executor) baseExecutor {
	e := baseExecutor{
		children:     children,
		ctx:          ctx,
		id:           id,
		schema:       schema,
		initCap:      ctx.GetSessionVars().InitChunkSize,
		maxChunkSize: ctx.GetSessionVars().MaxChunkSize,
	}

	if schema != nil {
		cols := schema.Columns
		e.retFieldTypes = make([]*types.FieldType, len(cols))
		for i := range cols {
			e.retFieldTypes[i] = cols[i].RetType
		}
	}
	return e
}

// newFirstChunk creates a new chunk to buffer current executor's result.
func NewFirstChunk(e Executor) *chunk.Chunk {
	base := e.base()
	return chunk.New(base.retFieldTypes, base.initCap, base.maxChunkSize)
}

// retTypes returns all output column types.
func retTypes(e Executor) []*types.FieldType {
	base := e.base()
	return base.retFieldTypes
}

type ZettaScanExec struct {
	baseExecutor

	tbl table.Table
}
type TableScanExec struct {
	baseExecutor
	done                  bool
	tbl                   table.Table
	columns               []*model.ColumnMeta
	virtualTableChunkList *chunk.List
	virtualTableChunkIdx  int

	IdxVals []types.Datum
	Index   *model.IndexMeta
	PkVals  []types.Datum

	Limit uint64
}

func (t *TableScanExec) Open(ctx context.Context) error {
	return t.baseExecutor.Open(ctx)
}

func (t *TableScanExec) Close() error {
	return t.baseExecutor.Close()
}

func (t *TableScanExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.GrowAndReset(t.maxChunkSize)
	return t.nextChunk4InfoSchema(ctx, req)
}

func (t *TableScanExec) getStartKey() []byte {
	tbId := t.tbl.Meta().Id
	if len(t.PkVals) != 0 {
		b, _ := tablecodec.EncodeRecordPrimaryKey(nil, tbId, t.PkVals)
		return b
	}
	if len(t.IdxVals) != 0 {
		b, _ := tablecodec.EncodeIndexUnique(nil, tbId, t.Index.Id, t.IdxVals)
		return b
	}
	return t.tbl.RecordPrefix()
}

func (e *TableScanExec) nextChunk4InfoSchema(ctx context.Context, chk *chunk.Chunk) error {
	if e.virtualTableChunkList == nil {
		e.virtualTableChunkList = chunk.NewList(retTypes(e), e.initCap, e.maxChunkSize)

		columns := make([]*table.Column, e.schema.Len())
		for i, colInfo := range e.columns {
			columns[i] = table.ToColumn(colInfo)
		}

		var err error
		startKey := e.getStartKey()
		mutableRow := chunk.MutRowFromTypes(retTypes(e))

		var rowNums uint64
		fn := func(rec []types.Datum, cols []*table.Column) (bool, error) {
			if e.Limit != 0 && rowNums >= e.Limit {
				return false, nil
			}
			mutableRow.SetDatums(rec...)
			e.virtualTableChunkList.AppendRow(mutableRow.ToRow())
			rowNums++
			return true, nil
		}
		if len(e.IdxVals) != 0 {
			err = e.tbl.FetchRecordsByIndex(e.ctx, e.IdxVals, columns, e.Index.Id, e.Index.Unique, fn)
		} else if len(e.PkVals) != 0 && len(e.tbl.Meta().PrimaryKey) == 1 {
			err = e.tbl.BatchGet(e.ctx, [][]types.Datum{e.PkVals}, columns, 0, true, fn)
		} else {
			err = e.tbl.IterRecords(e.ctx, startKey, nil, columns, false, fn)
		}
		if err != nil {
			return err
		}
	}
	// no more data.
	if e.virtualTableChunkIdx >= e.virtualTableChunkList.NumChunks() {
		return nil
	}
	virtualTableChunk := e.virtualTableChunkList.GetChunk(e.virtualTableChunkIdx)
	e.virtualTableChunkIdx++
	chk.SwapColumns(virtualTableChunk)
	return nil
}

type SelectionExec struct {
	baseExecutor

	batched     bool
	filters     []expression.Expression
	selected    []bool
	inputRow    chunk.Row
	inputIter   *chunk.Iterator4Chunk
	childResult *chunk.Chunk
}

func (e *SelectionExec) Open(ctx context.Context) error {
	if err := e.baseExecutor.Open(ctx); err != nil {
		return err
	}
	return e.open(ctx)
}

func (e *SelectionExec) open(ctx context.Context) error {
	e.childResult = NewFirstChunk(e.children[0])
	e.inputIter = chunk.NewIterator4Chunk(e.childResult)
	e.inputRow = e.inputIter.End()
	return nil
}

func (e *SelectionExec) Close() error {
	e.childResult = nil
	e.selected = nil
	return e.baseExecutor.Close()
}

func PrintStack() {
	var buf [4096]byte
	n := runtime.Stack(buf[:], false)
	fmt.Printf("==> %s\n", string(buf[:n]))
}

func (e *SelectionExec) Next(ctx context.Context, req *chunk.Chunk) error {
	if !e.batched {
		return e.unPatchedNext(ctx, req)
	}
	return nil
}

func (e *SelectionExec) unPatchedNext(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	for {
		for ; e.inputRow != e.inputIter.End(); e.inputRow = e.inputIter.Next() {
			selected, _, err := expression.EvalBool(e.ctx, e.filters, e.inputRow)
			if err != nil {
				return err
			}
			if selected {
				req.AppendRow(e.inputRow)
			}
		}
		err := Next(ctx, e.children[0], e.childResult)
		if err != nil {
			return err
		}
		e.inputRow = e.inputIter.Begin()
		if e.childResult.NumRows() == 0 {
			return nil
		}
	}
}

type ProjExec struct {
	baseExecutor
	inputRow    chunk.Row
	inputIter   *chunk.Iterator4Chunk
	childResult *chunk.Chunk
	Exprs       []expression.Expression
	colIndexMap map[int]int
}

func (p *ProjExec) Open(ctx context.Context) error {
	if err := p.baseExecutor.Open(ctx); err != nil {
		return err
	}
	return p.open()
}

func (p *ProjExec) open() error {
	p.childResult = NewFirstChunk(p.children[0])
	p.inputIter = chunk.NewIterator4Chunk(p.childResult)
	p.inputRow = p.inputIter.End()
	//p.colIndexMap = make(map[int]int)
	return nil
}

func (p *ProjExec) Close() error {
	p.childResult = nil
	return p.baseExecutor.Close()
}

// Only handle select database() for now.
func (p *ProjExec) evalExprs(input, output *chunk.Chunk) error {
	for _, expr := range p.Exprs {
		if e, ok := expr.(*expression.ScalarFunction); ok {
			if e.FuncName.L == ast.Database {
				db, _, err := e.EvalString(p.ctx, input.GetRow(0))
				if err != nil {
					return err
				}
				output.AppendString(0, db)
				return nil
			}
		}
	}
	return nil
}

func (p *ProjExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.GrowAndReset(p.maxChunkSize)
	p.childResult = p.childResult.SetRequiredRows(req.RequiredRows(), p.maxChunkSize)
	err := Next(ctx, p.children[0], p.childResult)
	if err != nil {
		return err
	}
	if p.childResult.NumRows() == 0 || req.IsFull() {
		return nil
	}
	for i, expr := range p.Exprs {
		if p.childResult.NumCols() != 0 {
			req.SwapColumn(i, p.childResult, p.colIndexMap[i])
		} else {
			value, err := expr.Eval(p.childResult.GetRow(0))
			if err != nil {
				return err
			}
			req.AppendDatum(i, &value)
		}
	}
	p.evalExprs(p.childResult, req)
	return nil
}

// TableDualExec represents a dual table executor.
type TableDualExec struct {
	baseExecutor

	// numDualRows can only be 0 or 1.
	numDualRows int
	numReturned int
}

// Open implements the Executor Open interface.
func (e *TableDualExec) Open(ctx context.Context) error {
	e.numReturned = 0
	return nil
}

// Next implements the Executor Next interface.
func (e *TableDualExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	if e.numReturned >= e.numDualRows {
		return nil
	}
	if e.Schema().Len() == 0 {
		req.SetNumVirtualRows(1)
	} else {
		for i := range e.Schema().Columns {
			req.AppendNull(i)
		}
	}
	e.numReturned = e.numDualRows
	return nil
}

type SimpleExec struct {
	baseExecutor

	Statement ast.StmtNode
	done      bool
	is        infoschema.InfoSchema
}

func (s *SimpleExec) Next(ctx context.Context, req *chunk.Chunk) error {
	if s.done {
		return nil
	}

	var err error
	switch v := s.Statement.(type) {
	case *ast.UseStmt:
		err = s.executeUse(v)
	case *ast.CommitStmt:
		s.executeCommit(v)
	case *ast.RollbackStmt:
		err = s.executeRollback(v)
	}
	s.done = true
	return err
}

func (s *SimpleExec) executeUse(u *ast.UseStmt) error {
	s.ctx.GetSessionVars().CurrentDB = u.DBName
	return nil
}

func (e *SimpleExec) executeCommit(s *ast.CommitStmt) {
	e.ctx.GetSessionVars().SetStatusFlag(mysql.ServerStatusInTrans, false)
}

func (e *SimpleExec) executeRollback(s *ast.RollbackStmt) error {
	sessVars := e.ctx.GetSessionVars()
	logutil.BgLogger().Debug("execute rollback statement", zap.Uint64("conn", sessVars.ConnectionID))
	sessVars.SetStatusFlag(mysql.ServerStatusInTrans, false)
	txn, err := e.ctx.GetTxn(false, false)
	if err != nil {
		return err
	}
	if txn.Valid() {
		/*
			duration := time.Since(sessVars.TxnCtx.CreateTime).Seconds()
				if sessVars.InRestrictedSQL {
					transactionDurationInternalRollback.Observe(duration)
				} else {
					transactionDurationGeneralRollback.Observe(duration)
				}
		*/
		sessVars.TxnCtx.ClearDelta()
		return txn.Rollback()
	}
	return nil
}

type InsertExec struct {
	baseExecutor

	tbl        table.Table
	insertCols []*ast.ColumnName
	valueLists [][]expression.Expression
	defaultCF  bool
}

func (e *InsertExec) evalExpr(exprs []expression.Expression) []types.Datum {
	cols := e.tbl.Cols()
	colLen := len(cols)
	stmtContext := &stmtctx.StatementContext{}
	hasValue := make([]bool, colLen)
	datums := make([]types.Datum, colLen)

	for i, expr := range exprs {
		var colType types.FieldType
		var offset int
		if len(e.insertCols) == 0 {
			colType = cols[i].FieldType
			offset = i
		} else {
			colType = e.tbl.Meta().FindColumnByName(e.insertCols[i].Name.L).FieldType
			offset = e.tbl.Meta().GetColumnIDMap()[e.insertCols[i].Name.L]
		}
		dt, _ := expr.Eval(chunk.Row{})
		datums[offset], _ = dt.ConvertTo(stmtContext, &colType)
		hasValue[offset] = true
	}
	return e.fillRow(datums, hasValue)
}

func (e *InsertExec) fillRow(datums []types.Datum, hasValue []bool) []types.Datum {
	cols := e.tbl.Cols()
	for i, has := range hasValue {
		if !has {
			dt, _ := tables.GetColDefaultValue(e.ctx, &cols[i].ColumnInfo)
			datums[i] = dt
		}
	}
	return datums
}

func (e *InsertExec) Next(ctx context.Context, req *chunk.Chunk) error {
	for _, exprs := range e.valueLists {
		datums := e.evalExpr(exprs)
		//AddRecord means add default column family.
		err := e.tbl.AddRecord(e.ctx, datums)
		if err != nil {
			return err
		}
		e.ctx.GetSessionVars().StmtCtx.AddAffectedRows(1)
	}
	return nil
}

// LimitExec represents limit executor
// It ignores 'Offset' rows from src, then returns 'Count' rows at maximum.
type LimitExec struct {
	baseExecutor

	begin  uint64
	end    uint64
	cursor uint64

	// meetFirstBatch represents whether we have met the first valid Chunk from child.
	meetFirstBatch bool

	childResult *chunk.Chunk
}

// Next implements the Executor Next interface.
func (e *LimitExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	if e.cursor >= e.end {
		return nil
	}
	for !e.meetFirstBatch {
		// transfer req's requiredRows to childResult and then adjust it in childResult
		e.childResult = e.childResult.SetRequiredRows(req.RequiredRows(), e.maxChunkSize)
		e.adjustRequiredRows(e.childResult)
		err := Next(ctx, e.children[0], e.adjustRequiredRows(e.childResult))
		if err != nil {
			return err
		}
		batchSize := uint64(e.childResult.NumRows())
		// no more data.
		if batchSize == 0 {
			return nil
		}
		if newCursor := e.cursor + batchSize; newCursor >= e.begin {
			e.meetFirstBatch = true
			begin, end := e.begin-e.cursor, batchSize
			if newCursor > e.end {
				end = e.end - e.cursor
			}
			e.cursor += end
			if begin == end {
				break
			}
			req.Append(e.childResult, int(begin), int(end))
			return nil
		}
		e.cursor += batchSize
	}
	e.adjustRequiredRows(req)
	err := Next(ctx, e.children[0], req)
	if err != nil {
		return err
	}
	batchSize := uint64(req.NumRows())
	// no more data.
	if batchSize == 0 {
		return nil
	}
	if e.cursor+batchSize > e.end {
		req.TruncateTo(int(e.end - e.cursor))
		batchSize = e.end - e.cursor
	}
	e.cursor += batchSize
	return nil
}

// Open implements the Executor Open interface.
func (e *LimitExec) Open(ctx context.Context) error {
	if err := e.baseExecutor.Open(ctx); err != nil {
		return err
	}
	e.childResult = NewFirstChunk(e.children[0])
	e.cursor = 0
	e.meetFirstBatch = e.begin == 0
	return nil
}

// Close implements the Executor Close interface.
func (e *LimitExec) Close() error {
	e.childResult = nil
	return e.baseExecutor.Close()
}

func (e *LimitExec) adjustRequiredRows(chk *chunk.Chunk) *chunk.Chunk {
	// the limit of maximum number of rows the LimitExec should read
	limitTotal := int(e.end - e.cursor)

	var limitRequired int
	if e.cursor < e.begin {
		// if cursor is less than begin, it have to read (begin-cursor) rows to ignore
		// and then read chk.RequiredRows() rows to return,
		// so the limit is (begin-cursor)+chk.RequiredRows().
		limitRequired = int(e.begin) - int(e.cursor) + chk.RequiredRows()
	} else {
		// if cursor is equal or larger than begin, just read chk.RequiredRows() rows to return.
		limitRequired = chk.RequiredRows()
	}

	return chk.SetRequiredRows(mathutil.Min(limitTotal, limitRequired), e.maxChunkSize)
}

type AggWorker struct {
	AggFunc aggfuncs.AggFunc
	pr      aggfuncs.PartialResult
}

func (w *AggWorker) open() error {
	w.pr = w.AggFunc.AllocPartialResult()
	return nil
}

func (w *AggWorker) update(ctx sctx.Context, chk *chunk.Chunk) error {
	rows := make([]chunk.Row, 0, chk.NumRows())
	rowIter := chunk.NewIterator4Chunk(chk)
	for row := rowIter.Begin(); row != rowIter.End(); row = rowIter.Next() {
		rows = append(rows, row)
	}
	w.AggFunc.UpdatePartialResult(ctx, rows, w.pr)
	return nil
}

func (w *AggWorker) done(ctx sctx.Context, chk *chunk.Chunk) error {
	return w.AggFunc.AppendFinalResult2Chunk(ctx, w.pr, chk)
}

type AggExec struct {
	baseExecutor
	AggFuncs    []aggfuncs.AggFunc
	workers     []*AggWorker
	childResult *chunk.Chunk
	done        bool
}

func (e *AggExec) Open(ctx context.Context) error {
	if err := e.baseExecutor.Open(ctx); err != nil {
		return err
	}
	e.workers = make([]*AggWorker, len(e.AggFuncs))
	for i, aggfunc := range e.AggFuncs {
		e.workers[i] = &AggWorker{
			AggFunc: aggfunc,
		}
		e.workers[i].open()
	}
	e.childResult = NewFirstChunk(e.children[0])
	return nil
}

func (e *AggExec) workersUpdate(ctx sctx.Context, chk *chunk.Chunk) error {
	for _, worker := range e.workers {
		worker.update(ctx, chk)
	}
	return nil
}

func (e *AggExec) workersDone(ctx sctx.Context, chk *chunk.Chunk) error {
	for _, worker := range e.workers {
		worker.done(ctx, chk)
	}
	return nil
}

func (e *AggExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	if e.done {
		return nil
	}
	for {
		e.childResult.Reset()
		Next(ctx, e.children[0], e.childResult)
		if e.childResult.NumRows() == 0 {
			break
		}
		e.workersUpdate(e.ctx, e.childResult)
	}
	e.workersDone(e.ctx, req)
	e.done = true
	return nil
}

func (e *AggExec) Close() error {
	e.childResult = nil
	e.workers = nil
	return e.baseExecutor.Close()
}

type SetExec struct {
	baseExecutor

	vars []*expression.VarAssignment
	done bool
}

func (e *SetExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	if e.done {
		return nil
	}
	e.done = true
	sessionVars := e.ctx.GetSessionVars()
	for _, v := range e.vars {
		name := strings.ToLower(v.Name)
		if !v.IsSystem {
			// Set user variable.
			value, err := v.Expr.Eval(chunk.Row{})
			if err != nil {
				return err
			}

			if value.IsNull() {
				delete(sessionVars.Users, name)
			} else {
				svalue, err1 := value.ToString()
				if err1 != nil {
					return err1
				}

				sessionVars.SetUserVar(name, stringutil.Copy(svalue), value.Collation())
			}
			continue
		}
		syns := e.getSynonyms(name)
		for _, n := range syns {
			err := e.setSysVariable(ctx, n, v)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (e *SetExec) getSynonyms(varName string) []string {
	synonyms, ok := variable.SynonymsSysVariables[varName]
	if ok {
		return synonyms
	}

	synonyms = []string{varName}
	return synonyms
}

func (e *SetExec) setSysVariable(ctx context.Context, name string, v *expression.VarAssignment) error {
	sessionVars := e.ctx.GetSessionVars()
	sysVar := variable.GetSysVar(name)
	if sysVar == nil {
		return variable.ErrUnknownSystemVar.GenWithStackByArgs(name)
	}
	if sysVar.Scope == variable.ScopeNone {
		return errors.Errorf("Variable '%s' is a read only variable", name)
	}
	var valStr string
	if v.IsGlobal {
		// Set global scope system variable.
		if sysVar.Scope&variable.ScopeGlobal == 0 {
			return errors.Errorf("Variable '%s' is a SESSION variable and can't be used with SET GLOBAL", name)
		}
		value, err := e.getVarValue(v, sysVar)
		if err != nil {
			return err
		}
		if value.IsNull() {
			value.SetString("", mysql.DefaultCollationName)
		}
		valStr, err = value.ToString()
		if err != nil {
			return err
		}
		fmt.Println("set global var:", name, valStr)
		err = sessionVars.GlobalVarsAccessor.SetGlobalSysVar(name, valStr)
		if err != nil {
			return err
		}
	} else {
		// Set session scope system variable.
		if sysVar.Scope&variable.ScopeSession == 0 {
			return errors.Errorf("Variable '%s' is a GLOBAL variable and should be set with SET GLOBAL", name)
		}
		value, err := e.getVarValue(v, nil)
		if err != nil {
			return err
		}
		err = variable.SetSessionSystemVar(sessionVars, name, value)
		if err != nil {
			return err
		}
		if value.IsNull() {
			valStr = "NULL"
		} else {
			var err error
			valStr, err = value.ToString()
			terror.Log(err)
		}
		if name != variable.AutoCommit {
			logutil.BgLogger().Info("set session var", zap.Uint64("conn", sessionVars.ConnectionID), zap.String("name", name), zap.String("val", valStr))
		} else {
			// Some applications will set `autocommit` variable before query.
			// This will print too many unnecessary log info.
			logutil.BgLogger().Debug("set session var", zap.Uint64("conn", sessionVars.ConnectionID), zap.String("name", name), zap.String("val", valStr))
		}
	}

	return nil
}

func (e *SetExec) getVarValue(v *expression.VarAssignment, sysVar *variable.SysVar) (value types.Datum, err error) {
	if v.IsDefault {
		// To set a SESSION variable to the GLOBAL value or a GLOBAL value
		// to the compiled-in MySQL default value, use the DEFAULT keyword.
		// See http://dev.mysql.com/doc/refman/5.7/en/set-statement.html
		if sysVar != nil {
			value = types.NewStringDatum(sysVar.Value)
		} else {
			s, err1 := variable.GetGlobalSystemVar(e.ctx.GetSessionVars(), v.Name)
			if err1 != nil {
				return value, err1
			}
			value = types.NewStringDatum(s)
		}
		return
	}
	value, err = v.Expr.Eval(chunk.Row{})
	return value, err
}

type BatchPointGetExecutor struct {
	baseExecutor

	indexMeta   *model.IndexMeta
	tableMeta   *model.TableMeta
	indexValues [][]types.Datum
	table       table.Table
	cols        []*table.Column
	delete      bool

	virtualTableChunkList *chunk.List
	virtualTableChunkIdx  int
}

func (e *BatchPointGetExecutor) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	if e.virtualTableChunkList == nil {
		e.virtualTableChunkList = chunk.NewList(retTypes(e), e.initCap, e.maxChunkSize)
		mutableRow := chunk.MutRowFromTypes(retTypes(e))
		fn := func(rec []types.Datum, cols []*table.Column) (bool, error) {
			if e.delete {
				err := e.table.RemoveRecord(e.ctx, rec)
				if err != nil {
					return false, err
				}
			}
			mutableRow.SetDatums(rec...)
			e.virtualTableChunkList.AppendRow(mutableRow.ToRow())
			return true, nil
		}
		isPrimary := (e.indexMeta == nil)
		var idxID int64
		if !isPrimary {
			idxID = e.indexMeta.Id
		}
		err := e.table.BatchGet(e.ctx, e.indexValues, e.cols, idxID, isPrimary, fn)
		if err != nil {
			return err
		}
	}
	// no more data.
	if e.virtualTableChunkIdx >= e.virtualTableChunkList.NumChunks() {
		return nil
	}
	virtualTableChunk := e.virtualTableChunkList.GetChunk(e.virtualTableChunkIdx)
	e.virtualTableChunkIdx++
	req.SwapColumns(virtualTableChunk)
	return nil
}

type SimpleScanExecutor struct {
	baseExecutor

	indexMeta  *model.IndexMeta
	table      table.Table
	indexValue *types.Datum
	cols       []*table.Column

	limit  int64
	lowers []*util.Bound
	uppers []*util.Bound

	virtualTableChunkList *chunk.List
	virtualTableChunkIdx  int
}

func (e *SimpleScanExecutor) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	if e.virtualTableChunkList == nil {
		e.virtualTableChunkList = chunk.NewList(retTypes(e), e.initCap, e.maxChunkSize)
		mutableRow := chunk.MutRowFromTypes(retTypes(e))
		var count int64
		fn := func(rec []types.Datum, cols []*table.Column) (bool, error) {
			mutableRow.SetDatums(rec...)
			e.virtualTableChunkList.AppendRow(mutableRow.ToRow())
			count++
			if e.limit != 0 && count >= e.limit {
				return false, nil
			}
			return true, nil
		}
		var isPrimary bool
		if e.indexMeta == nil || e.indexMeta.IsPrimary {
			isPrimary = true
		}
		startDatums := []types.Datum{}
		if e.indexValue != nil {
			startDatums = []types.Datum{*e.indexValue}
		}
		lower, upper, datums, batchGet, err := e.encodeLowerUpper()
		if err != nil {
			return err
		}
		if batchGet {
			if isPrimary {
				err = e.table.BatchGet(e.ctx, [][]types.Datum{datums}, e.cols, 0, true, fn)
			} else {
				err = e.table.FetchRecordsByIndex(e.ctx, datums, e.cols, e.indexMeta.Id, e.indexMeta.Unique, fn)
			}
			if err != nil {
				return err
			}
		} else {
			err = e.table.Scan(e.ctx, startDatums, e.indexMeta, isPrimary, lower, upper, e.cols, fn, e.limit)
			if err != nil {
				return err
			}
		}
	}
	// no more data.
	if e.virtualTableChunkIdx >= e.virtualTableChunkList.NumChunks() {
		return nil
	}
	virtualTableChunk := e.virtualTableChunkList.GetChunk(e.virtualTableChunkIdx)
	e.virtualTableChunkIdx++
	req.SwapColumns(virtualTableChunk)
	return nil
}

func (e *SimpleScanExecutor) encodeLowerUpper() (lower []byte, upper []byte, datums []types.Datum, batchGet bool, err error) {
	var isPrimary bool
	if e.indexMeta == nil || e.indexMeta.IsPrimary {
		isPrimary = true
	}

	tbID := e.table.Meta().Id
	lower = nil
	upper = nil
	lowerComplete := true
	upperComplete := true
	batchGet = false

	var (
		lowerIncluded bool
		upperIncluded bool
	)

	if isPrimary {
		lower = tablecodec.EncodeTablePrefix(tbID)
		lower = tablecodec.EncodePkPrefix(lower)

		upper = tablecodec.EncodeTablePrefix(tbID)
		upper = tablecodec.EncodePkPrefix(upper)

	} else {
		idxID := e.indexMeta.Id
		lower = tablecodec.EncodeTableIndexPrefix(tbID, idxID)
		upper = tablecodec.EncodeTableIndexPrefix(tbID, idxID)
	}
	datums = make([]types.Datum, 0)

	for i, bound := range e.lowers {
		if bound == nil {
			lowerComplete = false
			if i == 0 {
				lower = nil
			}
			break
		}
		lower, err = codec.EncodeKey(nil, lower, *bound.Value)
		lowerIncluded = bound.Included
		datums = append(datums, *bound.Value)
	}

	for i, bound := range e.uppers {
		if bound == nil {
			upperComplete = false
			if i == 0 {
				upper = nil
			}
			break
		}
		upper, err = codec.EncodeKey(nil, upper, *bound.Value)
		upperIncluded = bound.Included
	}

	if bytes.Equal(lower, upper) {
		if upperComplete && lowerComplete {
			batchGet = true
		} else {
			//Prefix scan
			upper = kv.Key(lower).PrefixNext()
		}
		return
	}

	if lower != nil && !lowerIncluded {
		lower = kv.Key(lower).PrefixNext()
	}
	if upper != nil && upperIncluded {
		upper = kv.Key(upper).PrefixNext()
	}
	return
}

type CountWorker struct {
	prefix  kv.Key
	jobChan chan *tikv.Region
	outChan chan int64
	txn     kv.Transaction
}

func newCountWorker(txn kv.Transaction, ch chan *tikv.Region, out chan int64, prefix kv.Key) *CountWorker {
	return &CountWorker{
		prefix:  prefix,
		jobChan: ch,
		outChan: out,
		txn:     txn,
	}
}

func (w *CountWorker) run() {
	for {
		select {
		case region, ok := <-w.jobChan:
			if !ok {
				return
			}
			w.outChan <- w.process(region)
		}
	}
}

func (w *CountWorker) process(region *tikv.Region) int64 {
	var count int64
	startKey := region.StartKey()
	if !kv.Key(startKey).HasPrefix(w.prefix) {
		startKey = w.prefix
	}
	iter, err := w.txn.Iter(startKey, region.EndKey())
	if err != nil {
		return -1
	}
	for iter.Valid() && iter.Key().HasPrefix(w.prefix) {
		count++
		if err = iter.Next(); err != nil {
			return -1
		}
	}
	return count
}

// use for table rows counting for now.
type ParallelScanExecutor struct {
	baseExecutor
	tbl table.Table

	regionCache *tikv.RegionCache
	workerCount int

	regionCount int
	doneCount   int

	jobChan chan *tikv.Region
	outChan chan int64
}

func (e *ParallelScanExecutor) Open(ctx context.Context) error {
	if err := e.baseExecutor.Open(ctx); err != nil {
		return err
	}
	e.workerCount = 48
	s := e.ctx.GetStore().(*zstore.ZStore)
	e.regionCache = s.GetRegionCache()

	regions, err := e.getRegions()
	if err != nil {
		return err
	}
	e.regionCount = len(regions)
	e.doneCount = 0
	e.jobChan = make(chan *tikv.Region)
	e.outChan = make(chan int64)

	go func() {
		for _, region := range regions {
			e.jobChan <- region
		}
	}()
	txn, err := e.ctx.GetTxn(true, false)
	if err != nil {
		return err
	}

	for i := 0; i < e.workerCount; i++ {
		go newCountWorker(txn, e.jobChan, e.outChan, e.tbl.RecordPrefix()).run()
	}
	return nil
}

func (e *ParallelScanExecutor) getRegions() ([]*tikv.Region, error) {
	startKey := e.tbl.RecordPrefix()
	endKey := startKey.PrefixNext()
	return e.regionCache.LoadRegionsInKeyRange(tikv.NewBackoffer(context.Background(), 2000), startKey, endKey)
}

func (e *ParallelScanExecutor) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	if e.doneCount >= e.regionCount {
		return nil
	}

	for {
		select {
		case count := <-e.outChan:
			req.AppendInt64(0, count)
			e.doneCount++
			if e.doneCount >= e.regionCount {
				close(e.jobChan)
				return nil
			}
		}
	}
}
