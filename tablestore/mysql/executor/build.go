package executor

import (
	"context"

	"github.com/cznic/mathutil"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/stringutil"
	"github.com/zhihu/zetta/tablestore/domain"
	"github.com/zhihu/zetta/tablestore/infoschema"
	"github.com/zhihu/zetta/tablestore/mysql/executor/aggfuncs"
	"github.com/zhihu/zetta/tablestore/mysql/expression"
	"github.com/zhihu/zetta/tablestore/mysql/planner"
	"github.com/zhihu/zetta/tablestore/mysql/sctx"
	"github.com/zhihu/zetta/tablestore/table"
)

// recordSet wraps an executor, implements sqlexec.RecordSet interface
type recordSet struct {
	fields          []*ast.ResultField
	executorBuilder *ExecutorBuilder
	executor        Executor
	lastErr         error
	txnStartTS      uint64
}

func (a *recordSet) Fields() []*ast.ResultField {
	if len(a.fields) == 0 {
		a.fields = colNames2ResultFields(a.executor.Schema(), a.executorBuilder.OutputNames, a.executorBuilder.Ctx.GetSessionVars().CurrentDB)
	}
	return a.fields
}

func colNames2ResultFields(schema *expression.Schema, names []*types.FieldName, defaultDB string) []*ast.ResultField {
	rfs := make([]*ast.ResultField, 0, schema.Len())
	defaultDBCIStr := model.NewCIStr(defaultDB)
	for i := 0; i < schema.Len(); i++ {
		dbName := names[i].DBName
		if dbName.L == "" && names[i].TblName.L != "" {
			dbName = defaultDBCIStr
		}
		origColName := names[i].OrigColName
		if origColName.L == "" {
			origColName = names[i].ColName
		}
		rf := &ast.ResultField{
			Column:       &model.ColumnInfo{Name: origColName, FieldType: *schema.Columns[i].RetType},
			ColumnAsName: names[i].ColName,
			Table:        &model.TableInfo{Name: names[i].OrigTblName},
			TableAsName:  names[i].TblName,
			DBName:       dbName,
		}
		rfs = append(rfs, rf)
	}
	return rfs
}

// Next use uses recordSet's executor to get next available chunk for later usage.
// If chunk does not contain any rows, then we update last query found rows in session variable as current found rows.
// The reason we need update is that chunk with 0 rows indicating we already finished current query, we need prepare for
// next query.
// If stmt is not nil and chunk with some rows inside, we simply update last query found rows by the number of row in chunk.
func (a *recordSet) Next(ctx context.Context, req *chunk.Chunk) (err error) {
	err = Next(ctx, a.executor, req)
	if err != nil {
		a.lastErr = err
		return err
	}
	numRows := req.NumRows()
	if numRows == 0 {
		return nil
	}
	return nil
}

// NewChunk create a chunk base on top-level executor's newFirstChunk().
func (a *recordSet) NewChunk() *chunk.Chunk {
	return NewFirstChunk(a.executor)
}

func (a *recordSet) Close() error {
	err := a.executor.Close()
	if err != nil {
		return err
	}
	//sessVars := a.executorBuilder.Ctx.GetSessionVars()
	//if !sessVars.InTxn() {
	err = a.executorBuilder.Ctx.CommitTxnWrapper(context.Background())
	//}
	return err
}

type Compiler struct {
	Ctx sctx.Context
}

func (c *Compiler) Compile(ctx context.Context, stmtNode ast.StmtNode) (*ExecutorBuilder, error) {
	is := domain.GetOnlyDomain().InfoSchema()
	planBuilder := planner.NewPlanBuilder(c.Ctx, is)
	plan, err := planBuilder.Build(ctx, stmtNode)
	if err != nil {
		return nil, err
	}
	eb := &ExecutorBuilder{
		Plan:     plan,
		Ctx:      c.Ctx,
		is:       is,
		StmtNode: stmtNode,
	}
	if plan != nil {
		eb.OutputNames = plan.OutputNames()
	}
	return eb, nil
}

type ExecutorBuilder struct {
	Plan        planner.Plan
	Ctx         sctx.Context
	StmtNode    ast.StmtNode
	is          infoschema.InfoSchema
	OutputNames types.NameSlice
}

func (eb *ExecutorBuilder) Build(ctx context.Context) (*recordSet, error) {
	e, err := eb.build(eb.Plan)
	if err != nil {
		return nil, err
	}
	err = e.Open(ctx)
	if err != nil {
		return nil, err
	}
	return &recordSet{
		executor:        e,
		executorBuilder: eb,
	}, nil
}

func (eb *ExecutorBuilder) build(p planner.Plan) (Executor, error) {
	switch v := p.(type) {
	case *planner.LogicalShow:
		return eb.buildShow(v)
	case *planner.LogicalProjection:
		return eb.buildProjection(v)
	case *planner.LogicalSelection:
		return eb.buildSelection(v)
	case *planner.DataSource:
		return eb.buildTableScan(v)
	case *planner.LogicalTableDual:
		return eb.buildTableDual(v)
	case *planner.Simple:
		return eb.buildSimple(v)
	case *planner.DDL:
		return eb.buildDDL(v)
	case *planner.Insert:
		return eb.buildInsert(v)
	case *planner.Delete:
		return eb.buildDelete(v)
	case *planner.LogicalUpdate:
		return eb.buildUpdate(v)
	case *planner.LogicalLimit:
		return eb.buildLimit(v)
	case *planner.LogicalSort:
		return eb.buildSort(v)
	case *planner.LogicalAggregation:
		return eb.buildAgg(v)
	case *planner.Set:
		return eb.buildSet(v)
	case *planner.SplitRegion:
		return eb.buildSplitRegion(v)
	case *planner.BatchPointGetPlan:
		return eb.buildBatchPointGetPlan(v)
	case *planner.SimpleScanPlan:
		return eb.buildSimpleScanPlan(v)
	default:
		return eb.buildFaker(v)
	}
	return nil, errors.New("Unsupported plan")
}

func (eb *ExecutorBuilder) buildSimpleScanPlan(v *planner.SimpleScanPlan) (Executor, error) {
	cols := make([]*table.Column, v.Schema().Len())
	for i, ecol := range v.Schema().Columns {
		col := v.Table.Meta().FindColumnByName(ecol.OrigName)
		cols[i] = &table.Column{col}
	}
	e := &SimpleScanExecutor{
		baseExecutor: newBaseExecutor(eb.Ctx, v.Schema(), v.ExplainID()),
		indexMeta:    v.IndexMeta,
		//indexValue:   v.IndexValue,
		table: v.Table,
		cols:  cols,

		uppers: v.Upper,
		lowers: v.Lower,
		//desc:  v.Desc,
		limit: v.Limit,
	}
	return e, nil
}

func (eb *ExecutorBuilder) buildBatchPointGetPlan(v *planner.BatchPointGetPlan) (Executor, error) {
	cols := make([]*table.Column, v.Schema().Len())
	for i, ecol := range v.Schema().Columns {
		col := v.Table.Meta().FindColumnByName(ecol.OrigName)
		cols[i] = &table.Column{col}
	}
	e := &BatchPointGetExecutor{
		baseExecutor: newBaseExecutor(eb.Ctx, v.Schema(), v.ExplainID()),
		indexMeta:    v.IndexMeta,
		indexValues:  v.IndexValues,
		table:        v.Table,
		cols:         cols,
		delete:       v.Delete,
	}
	return e, nil
}

func (b *ExecutorBuilder) buildSplitRegion(v *planner.SplitRegion) (Executor, error) {
	e := &SplitIndexRegionExec{
		baseExecutor: newBaseExecutor(b.Ctx, v.Schema(), v.ExplainID()),
		tableInfo:    v.TableMeta,
		indexInfo:    v.IndexMeta,
		lower:        v.Lower,
		upper:        v.Upper,
		valueLists:   v.ValueLists,
		num:          v.Num,
	}
	return e, nil
}

func (b *ExecutorBuilder) buildSet(v *planner.Set) (Executor, error) {
	return &SetExec{
		baseExecutor: newBaseExecutor(b.Ctx, v.Schema(), v.ExplainID()),
		vars:         v.VarAssigns,
	}, nil
}

func (b *ExecutorBuilder) buildAgg(v *planner.LogicalAggregation) (Executor, error) {
	//childExec, err := b.build(v.Children()[0])
	//if err != nil {
	//	return nil, err
	//}
	childExec := &ParallelScanExecutor{
		baseExecutor: newBaseExecutor(b.Ctx, v.Schema(), v.ExplainID()),
		tbl:          v.Table,
	}
	aggFuncs := make([]aggfuncs.AggFunc, len(v.AggFuncs))
	for i, agg := range v.AggFuncs {
		af := aggfuncs.Build(b.Ctx, agg, i)
		aggFuncs[i] = af
	}
	return &AggExec{
		baseExecutor: newBaseExecutor(b.Ctx, v.Schema(), v.ExplainID(), childExec),
		AggFuncs:     aggFuncs,
	}, nil
}

func (b *ExecutorBuilder) buildSort(v *planner.LogicalSort) (Executor, error) {
	childExec, err := b.build(v.Children()[0])
	if err != nil {
		return nil, err
	}
	sortExec := SortExec{
		baseExecutor: newBaseExecutor(b.Ctx, v.Schema(), v.ExplainID(), childExec),
		ByItems:      v.ByItems,
		schema:       v.Schema(),
	}
	return &sortExec, nil
}

func (b *ExecutorBuilder) buildLimit(v *planner.LogicalLimit) (Executor, error) {
	childExec, err := b.build(v.Children()[0])
	if err != nil {
		return nil, err
	}
	n := int(mathutil.MinUint64(v.Count, uint64(b.Ctx.GetSessionVars().MaxChunkSize)))
	base := newBaseExecutor(b.Ctx, v.Schema(), v.ExplainID(), childExec)
	base.initCap = n
	e := &LimitExec{
		baseExecutor: base,
		begin:        v.Offset,
		end:          v.Offset + v.Count,
	}
	return e, nil
}

func (eb *ExecutorBuilder) buildUpdate(p *planner.LogicalUpdate) (Executor, error) {
	updateExec := &UpdateExec{
		baseExecutor: newBaseExecutor(eb.Ctx, p.Schema(), p.ExplainID()),
		tbl:          p.Table,
		values:       p.ColValues,
	}

	if p.DataSource != nil {
		selExec, err := eb.build(p.DataSource)
		if err != nil {
			return nil, err
		}
		updateExec.children = append(updateExec.children, selExec)
	}
	return updateExec, nil
}

func (eb *ExecutorBuilder) buildDelete(p *planner.Delete) (Executor, error) {
	delExec := &DeleteExec{
		baseExecutor: newBaseExecutor(eb.Ctx, p.Schema(), p.ExplainID()),
		tbl:          p.Table,
	}
	// SelectionPlan and SimplePointGetPlan can not be not nil at the same time.
	if p.SelectionPlan != nil {
		selExec, err := eb.build(p.SelectionPlan)
		if err != nil {
			return nil, err
		}
		delExec.children = append(delExec.children, selExec)
	}
	if p.SimplePointGetPlan != nil {
		spExec, err := eb.build(p.SimplePointGetPlan)
		if err != nil {
			return nil, err
		}
		delExec.children = append(delExec.children, spExec)
	}
	return delExec, nil
}

func (eb *ExecutorBuilder) buildInsert(p *planner.Insert) (Executor, error) {
	return &InsertExec{
		baseExecutor: newBaseExecutor(eb.Ctx, p.Schema(), p.ExplainID()),
		tbl:          p.Table,
		valueLists:   p.Lists,
		insertCols:   p.Columns,
		defaultCF:    p.DefaultCF,
	}, nil
}

func (eb *ExecutorBuilder) buildDDL(p *planner.DDL) (Executor, error) {
	return &DDLExec{
		baseExecutor: newBaseExecutor(eb.Ctx, p.Schema(), p.ExplainID()),
		stmt:         p.Statement,
		is:           eb.is,
	}, nil
}

func (eb *ExecutorBuilder) buildSimple(p *planner.Simple) (Executor, error) {
	return &SimpleExec{
		baseExecutor: newBaseExecutor(eb.Ctx, p.Schema(), p.ExplainID()),
		Statement:    p.Statement,
		is:           eb.is,
	}, nil
}

func (eb *ExecutorBuilder) buildTableDual(p *planner.LogicalTableDual) (Executor, error) {
	return &TableDualExec{
		baseExecutor: newBaseExecutor(eb.Ctx, p.Schema(), p.ExplainID()),
		numDualRows:  p.RowCount,
	}, nil
}

func (eb *ExecutorBuilder) buildProjection(p *planner.LogicalProjection) (Executor, error) {
	childExec, err := eb.build(p.Children()[0])
	if err != nil {
		return nil, err
	}
	return &ProjExec{
		baseExecutor: newBaseExecutor(eb.Ctx, p.Schema(), p.ExplainID(), childExec),
		Exprs:        p.Exprs,
		colIndexMap:  p.ColIndexMap,
	}, nil
}

func (eb *ExecutorBuilder) buildTableScan(p *planner.DataSource) (Executor, error) {
	if p.SimpleScanPlan != nil {
		return eb.buildSimpleScanPlan(p.SimpleScanPlan)
	}
	return &TableScanExec{
		baseExecutor: newBaseExecutor(eb.Ctx, p.Schema(), p.ExplainID()),
		columns:      p.Columns,
		tbl:          p.Table(),
		IdxVals:      p.IdxVals,
		Index:        p.Index,
		PkVals:       p.PkVals,
		Limit:        p.Limit,
	}, nil
}

func (eb *ExecutorBuilder) buildSelection(p *planner.LogicalSelection) (Executor, error) {
	childExec, err := eb.build(p.Children()[0])
	if err != nil {
		return nil, err
	}
	return &SelectionExec{
		baseExecutor: newBaseExecutor(eb.Ctx, p.Schema(), p.ExplainID(), childExec),
		filters:      p.Conditions,
	}, nil
}

func (eb *ExecutorBuilder) buildShow(p *planner.LogicalShow) (Executor, error) {
	return &ShowExec{
		Tp:           p.Tp,
		baseExecutor: newBaseExecutor(eb.Ctx, p.Schema(), p.ExplainID()),
		Plan:         p,
		is:           eb.is,
	}, nil
}

func (eb *ExecutorBuilder) buildFaker(p planner.Plan) (Executor, error) {
	id := stringutil.MemoizeStr(func() string {
		return "faker"
	})
	return &FakerExec{
		baseExecutor: newBaseExecutor(eb.Ctx, nil, id),
	}, nil
}
