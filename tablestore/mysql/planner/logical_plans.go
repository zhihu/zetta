package planner

import (
	"github.com/pingcap/parser/ast"
	parser_model "github.com/pingcap/parser/model"
	"github.com/pingcap/parser/opcode"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/plancodec"
	"github.com/zhihu/zetta/pkg/model"
	"github.com/zhihu/zetta/tablestore/mysql/expression"
	"github.com/zhihu/zetta/tablestore/mysql/expression/aggregation"
	"github.com/zhihu/zetta/tablestore/mysql/sctx"
	"github.com/zhihu/zetta/tablestore/table"
	"github.com/zhihu/zetta/tablestore/util"
)

type LogicalUpdate struct {
	baseLogicalPlan
	Table table.Table

	ColValues  map[int64]types.Datum
	DataSource Plan
}

func (p LogicalUpdate) Init(ctx sctx.Context) *LogicalUpdate {
	p.baseLogicalPlan = newBaseLogicalPlan(ctx, plancodec.TypeUpdate, &p)
	return &p
}

// LogicalSelection represents a where or having predicate.
type LogicalSelection struct {
	baseLogicalPlan

	// Originally the WHERE or ON condition is parsed into a single expression,
	// but after we converted to CNF(Conjunctive normal form), it can be
	// split into a list of AND conditions.
	Conditions []expression.Expression
}

// Init initializes LogicalSelection.
func (p LogicalSelection) Init(ctx sctx.Context) *LogicalSelection {
	p.baseLogicalPlan = newBaseLogicalPlan(ctx, plancodec.TypeSel, &p)
	return &p
}

func (p *LogicalSelection) Schema() *expression.Schema {
	return p.Children()[0].Schema()
}

func (p *LogicalSelection) OutputNames() types.NameSlice {
	return p.Children()[0].OutputNames()
}

func (p *LogicalSelection) PushFilterDown(exprs []expression.Expression) []expression.Expression {
	child := p.children[0]
	if datasource, ok := child.(*DataSource); ok {
		return datasource.PushFilterDown(exprs)
	}
	return exprs
}

// TODO: Should prune columns based on cols related in where condition.
func (p *LogicalSelection) PruneColumns(usedColumns []*expression.Column) ([]*expression.Column, error) {
	return p.Children()[0].Schema().Columns, nil
}

// ShowContents stores the contents for the `SHOW` statement.
type ShowContents struct {
	Tp        ast.ShowStmtType // Databases/Tables/Columns/....
	DBName    string
	Table     *ast.TableName  // Used for showing columns.
	Column    *ast.ColumnName // Used for `desc table column`.
	IndexName parser_model.CIStr
}

// LogicalShow represents a show plan.
type LogicalShow struct {
	logicalSchemaProducer
	ShowContents
}

// Init initializes LogicalSelection.
func (p LogicalShow) Init(ctx sctx.Context) *LogicalShow {
	p.baseLogicalPlan = newBaseLogicalPlan(ctx, plancodec.TypeShow, &p)
	return &p
}

// DataSource represents a tableScan without condition push down.
type DataSource struct {
	logicalSchemaProducer

	table     table.Table
	tableInfo *model.TableMeta
	Columns   []*model.ColumnMeta

	DBName      parser_model.CIStr
	TableAsName *parser_model.CIStr

	TblCols []*expression.Column

	IdxVals []types.Datum
	Index   *model.IndexMeta

	PkVals []types.Datum
	Limit  uint64

	SimpleScanPlan *SimpleScanPlan
}

func (d DataSource) Init(ctx sctx.Context) *DataSource {
	d.baseLogicalPlan = newBaseLogicalPlan(ctx, plancodec.TypeDataSource, &d)
	return &d
}

func (d *DataSource) Table() table.Table {
	return d.table
}

func (d *DataSource) PruneColumns(usedColumns []*expression.Column) ([]*expression.Column, error) {
	usedCols := make([]*model.ColumnMeta, len(usedColumns))
	for i, uc := range usedColumns {
		usedCols[i] = d.table.Meta().FindColumnByName(uc.OrigName)
	}
	d.Columns = usedCols
	d.schema.Columns = usedColumns
	return d.schema.Columns, nil
}

func (d *DataSource) PushFilterDown(exprs []expression.Expression) []expression.Expression {
	left, selected := selectExprsByIndex(d.table.Meta(), exprs)
	if selected != nil && selected.meta != nil && selected.score >= 100 {
		d.SimpleScanPlan = d.buildSimpleScanPlan(selected)
		return left
	}
	return exprs
}

func (d *DataSource) buildSimpleScanPlan(selected *indexSelection) *SimpleScanPlan {
	lowers := make([]*util.Bound, len(selected.meta.DefinedColumns))
	uppers := make([]*util.Bound, len(selected.meta.DefinedColumns))
	for i := range selected.exprs {
		l, u := d.buildLowUpper(selected, i)
		lowers[selected.offsets[i]] = l
		uppers[selected.offsets[i]] = u
	}
	ssp := SimpleScanPlan{
		ctx:       d.ctx,
		IndexMeta: selected.meta,
		Table:     d.table,
		Upper:     uppers,
		Lower:     lowers,
		Limit:     int64(d.Limit),
	}.Init(d.ctx, d.Schema(), d.OutputNames())
	return ssp
}

func (d *DataSource) buildLowUpper(selected *indexSelection, offset int) (lower *util.Bound, upper *util.Bound) {
	lower = &util.Bound{}
	upper = &util.Bound{}
	if x, ok := selected.exprs[offset].(*expression.ScalarFunction); !ok {
		return nil, nil
	} else {
		switch x.FuncName.L {
		case opcode.LT.String():
			lower = nil
		case opcode.LE.String():
			lower = nil
			upper.Included = true
		case opcode.GT.String():
			upper = nil
		case opcode.GE.String():
			upper = nil
			lower.Included = true
		case opcode.EQ.String():
			upper.Included = true
			lower.Included = true
		default:
			return nil, nil
		}
	}
	if lower != nil {
		lower.Value = &selected.datums[offset]
	}
	if upper != nil {
		upper.Value = &selected.datums[offset]
	}
	return
}

func (d *DataSource) PushFilterDown2(exprs []expression.Expression) []expression.Expression {
	leaves := []expression.Expression{}
	for i, expr := range exprs {
		switch x := expr.(type) {
		case *expression.ScalarFunction:
			// Only support "="" operation now.
			if x.FuncName.L != opcode.EQ.String() {
				leaves = append(leaves, expr)
				continue
			}
			args := x.GetArgs()
			if col, ok := args[0].(*expression.Column); ok {
				if value, ok := args[1].(*expression.Constant); ok {
					for _, pk := range d.tableInfo.PrimaryKey {
						if pk == col.OrigName && len(d.tableInfo.PrimaryKey) == 1 {
							dt, _ := value.Value.ConvertTo(&stmtctx.StatementContext{}, col.GetType())
							d.PkVals = []types.Datum{dt}
						}
					}
					if _, index := d.tableInfo.GetIndexByColumnName(col.OrigName); index != nil {
						value.Value.ConvertTo(&stmtctx.StatementContext{}, col.GetType())
						d.IdxVals = []types.Datum{value.Value}
						d.Index = index
						leaves = append(leaves, exprs[i+1:]...)
						return leaves
					}
				}
			}
		}
		leaves = append(leaves, expr)
	}
	return leaves
}

func (d *DataSource) PushLimitDown(limit uint64) {
	if d.SimpleScanPlan != nil {
		d.SimpleScanPlan.Limit = int64(limit)
	}
	d.Limit = limit
}

type LogicalProjection struct {
	logicalSchemaProducer
	selectFields []*ast.SelectField
	Exprs        []expression.Expression

	// key -> col index in proj schema
	// value -> col index in row fetched(table scan schema)
	ColIndexMap map[int]int
}

func (p LogicalProjection) Init(ctx sctx.Context) *LogicalProjection {
	p.baseLogicalPlan = newBaseLogicalPlan(ctx, plancodec.TypeProj, &p)
	p.ColIndexMap = make(map[int]int)
	return &p
}

func (p *LogicalProjection) PruneColumns(usedColumns []*expression.Column) ([]*expression.Column, error) {
	child := p.children[0]
	return child.PruneColumns(usedColumns)
}

// LogicalTableDual represents a dual table plan.
type LogicalTableDual struct {
	logicalSchemaProducer

	RowCount int
}

// Init initializes LogicalTableDual.
func (p LogicalTableDual) Init(ctx sctx.Context) *LogicalTableDual {
	p.baseLogicalPlan = newBaseLogicalPlan(ctx, plancodec.TypeDual, &p)
	return &p
}

type DDL struct {
	baseSchemaProducer

	Statement ast.DDLNode
}

type Insert struct {
	baseSchemaProducer

	Table         table.Table
	tableSchema   *expression.Schema
	tableColNames types.NameSlice
	Columns       []*ast.ColumnName
	Lists         [][]expression.Expression
	DefaultCF     bool
}

func (i Insert) Init(ctx sctx.Context) *Insert {
	i.basePlan = newBasePlan(ctx, plancodec.TypeInsert)
	return &i
}

type Delete struct {
	baseSchemaProducer

	Table              table.Table
	SelectionPlan      Plan
	SimplePointGetPlan Plan
}

func (p Delete) Init(ctx sctx.Context) *Delete {
	p.basePlan = newBasePlan(ctx, plancodec.TypeDelete)
	return &p
}

// LogicalLimit represents offset and limit plan.
type LogicalLimit struct {
	baseLogicalPlan

	Offset uint64
	Count  uint64
}

// Init initializes LogicalLimit.
func (p LogicalLimit) Init(ctx sctx.Context) *LogicalLimit {
	p.baseLogicalPlan = newBaseLogicalPlan(ctx, plancodec.TypeLimit, &p)
	return &p
}

func (p *LogicalLimit) Schema() *expression.Schema {
	return p.children[0].Schema()
}

func (p *LogicalLimit) OutputNames() types.NameSlice {
	return p.Children()[0].OutputNames()
}

type LogicalSort struct {
	baseLogicalPlan

	ByItems []*ByItems
}

func (p LogicalSort) Init(ctx sctx.Context) *LogicalSort {
	p.baseLogicalPlan = newBaseLogicalPlan(ctx, plancodec.TypeSort, &p)
	return &p
}

func (p *LogicalSort) Schema() *expression.Schema {
	return p.children[0].Schema()
}

func (p *LogicalSort) OutputNames() types.NameSlice {
	return p.Children()[0].OutputNames()
}

//Sort can not push limit down unless the by item has an index.
//Ignore the situation for now.
func (p *LogicalSort) PushLimitDown(limit uint64) {}

type LogicalAggregation struct {
	logicalSchemaProducer

	AggFuncs []*aggregation.AggFuncDesc
	Table    table.Table
}

func (la LogicalAggregation) Init(ctx sctx.Context) *LogicalAggregation {
	la.baseLogicalPlan = newBaseLogicalPlan(ctx, plancodec.TypeAgg, &la)
	return &la
}

func (p *LogicalAggregation) PushLimitDown(limit uint64) {}

// GetUsedCols extracts all of the Columns used by agg including GroupByItems and AggFuncs.
func (la *LogicalAggregation) GetUsedCols() (usedCols []*expression.Column) {
	for _, aggDesc := range la.AggFuncs {
		for _, expr := range aggDesc.Args {
			usedCols = append(usedCols, expression.ExtractColumns(expr)...)
		}
	}
	return usedCols
}
