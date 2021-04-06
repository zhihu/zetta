package planner

import (
	"fmt"

	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/stringutil"
	"github.com/zhihu/zetta/tablestore/mysql/expression"
	"github.com/zhihu/zetta/tablestore/mysql/sctx"
)

type Plan interface {
	Schema() *expression.Schema
	Type() string
	// OutputNames returns the outputting names of each column.
	OutputNames() types.NameSlice

	// SetOutputNames sets the outputting name by the given slice.
	SetOutputNames(names types.NameSlice)
	ExplainID() fmt.Stringer
}

type LogicalPlan interface {
	Plan
	// Get all the children.
	Children() []LogicalPlan

	// SetChildren sets the children for the plan.
	SetChildren(...LogicalPlan)

	// SetChild sets the ith child for the plan.
	SetChild(i int, child LogicalPlan)

	// PruneColumns prunes the unused columns.
	PruneColumns([]*expression.Column) ([]*expression.Column, error)

	// Push the expression in where clause to datasource to use index.
	// Only selection need to implement this now.
	PushFilterDown([]expression.Expression) []expression.Expression

	PushLimitDown(uint64)
}

type PhysicalPlan interface {
	Plan
	// Get all the children.
	Children() []PhysicalPlan

	// SetChildren sets the children for the plan.
	SetChildren(...PhysicalPlan)

	// SetChild sets the ith child for the plan.
	SetChild(i int, child PhysicalPlan)
}

type basePlan struct {
	ctx    sctx.Context
	tp     string
	schema *expression.Schema
}

func newBasePlan(ctx sctx.Context, tp string) basePlan {
	return basePlan{
		tp:  tp,
		ctx: ctx,
	}
}

func (bp *basePlan) Type() string {
	return bp.tp
}

func (p *basePlan) ExplainID() fmt.Stringer {
	return stringutil.MemoizeStr(func() string {
		return p.tp
	})
}

func (bp *basePlan) Schema() *expression.Schema {
	return bp.schema
}

// OutputNames returns the outputting names of each column.
func (p *basePlan) OutputNames() types.NameSlice {
	return nil
}

func (p *basePlan) SetOutputNames(names types.NameSlice) {
}

type baseLogicalPlan struct {
	basePlan
	self     LogicalPlan
	children []LogicalPlan
}

func newBaseLogicalPlan(ctx sctx.Context, tp string, plan LogicalPlan) baseLogicalPlan {
	return baseLogicalPlan{
		basePlan: newBasePlan(ctx, tp),
		self:     plan,
		children: make([]LogicalPlan, 0),
	}
}

func (blp *baseLogicalPlan) Children() []LogicalPlan {
	return blp.children
}

func (blp *baseLogicalPlan) SetChildren(children ...LogicalPlan) {
	blp.children = children
}

func (blp *baseLogicalPlan) SetChild(i int, child LogicalPlan) {
	blp.children[i] = child
}

func (blp *baseLogicalPlan) PruneColumns(usedColumns []*expression.Column) ([]*expression.Column, error) {
	return usedColumns, nil
}

func (blp *baseLogicalPlan) PushFilterDown(exprs []expression.Expression) []expression.Expression {
	return nil
}

func (blp *baseLogicalPlan) PushLimitDown(limit uint64) {
	if len(blp.children) > 0 {
		blp.children[0].PushLimitDown(limit)
	}
}

type basePhysicalPlan struct {
	basePlan
	children []PhysicalPlan
	self     PhysicalPlan
}

func newBasePhysicalPlan(ctx sctx.Context, tp string, plan PhysicalPlan) *basePhysicalPlan {
	return &basePhysicalPlan{
		basePlan: newBasePlan(ctx, tp),
		self:     plan,
		children: make([]PhysicalPlan, 0),
	}
}

func (bpp *basePhysicalPlan) Children() []PhysicalPlan {
	return bpp.children
}
func (bpp *basePhysicalPlan) SetChildren(children ...PhysicalPlan) {
	bpp.children = children
}

func (bpp *basePhysicalPlan) SetChild(i int, child PhysicalPlan) {
	bpp.children[i] = child
}
