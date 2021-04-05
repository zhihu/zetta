package aggregation

import (
	"github.com/pingcap/parser/ast"
	"github.com/zhihu/zetta/tablestore/mysql/expression"
	"github.com/zhihu/zetta/tablestore/mysql/sctx"
)

// AggFuncDesc describes an aggregation function signature, only used in planner.
type AggFuncDesc struct {
	baseFuncDesc
	// Mode represents the execution mode of the aggregation function.
	//Mode AggFunctionMode
	// HasDistinct represents whether the aggregation function contains distinct attribute.
	HasDistinct bool
	// OrderByItems represents the order by clause used in GROUP_CONCAT
	//OrderByItems []*util.ByItems
}

// NewAggFuncDesc creates an aggregation function signature descriptor.
func NewAggFuncDesc(ctx sctx.Context, name string, args []expression.Expression, hasDistinct bool) (*AggFuncDesc, error) {
	b, err := newBaseFuncDesc(ctx, name, args)
	if err != nil {
		return nil, err
	}
	return &AggFuncDesc{baseFuncDesc: b, HasDistinct: hasDistinct}, nil
}

// GetAggFunc gets an evaluator according to the aggregation function signature.
func (a *AggFuncDesc) GetAggFunc(ctx sctx.Context) Aggregation {
	aggFunc := aggFunction{AggFuncDesc: a}
	switch a.Name {
	case ast.AggFuncSum:
		return &sumFunction{aggFunction: aggFunc}
	case ast.AggFuncCount:
		return &countFunction{aggFunction: aggFunc}
	case ast.AggFuncAvg:
		return &avgFunction{aggFunction: aggFunc}
	case ast.AggFuncMax:
		return &maxMinFunction{aggFunction: aggFunc, isMax: true}
	case ast.AggFuncMin:
		return &maxMinFunction{aggFunction: aggFunc, isMax: false}
	default:
		panic("unsupported agg function")
	}
}
