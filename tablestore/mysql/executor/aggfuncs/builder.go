package aggfuncs

import (
	"github.com/pingcap/parser/ast"
	"github.com/zhihu/zetta/tablestore/mysql/expression/aggregation"
	"github.com/zhihu/zetta/tablestore/mysql/sctx"
)

func Build(ctx sctx.Context, aggFuncDesc *aggregation.AggFuncDesc, ordinal int) AggFunc {
	switch aggFuncDesc.Name {
	case ast.AggFuncCount:
		return buildCount(aggFuncDesc, ordinal)
	}
	return nil
}

func buildCount(aggFuncDesc *aggregation.AggFuncDesc, ordinal int) AggFunc {
	base := baseAggFunc{
		args:    aggFuncDesc.Args,
		ordinal: ordinal,
	}
	return &partialCount{baseCount{base}}
}
