package aggregation

import (
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
)

type sumFunction struct {
	aggFunction
}

// Update implements Aggregation interface.
func (sf *sumFunction) Update(evalCtx *AggEvaluateContext, sc *stmtctx.StatementContext, row chunk.Row) error {
	return sf.updateSum(sc, evalCtx, row)
}

// GetResult implements Aggregation interface.
func (sf *sumFunction) GetResult(evalCtx *AggEvaluateContext) (d types.Datum) {
	return evalCtx.Value
}

// GetPartialResult implements Aggregation interface.
func (sf *sumFunction) GetPartialResult(evalCtx *AggEvaluateContext) []types.Datum {
	return []types.Datum{sf.GetResult(evalCtx)}
}
