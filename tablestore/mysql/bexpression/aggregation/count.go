package aggregation

import (
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
)

type countFunction struct {
	aggFunction
}

// Update implements Aggregation interface.
func (cf *countFunction) Update(evalCtx *AggEvaluateContext, sc *stmtctx.StatementContext, row chunk.Row) error {
	for _, a := range cf.Args {
		value, err := a.Eval(row)
		if err != nil {
			return err
		}
		if value.IsNull() {
			return nil
		}
		evalCtx.Count++
	}
	return nil
}

func (cf *countFunction) ResetContext(sc *stmtctx.StatementContext, evalCtx *AggEvaluateContext) {
	evalCtx.Count = 0
}

// GetResult implements Aggregation interface.
func (cf *countFunction) GetResult(evalCtx *AggEvaluateContext) (d types.Datum) {
	d.SetInt64(evalCtx.Count)
	return d
}

// GetPartialResult implements Aggregation interface.
func (cf *countFunction) GetPartialResult(evalCtx *AggEvaluateContext) []types.Datum {
	return []types.Datum{cf.GetResult(evalCtx)}
}
