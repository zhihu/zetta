package aggregation

import (
	"bytes"

	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/zhihu/zetta/tablestore/mysql/expression"
)

// Aggregation stands for aggregate functions.
type Aggregation interface {
	// Update during executing.
	Update(evalCtx *AggEvaluateContext, sc *stmtctx.StatementContext, row chunk.Row) error

	// GetPartialResult will called by coprocessor to get partial results. For avg function, partial results will return
	// sum and count values at the same time.
	GetPartialResult(evalCtx *AggEvaluateContext) []types.Datum

	// GetResult will be called when all data have been processed.
	GetResult(evalCtx *AggEvaluateContext) types.Datum

	// CreateContext creates a new AggEvaluateContext for the aggregation function.
	CreateContext(sc *stmtctx.StatementContext) *AggEvaluateContext

	// ResetContext resets the content of the evaluate context.
	ResetContext(sc *stmtctx.StatementContext, evalCtx *AggEvaluateContext)
}

// AggEvaluateContext is used to store intermediate result when calculating aggregate functions.
type AggEvaluateContext struct {
	DistinctChecker *distinctChecker
	Count           int64
	Value           types.Datum
	Buffer          *bytes.Buffer // Buffer is used for group_concat.
	GotFirstRow     bool          // It will check if the agg has met the first row key.
}

type aggFunction struct {
	*AggFuncDesc
}

func newAggFunc(funcName string, args []expression.Expression, hasDistinct bool) aggFunction {
	agg := &AggFuncDesc{HasDistinct: hasDistinct}
	agg.Name = funcName
	agg.Args = args
	return aggFunction{AggFuncDesc: agg}
}

// CreateContext implements Aggregation interface.
func (af *aggFunction) CreateContext(sc *stmtctx.StatementContext) *AggEvaluateContext {
	evalCtx := &AggEvaluateContext{}
	return evalCtx
}

func (af *aggFunction) ResetContext(sc *stmtctx.StatementContext, evalCtx *AggEvaluateContext) {
	evalCtx.Value.SetNull()
}

func (af *aggFunction) updateSum(sc *stmtctx.StatementContext, evalCtx *AggEvaluateContext, row chunk.Row) error {
	a := af.Args[0]
	value, err := a.Eval(row)
	if err != nil {
		return err
	}
	if value.IsNull() {
		return nil
	}

	evalCtx.Value, err = calculateSum(sc, evalCtx.Value, value)
	if err != nil {
		return err
	}
	evalCtx.Count++
	return nil
}
