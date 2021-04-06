package expression

import (
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/zhihu/zetta/tablestore/mysql/sctx"
)

// Constant stands for a constant value.
type Constant struct {
	Value   types.Datum
	RetType *types.FieldType
	// DeferredExpr holds deferred function in PlanCache cached plan.
	// it's only used to represent non-deterministic functions(see expression.DeferredFunctions)
	// in PlanCache cached plan, so let them can be evaluated until cached item be used.
	//DeferredExpr Expression
	// ParamMarker holds param index inside sessionVars.PreparedParams.
	// It's only used to reference a user variable provided in the `EXECUTE` statement or `COM_EXECUTE` binary protocol.
	//ParamMarker *ParamMarker
	hashcode []byte

	//collationInfo
}

// Clone implements Expression interface.
func (c *Constant) Clone() Expression {
	con := *c
	return &con
}

// Equal implements Expression interface.
func (c *Constant) Equal(ctx sctx.Context, b Expression) bool {
	y, ok := b.(*Constant)
	if !ok {
		return false
	}
	_, err1 := y.Eval(chunk.Row{})
	_, err2 := c.Eval(chunk.Row{})
	if err1 != nil || err2 != nil {
		return false
	}
	con, err := c.Value.CompareDatum(ctx.GetSessionVars().StmtCtx, &y.Value)
	if err != nil || con != 0 {
		return false
	}
	return true
}

// GetType implements Expression interface.
func (c *Constant) GetType() *types.FieldType {
	return c.RetType
}

// Eval implements Expression interface.
func (c *Constant) Eval(row chunk.Row) (types.Datum, error) {
	return c.Value, nil
}

// EvalInt returns int representation of Constant.
func (c *Constant) EvalInt(ctx sctx.Context, row chunk.Row) (int64, bool, error) {
	dt := c.Value
	if c.GetType().Tp == mysql.TypeNull || dt.IsNull() {
		return 0, true, nil
	} else if dt.Kind() == types.KindBinaryLiteral {
		val, err := dt.GetBinaryLiteral().ToInt(ctx.GetSessionVars().StmtCtx)
		return int64(val), err != nil, err
	} else if c.GetType().Hybrid() || dt.Kind() == types.KindString {
		res, err := dt.ToInt64(ctx.GetSessionVars().StmtCtx)
		return res, false, err
	}
	return dt.GetInt64(), false, nil
}

// EvalString returns string representation of Constant.
func (c *Constant) EvalString(ctx sctx.Context, row chunk.Row) (string, bool, error) {
	dt := c.Value
	if c.GetType().Tp == mysql.TypeNull || dt.IsNull() {
		return "", true, nil
	}
	res, err := dt.ToString()
	return res, false, err
}
