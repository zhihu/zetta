package expression

import (
	"fmt"
	"strings"

	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/zhihu/zetta/pkg/model"
	"github.com/zhihu/zetta/tablestore/mysql/sctx"
)

// Column represents a column.
type Column struct {
	RetType *types.FieldType
	// ID is used to specify whether this column is ExtraHandleColumn or to access histogram.
	// We'll try to remove it in the future.
	ID int64
	// UniqueID is the unique id of this column.
	UniqueID int64

	// Index is used for execution, to tell the column's position in the given row.
	Index int

	hashcode []byte

	// VirtualExpr is used to save expression for virtual column
	//VirtualExpr Expression

	OrigName string
	IsHidden bool

	// InOperand indicates whether this column is the inner operand of column equal condition converted
	// from `[not] in (subq)`.
	//InOperand bool

	//collationInfo
	Family string
}

const columnPrefix = "Column#"

// String implements Stringer interface.
func (col *Column) String() string {
	if col.OrigName != "" {
		return col.OrigName
	}
	var builder strings.Builder
	fmt.Fprintf(&builder, "%s%d", columnPrefix, col.UniqueID)
	return builder.String()
}

// GetType implements Expression interface.
func (col *Column) GetType() *types.FieldType {
	return col.RetType
}

// Eval implements Expression interface.
func (col *Column) Eval(row chunk.Row) (types.Datum, error) {
	return row.GetDatum(col.Index, col.RetType), nil
}

// EvalInt returns int representation of Column.
func (col *Column) EvalInt(ctx sctx.Context, row chunk.Row) (int64, bool, error) {
	if col.GetType().Hybrid() {
		val := row.GetDatum(col.Index, col.RetType)
		if val.IsNull() {
			return 0, true, nil
		}
		if val.Kind() == types.KindMysqlBit {
			val, err := val.GetBinaryLiteral().ToInt(ctx.GetSessionVars().StmtCtx)
			return int64(val), err != nil, err
		}
		res, err := val.ToInt64(ctx.GetSessionVars().StmtCtx)
		return res, err != nil, err
	}
	if row.IsNull(col.Index) {
		return 0, true, nil
	}
	return row.GetInt64(col.Index), false, nil
}

// EvalString returns string representation of Column.
func (col *Column) EvalString(ctx sctx.Context, row chunk.Row) (string, bool, error) {
	if row.IsNull(col.Index) {
		return "", true, nil
	}

	// Specially handle the ENUM/SET/BIT input value.
	if col.GetType().Hybrid() {
		val := row.GetDatum(col.Index, col.RetType)
		res, err := val.ToString()
		return res, err != nil, err
	}

	val := row.GetString(col.Index)
	return val, false, nil
}

// Equal implements Expression interface.
func (col *Column) Equal(_ sctx.Context, expr Expression) bool {
	if newCol, ok := expr.(*Column); ok {
		return newCol.UniqueID == col.UniqueID
	}
	return false
}

// Clone implements Expression interface.
func (col *Column) Clone() Expression {
	newCol := *col
	return &newCol
}

func (col *Column) ToColumnMeta() *model.ColumnMeta {
	cm := &model.ColumnMeta{}
	cm.ColumnMeta.Name = col.OrigName
	cm.FieldType = *col.RetType
	cm.Family = col.Family
	return cm
}
