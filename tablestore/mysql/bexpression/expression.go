package expression

import (
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/zhihu/zetta/tablestore/mysql/sctx"
)

// Expression represents all scalar expression in SQL.
type Expression interface {
	// Eval evaluates an expression through a row.
	Eval(row chunk.Row) (types.Datum, error)

	// EvalInt returns the int64 representation of expression.
	EvalInt(ctx sctx.Context, row chunk.Row) (val int64, isNull bool, err error)

	// EvalReal returns the float64 representation of expression.
	//EvalReal(ctx sessionctx.Context, row chunk.Row) (val float64, isNull bool, err error)

	// EvalString returns the string representation of expression.
	EvalString(ctx sctx.Context, row chunk.Row) (val string, isNull bool, err error)

	// GetType gets the type that the expression returns.
	GetType() *types.FieldType

	// Equal checks whether two expressions are equal.
	Equal(ctx sctx.Context, e Expression) bool

	// Clone copies an expression totally.
	Clone() Expression
}

// FindFieldName finds the column name from NameSlice.
func FindFieldName(names types.NameSlice, astCol *ast.ColumnName) (int, error) {
	dbName, tblName, colName := astCol.Schema, astCol.Table, astCol.Name
	idx := -1
	for i, name := range names {
		if (dbName.L == "" || dbName.L == name.DBName.L) &&
			(tblName.L == "" || tblName.L == name.TblName.L) &&
			(colName.L == name.ColName.L) {
			if idx == -1 {
				idx = i
			} else {
				return -1, errNonUniq.GenWithStackByArgs(name.String(), "field list")
			}
		}
	}
	return idx, nil
}

// CNFExprs stands for a CNF expression.
type CNFExprs []Expression

// Clone clones itself.
func (e CNFExprs) Clone() CNFExprs {
	cnf := make(CNFExprs, 0, len(e))
	for _, expr := range e {
		cnf = append(cnf, expr.Clone())
	}
	return cnf
}

// Shallow makes a shallow copy of itself.
func (e CNFExprs) Shallow() CNFExprs {
	cnf := make(CNFExprs, 0, len(e))
	cnf = append(cnf, e...)
	return cnf
}

// GetRowLen gets the length if the func is row, returns 1 if not row.
func GetRowLen(e Expression) int {
	if f, ok := e.(*ScalarFunction); ok && f.FuncName.L == ast.RowFunc {
		return len(f.GetArgs())
	}
	return 1
}

// DatumToConstant generates a Constant expression from a Datum.
func DatumToConstant(d types.Datum, tp byte) *Constant {
	return &Constant{Value: d, RetType: types.NewFieldType(tp)}
}

// EvalBool evaluates expression list to a boolean value. The first returned value
// indicates bool result of the expression list, the second returned value indicates
// whether the result of the expression list is null, it can only be true when the
// first returned values is false.
func EvalBool(ctx sctx.Context, exprList CNFExprs, row chunk.Row) (bool, bool, error) {
	for _, expr := range exprList {
		data, err := expr.Eval(row)
		if err != nil {
			return false, false, err
		}
		if data.IsNull() {
			return false, false, nil
		}

		i, err := data.ToBool(ctx.GetSessionVars().StmtCtx)
		if err != nil {
			return false, false, err
		}
		if i == 0 {
			return false, false, nil
		}
	}
	return true, false, nil
}

type VarAssignment struct {
	Name        string
	Expr        Expression
	IsDefault   bool
	IsGlobal    bool
	IsSystem    bool
	ExtendValue *Constant
}
