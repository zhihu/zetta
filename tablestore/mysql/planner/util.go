package planner

import (
	"github.com/pingcap/parser/ast"
	"github.com/zhihu/zetta/tablestore/mysql/expression"
	"github.com/zhihu/zetta/tablestore/mysql/sctx"
)

// AggregateFuncExtractor visits Expr tree.
// It converts ColunmNameExpr to AggregateFuncExpr and collects AggregateFuncExpr.
type AggregateFuncExtractor struct {
	inAggregateFuncExpr bool
	// AggFuncs is the collected AggregateFuncExprs.
	AggFuncs []*ast.AggregateFuncExpr
}

// Enter implements Visitor interface.
func (a *AggregateFuncExtractor) Enter(n ast.Node) (ast.Node, bool) {
	switch n.(type) {
	case *ast.AggregateFuncExpr:
		a.inAggregateFuncExpr = true
	case *ast.SelectStmt, *ast.UnionStmt:
		return n, true
	}
	return n, false
}

// Leave implements Visitor interface.
func (a *AggregateFuncExtractor) Leave(n ast.Node) (ast.Node, bool) {
	switch v := n.(type) {
	case *ast.AggregateFuncExpr:
		a.inAggregateFuncExpr = false
		a.AggFuncs = append(a.AggFuncs, v)
	}
	return n, true
}

// ByItems wraps a "by" item.
type ByItems struct {
	Expr expression.Expression
	Desc bool
}

// String implements fmt.Stringer interface.
//func (by *ByItems) String() string {
//	if by.Desc {
//return fmt.Sprintf("%s true", by.Expr)
//}
//return by.Expr.String()
//}

// Clone makes a copy of ByItems.
func (by *ByItems) Clone() *ByItems {
	return &ByItems{Expr: by.Expr.Clone(), Desc: by.Desc}
}

// Equal checks whether two ByItems are equal.
func (by *ByItems) Equal(ctx sctx.Context, other *ByItems) bool {
	return by.Expr.Equal(ctx, other.Expr) && by.Desc == other.Desc
}
