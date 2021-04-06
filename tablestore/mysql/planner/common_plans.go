package planner

import (
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/tidb/types"
	"github.com/zhihu/zetta/pkg/model"
	"github.com/zhihu/zetta/tablestore/mysql/expression"
)

// Simple represents a simple statement plan which doesn't need any optimization.
type Simple struct {
	baseSchemaProducer

	Statement ast.StmtNode
}

type Set struct {
	baseSchemaProducer

	VarAssigns []*expression.VarAssignment
}

type SplitRegion struct {
	baseSchemaProducer

	IndexMeta  *model.IndexMeta
	TableMeta  *model.TableMeta
	Lower      []types.Datum
	Upper      []types.Datum
	ValueLists [][]types.Datum
	Num        int
}
