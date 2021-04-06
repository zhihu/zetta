package sqlexec

import (
	"context"

	"github.com/pingcap/parser/ast"
	"github.com/pingcap/tidb/util/chunk"
)

type SQLExecutor interface {
	Execute(ctx context.Context, sql string) (RecordSet, error)
	Close()
}

// RecordSet is an abstract result set interface to help get data from Plan.
type RecordSet interface {
	// Fields gets result fields.
	Fields() []*ast.ResultField

	// Next reads records into chunk.
	Next(ctx context.Context, req *chunk.Chunk) error

	// NewChunk create a chunk.
	NewChunk() *chunk.Chunk

	// Close closes the underlying iterator, call Next after Close will
	// restart the iteration.
	Close() error
}
