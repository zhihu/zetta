package executor

import (
	"context"

	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/zhihu/zetta/tablestore/table"
)

type UpdateExec struct {
	baseExecutor

	tbl    table.Table
	values map[int64]types.Datum
}

func (e *UpdateExec) Open(ctx context.Context) error {
	return e.children[0].Open(ctx)
}

func (e *UpdateExec) Close() error {
	return e.children[0].Close()
}

func (e *UpdateExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.GrowAndReset(e.maxChunkSize)
	chk := NewFirstChunk(e.children[0])
	err := Next(ctx, e.children[0], chk)
	if err != nil {
		return err
	}
	fts := retTypes(e.children[0])
	chunkIter := chunk.NewIterator4Chunk(chk)
	rowIter := chunkIter.Begin()
	for ; rowIter != chunkIter.End(); rowIter = chunkIter.Next() {
		rowDatums := rowIter.GetDatumRow(fts)
		if err = e.tbl.UpdateRecord(e.ctx, rowDatums, e.values); err != nil {
			return err
		}
		e.ctx.GetSessionVars().StmtCtx.AddAffectedRows(1)
	}
	return nil
}

type DeleteExec struct {
	baseExecutor

	tbl table.Table
}

func (e *DeleteExec) Open(ctx context.Context) error {
	return e.children[0].Open(ctx)
}

func (e *DeleteExec) Close() error {
	return e.children[0].Close()
}

func (e *DeleteExec) deleteOneRow(ctx context.Context, row []types.Datum) error {
	return e.tbl.RemoveRecord(e.ctx, row)
}

func (e *DeleteExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.GrowAndReset(e.maxChunkSize)
	chk := NewFirstChunk(e.children[0])
	err := Next(ctx, e.children[0], chk)
	if err != nil {
		return err
	}
	fts := retTypes(e.children[0])
	chunkIter := chunk.NewIterator4Chunk(chk)
	rowIter := chunkIter.Begin()
	for ; rowIter != chunkIter.End(); rowIter = chunkIter.Next() {
		rowDatums := rowIter.GetDatumRow(fts)
		if err = e.tbl.RemoveRecord(e.ctx, rowDatums); err != nil {
			return err
		}
		e.ctx.GetSessionVars().StmtCtx.AddAffectedRows(1)
	}
	return nil
}
