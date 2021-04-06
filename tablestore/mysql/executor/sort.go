package executor

import (
	"container/heap"
	"context"
	"errors"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/disk"
	"github.com/pingcap/tidb/util/memory"
	"github.com/zhihu/zetta/tablestore/mysql/expression"
	"github.com/zhihu/zetta/tablestore/mysql/planner"
)

// SortExec represents sorting executor.
type SortExec struct {
	baseExecutor

	ByItems []*planner.ByItems
	Idx     int
	fetched bool
	schema  *expression.Schema

	keyExprs []expression.Expression
	keyTypes []*types.FieldType
	// keyColumns is the column index of the by items.
	keyColumns []int
	// keyCmpFuncs is used to compare each ByItem.
	keyCmpFuncs []chunk.CompareFunc
	// rowChunks is the chunks to store row values.
	rowChunks *chunk.SortedRowContainer

	memTracker  *memory.Tracker
	diskTracker *disk.Tracker

	// partitionList is the chunks to store row values for partitions. Every partition is a sorted list.
	partitionList []*chunk.SortedRowContainer

	// multiWayMerge uses multi-way merge for spill disk.
	// The multi-way merge algorithm can refer to https://en.wikipedia.org/wiki/K-way_merge_algorithm
	multiWayMerge *multiWayMerge
	// spillAction save the Action for spill disk.
	spillAction *chunk.SortAndSpillDiskAction
}

// Close implements the Executor Close interface.
func (e *SortExec) Close() error {
	for _, container := range e.partitionList {
		err := container.Close()
		if err != nil {
			return err
		}
	}
	e.partitionList = e.partitionList[:0]

	if e.rowChunks != nil {
		e.memTracker.Consume(-e.rowChunks.GetMemTracker().BytesConsumed())
		e.rowChunks = nil
	}
	e.memTracker = nil
	e.diskTracker = nil
	e.multiWayMerge = nil
	e.spillAction = nil
	return e.children[0].Close()
}

// Open implements the Executor Open interface.
func (e *SortExec) Open(ctx context.Context) error {
	e.fetched = false
	e.Idx = 0

	// To avoid duplicated initialization for TopNExec.
	if e.memTracker == nil {
		e.memTracker = memory.NewTracker(e.id, -1)
		//e.memTracker.AttachTo(e.ctx.GetSessionVars().StmtCtx.MemTracker)
		e.diskTracker = memory.NewTracker(e.id, -1)
		//e.diskTracker.AttachTo(e.ctx.GetSessionVars().StmtCtx.DiskTracker)
	}
	e.partitionList = e.partitionList[:0]
	return e.children[0].Open(ctx)
}

// Next implements the Executor Next interface.
// Sort constructs the result following these step:
// 1. Read as mush as rows into memory.
// 2. If memory quota is triggered, sort these rows in memory and put them into disk as partition 1, then reset
//    the memory quota trigger and return to step 1
// 3. If memory quota is not triggered and child is consumed, sort these rows in memory as partition N.
// 4. Merge sort if the count of partitions is larger than 1. If there is only one partition in step 4, it works
//    just like in-memory sort before.
func (e *SortExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	if !e.fetched {
		e.initCompareFuncs()
		e.buildKeyColumns()
		err := e.fetchRowChunks(ctx)
		if err != nil {
			return err
		}
		e.fetched = true
	}

	if len(e.partitionList) == 0 {
		return nil
	}
	if len(e.partitionList) > 1 {
		if err := e.externalSorting(req); err != nil {
			return err
		}
	} else {
		for !req.IsFull() && e.Idx < e.partitionList[0].NumRow() {
			row, err := e.partitionList[0].GetSortedRow(e.Idx)
			if err != nil {
				return err
			}
			req.AppendRow(row)
			e.Idx++
		}
	}
	return nil
}

type partitionPointer struct {
	row         chunk.Row
	partitionID int
	consumed    int
}

type multiWayMerge struct {
	lessRowFunction func(rowI chunk.Row, rowJ chunk.Row) bool
	elements        []partitionPointer
}

func (h *multiWayMerge) Less(i, j int) bool {
	rowI := h.elements[i].row
	rowJ := h.elements[j].row
	return h.lessRowFunction(rowI, rowJ)
}

func (h *multiWayMerge) Len() int {
	return len(h.elements)
}

func (h *multiWayMerge) Push(x interface{}) {
	// Should never be called.
}

func (h *multiWayMerge) Pop() interface{} {
	h.elements = h.elements[:len(h.elements)-1]
	return nil
}

func (h *multiWayMerge) Swap(i, j int) {
	h.elements[i], h.elements[j] = h.elements[j], h.elements[i]
}

func (e *SortExec) externalSorting(req *chunk.Chunk) (err error) {
	if e.multiWayMerge == nil {
		e.multiWayMerge = &multiWayMerge{e.lessRow, make([]partitionPointer, 0, len(e.partitionList))}
		for i := 0; i < len(e.partitionList); i++ {
			row, err := e.partitionList[i].GetSortedRow(0)
			if err != nil {
				return err
			}
			e.multiWayMerge.elements = append(e.multiWayMerge.elements, partitionPointer{row: row, partitionID: i, consumed: 0})
		}
		heap.Init(e.multiWayMerge)
	}

	for !req.IsFull() && e.multiWayMerge.Len() > 0 {
		partitionPtr := e.multiWayMerge.elements[0]
		req.AppendRow(partitionPtr.row)
		partitionPtr.consumed++
		if partitionPtr.consumed >= e.partitionList[partitionPtr.partitionID].NumRow() {
			heap.Remove(e.multiWayMerge, 0)
			continue
		}
		partitionPtr.row, err = e.partitionList[partitionPtr.partitionID].
			GetSortedRow(partitionPtr.consumed)
		if err != nil {
			return err
		}
		e.multiWayMerge.elements[0] = partitionPtr
		heap.Fix(e.multiWayMerge, 0)
	}
	return nil
}

func (e *SortExec) fetchRowChunks(ctx context.Context) error {
	fields := retTypes(e)
	byItemsDesc := make([]bool, len(e.ByItems))
	for i, byItem := range e.ByItems {
		byItemsDesc[i] = byItem.Desc
	}
	e.rowChunks = chunk.NewSortedRowContainer(fields, e.maxChunkSize, byItemsDesc, e.keyColumns, e.keyCmpFuncs)
	e.rowChunks.GetMemTracker().AttachTo(e.memTracker)
	if config.GetGlobalConfig().OOMUseTmpStorage {
		e.spillAction = e.rowChunks.ActionSpill()
		failpoint.Inject("testSortedRowContainerSpill", func(val failpoint.Value) {
			if val.(bool) {
				e.spillAction = e.rowChunks.ActionSpillForTest()
				defer e.spillAction.WaitForTest()
			}
		})
		//e.ctx.GetSessionVars().StmtCtx.MemTracker.FallbackOldAndSetNewAction(e.spillAction)
		e.rowChunks.GetDiskTracker().AttachTo(e.diskTracker)
	}
	for {
		chk := NewFirstChunk(e.children[0])
		err := Next(ctx, e.children[0], chk)
		if err != nil {
			return err
		}
		rowCount := chk.NumRows()
		if rowCount == 0 {
			break
		}
		if err := e.rowChunks.Add(chk); err != nil {
			if errors.Is(err, chunk.ErrCannotAddBecauseSorted) {
				e.partitionList = append(e.partitionList, e.rowChunks)
				e.rowChunks = chunk.NewSortedRowContainer(fields, e.maxChunkSize, byItemsDesc, e.keyColumns, e.keyCmpFuncs)
				e.rowChunks.GetMemTracker().AttachTo(e.memTracker)
				e.rowChunks.GetDiskTracker().AttachTo(e.diskTracker)
				e.spillAction = e.rowChunks.ActionSpill()
				failpoint.Inject("testSortedRowContainerSpill", func(val failpoint.Value) {
					if val.(bool) {
						e.spillAction = e.rowChunks.ActionSpillForTest()
						defer e.spillAction.WaitForTest()
					}
				})
				//e.ctx.GetSessionVars().StmtCtx.MemTracker.FallbackOldAndSetNewAction(e.spillAction)
				err = e.rowChunks.Add(chk)
			}
			if err != nil {
				return err
			}
		}
	}
	if e.rowChunks.NumRow() > 0 {
		e.rowChunks.Sort()
		e.partitionList = append(e.partitionList, e.rowChunks)
	}
	return nil
}

func (e *SortExec) initCompareFuncs() {
	e.keyCmpFuncs = make([]chunk.CompareFunc, len(e.ByItems))
	for i := range e.ByItems {
		keyType := e.ByItems[i].Expr.GetType()
		e.keyCmpFuncs[i] = chunk.GetCompareFunc(keyType)
	}
}

func (e *SortExec) buildKeyColumns() {
	e.keyColumns = make([]int, 0, len(e.ByItems))
	for _, by := range e.ByItems {
		col := by.Expr.(*expression.Column)
		e.keyColumns = append(e.keyColumns, col.Index)
	}
}

func (e *SortExec) lessRow(rowI, rowJ chunk.Row) bool {
	for i, colIdx := range e.keyColumns {
		cmpFunc := e.keyCmpFuncs[i]
		cmp := cmpFunc(rowI, colIdx, rowJ, colIdx)
		if e.ByItems[i].Desc {
			cmp = -cmp
		}
		if cmp < 0 {
			return true
		} else if cmp > 0 {
			return false
		}
	}
	return false
}
