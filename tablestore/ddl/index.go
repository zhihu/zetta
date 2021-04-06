// Copyright 2020 Zhizhesihai (Beijing) Technology Limited.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package ddl

import (
	"bytes"
	"context"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/zhihu/zetta/pkg/meta"
	"github.com/zhihu/zetta/pkg/model"

	//"github.com/zhihu/zetta/pkg/store/tikv"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/zhihu/zetta/pkg/tablecodec"
	"github.com/zhihu/zetta/tablestore/table"
	"github.com/zhihu/zetta/tablestore/table/tables"
	"go.uber.org/zap"
)

func onAddIndex(d *ddlCtx, t *meta.Meta, job *model.Job) (ver int64, err error) {
	if job.IsRollingback() {
		ver, err = onDropIndex(t, job)
		if err != nil {
			return ver, errors.Trace(err)
		}
		return
	}

	// Handle normal job.
	schemaID := job.SchemaID
	tblInfo, err := getTableInfoAndCancelFaultJob(t, job, schemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}

	idxMeta := model.IndexMeta{}
	err = job.DecodeArgs(&idxMeta)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	idxInfo := tblInfo.FindIndexByName(idxMeta.Name)
	if idxInfo != nil && idxInfo.State == model.StatePublic {
		job.State = model.JobStateCancelled
		err = ErrDupKeyName.GenWithStack("index already exist %s", idxMeta.Name)
		return ver, err
	}
	if idxInfo == nil {
		idxInfo = &idxMeta
		idxInfo.Id = allocateIndexID(tblInfo)
		idxInfo.State = model.StateNone
		tblInfo.Indices = append(tblInfo.Indices, idxInfo)
	}
	originalState := idxInfo.State

	switch idxInfo.State {
	case model.StateNone:
		// none -> delete only
		job.SchemaState = model.StateDeleteOnly
		idxInfo.State = model.StateDeleteOnly
		ver, err = updateVersionAndTableInfoWithCheck(t, job, tblInfo, originalState != idxInfo.State)
	case model.StateDeleteOnly:
		// delete only -> write only
		job.SchemaState = model.StateWriteOnly
		idxInfo.State = model.StateWriteOnly
		//_, err = checkPrimaryKeyNotNull(w, sqlMode, t, job, tblInfo, indexInfo)
		//if err != nil {
		//	break
		//}
		ver, err = updateVersionAndTableInfo(t, job, tblInfo, originalState != idxInfo.State)
	case model.StateWriteOnly:
		// write only -> reorganization
		job.SchemaState = model.StateWriteReorganization
		idxInfo.State = model.StateWriteReorganization
		//_, err = checkPrimaryKeyNotNull(w, sqlMode, t, job, tblInfo, indexInfo)
		//if err != nil {
		//	break
		//}
		// Initialize SnapshotVer to 0 for later reorganization check.
		job.SnapshotVer = 0
		ver, err = updateVersionAndTableInfo(t, job, tblInfo, originalState != idxInfo.State)
	case model.StateWriteReorganization:
		// reorganization -> public

		//tbl, err := getTable(d.store, schemaID, tblInfo)
		//if err != nil {
		//	return ver, errors.Trace(err)
		//}

		//reorgInfo, err := getReorgInfo(d, t, job, tbl)
		//if err != nil || reorgInfo.first {
		// If we run reorg firstly, we should update the job snapshot version
		// and then run the reorg next time.
		//	return ver, errors.Trace(err)
		//}

		//err = addTableIndexByWorker(tbl, idxInfo, reorgInfo)
		//idxInfo.State = model.StatePublic
		ver, err = updateVersionAndTableInfo(t, job, tblInfo, originalState != idxInfo.State)
		if err != nil {
			return ver, errors.Trace(err)
		}
		job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tblInfo)
	default:
		err = ErrInvalidIndexState.GenWithStack("invalid index state %v", tblInfo.State)
	}

	return ver, errors.Trace(err)
}

type Worker interface {
	Run(t Task, wg *sync.WaitGroup)
	GetError() error
	CleanError()
}

type Task interface {
	GetType() string
}

type WorkerPool struct {
	workers []Worker
	tasks   []Task
}

func NewWorkerPool() *WorkerPool {
	return &WorkerPool{
		workers: make([]Worker, 0),
		tasks:   make([]Task, 0),
	}
}

func (p *WorkerPool) AddWorkers(w Worker) error {
	p.workers = append(p.workers, w)
	return nil
}

func (p *WorkerPool) AddTask(t Task) {
	p.tasks = append(p.tasks, t)
}

// Return whether to continue or not.
func (p *WorkerPool) Run() (bool, error) {
	var (
		wg  sync.WaitGroup
		err error
	)
	defer p.cleanWorkerError()
	pl := len(p.workers)
	tl := len(p.tasks)
	if pl >= tl {
		for i, t := range p.tasks {
			wg.Add(1)
			p.workers[i].Run(t, &wg)
		}
		wg.Wait()
		err = p.getWorkersError()
		p.tasks = p.tasks[:]
		return false, err
	}
	for i, w := range p.workers {
		wg.Add(1)
		w.Run(p.tasks[i], &wg)
	}
	wg.Wait()
	err = p.getWorkersError()
	p.tasks = p.tasks[pl:]
	return true, err
}

func (p *WorkerPool) Reset() {
	p.workers = make([]Worker, 0)
	p.tasks = make([]Task, 0)
}

func (p *WorkerPool) cleanWorkerError() {
	for _, w := range p.workers {
		w.CleanError()
	}
}

func (p *WorkerPool) getWorkersError() error {
	for _, w := range p.workers {
		if err := w.GetError(); err != nil {
			return err
		}
	}
	return nil
}

type AddIndexWorker struct {
	tbl       table.Table
	idx       *model.IndexMeta
	reorg     *reorgInfo
	batchSize int64
	err       error
}

const (
	addIndexBatch    = 150
	nAddIndexWorkers = 10
)

func newAddIndexWorker(tbl table.Table, idx *model.IndexMeta, reorg *reorgInfo) Worker {
	return &AddIndexWorker{
		tbl:       tbl,
		idx:       idx,
		reorg:     reorg,
		batchSize: addIndexBatch,
	}
}

func (a *AddIndexWorker) Run(t Task, wg *sync.WaitGroup) {
	task := t.(*IndexTask)
	defer wg.Done()
	for {
		next, err := a.createIndex(task)
		if err != nil {
			a.err = err
			return
		}
		if next == nil {
			break
		}
		task.start = next
	}
}

func (a *AddIndexWorker) createIndex(t *IndexTask) ([]byte, error) {
	store := a.reorg.d.store
	ii := tables.NewIndexInfo(a.idx)
	ii.Init(a.tbl.Meta())
	var next_ []byte
	errTxn := kv.RunInNewTxn(store, false, func(txn kv.Transaction) error {
		records, next, err := a.fetchIndexRecords(txn.StartTS(), t.start, t.end)
		if err != nil {
			return errors.Trace(err)
		}
		indexBatchKeys := make([]kv.Key, 0)
		for _, r := range records {
			iKey, _, err := ii.BuildIndex(a.tbl.Meta(), r.key, r.value)
			if err != nil {
				return errors.Trace(err)
			}
			indexBatchKeys = append(indexBatchKeys, iKey)
			r.indexKey = iKey
		}
		batchValues, err := txn.BatchGet(context.TODO(), indexBatchKeys)
		for _, r := range records {
			if _, ok := batchValues[string(r.indexKey)]; !ok {
				// Lock the row key to notify us that someone delete or update the row,
				// then we should not backfill the index of it, otherwise the adding index is redundant.
				err = txn.LockKeys(context.Background(), new(kv.LockCtx), r.key)
				if err != nil {
					return errors.Trace(err)
				}
				err = txn.Set(r.indexKey, r.pk)
				if err != nil {
					return errors.Trace(err)
				}
			}
		}
		next_ = next
		return nil
	})
	return next_, errTxn
}

func (a *AddIndexWorker) GetError() error {
	return a.err
}

func (a *AddIndexWorker) CleanError() {
	a.err = nil
}

func (a *AddIndexWorker) fetchIndexRecords(ver uint64, start, end []byte) ([]*indexRecord, []byte, error) {
	irs := make([]*indexRecord, 0)
	var next kv.Key
	iterateSnapshotRows(a.reorg.d.store, 0, a.tbl, ver, start, end, true, false,
		func(pk []byte, rowKey kv.Key, rawRecord []byte) (more bool, err error) {
			ir := &indexRecord{
				key:   rowKey,
				value: rawRecord,
				pk:    pk,
			}
			irs = append(irs, ir)
			if bytes.Compare(rowKey, end) == 0 || len(irs) == int(a.batchSize) {
				next = kv.Key(end).Next()
				return false, nil
			}
			return true, nil
		})
	return irs, next, nil
}

type IndexTask struct {
	start []byte
	end   []byte
}

func (t *IndexTask) GetType() string {
	return "index-task"
}

func getAddIndexTasks(reorg *reorgInfo) ([]Task, error) {
	tasks := make([]Task, 0)
	ranges, err := splitTableRanges(reorg.d.store, reorg.StartHandle, reorg.EndHandle)
	for _, r := range ranges {
		t := &IndexTask{
			start: r.StartKey,
			end:   r.EndKey,
		}
		tasks = append(tasks, t)
	}
	if err != nil {
		return tasks, errors.Trace(err)
	}
	return tasks, nil
}

// splitTableRanges uses PD region's key ranges to split the backfilling table key range space,
// to speed up adding index in table with disperse handle.
// The `t` should be a non-partitioned table or a partition.
func splitTableRanges(store kv.Storage, start, end kv.Key) ([]kv.KeyRange, error) {
	startRecordKey := start
	endRecordKey := end.Next()

	logutil.Logger(ddlLogCtx).Info("[ddl] split table range from PD",
		zap.ByteString("startHandle", startRecordKey), zap.ByteString("endHandle", endRecordKey))

	kvRange := kv.KeyRange{StartKey: startRecordKey, EndKey: endRecordKey}
	s, ok := store.(tikv.Storage)
	if !ok {
		// Only support split ranges in tikv.Storage now.
		return []kv.KeyRange{kvRange}, nil
	}

	maxSleep := 10000 // ms
	bo := tikv.NewBackoffer(context.Background(), maxSleep)
	ranges, err := tikv.SplitRegionRanges(bo, s.GetRegionCache(), []kv.KeyRange{kvRange})
	if err != nil {
		return nil, errors.Trace(err)
	}
	if len(ranges) == 0 {
		return nil, errors.Trace(errInvalidSplitRegionRanges)
	}
	return ranges, nil
}

func addTableIndexByWorker(tbl table.Table, idx *model.IndexMeta, reorg *reorgInfo) error {
	workerPool := NewWorkerPool()
	var (
		err  error
		more bool
	)
	for i := 0; i < nAddIndexWorkers; i++ {
		w := newAddIndexWorker(tbl, idx, reorg)
		workerPool.AddWorkers(w)
	}
	tasks, err := getAddIndexTasks(reorg)
	if err != nil {
		return errors.Trace(err)
	}
	for _, t := range tasks {
		workerPool.AddTask(t)
	}
	for {
		more, err = workerPool.Run()
		if err != nil {
			return err
		}
		if !more {
			break
		}
	}
	return nil
}

// TODO:
// 1. timeout
// 2. resumable
// 3. idempodent
// 4. unique and not unique
func addTableIndex(tbl table.Table, idx *model.IndexMeta, reorg *reorgInfo) error {
	store := reorg.d.store
	ii := tables.NewIndexInfo(idx)
	ii.Init(tbl.Meta())
	errTxn := kv.RunInNewTxn(store, false, func(txn kv.Transaction) error {
		indexChan := fetchIndexRecords(store, tbl, reorg, txn.StartTS())
		for {
			select {
			case ir := <-indexChan:
				if ir == nil {
					return nil
				}
				iKey, iValue, err := ii.BuildIndex(tbl.Meta(), ir.key, ir.value)
				if err != nil {
					return errors.Trace(err)
				}
				err = txn.Set(iKey, iValue)
				if err != nil {
					return errors.Trace(err)
				}
			}
		}
	})
	return errTxn
}

type indexRecord struct {
	key      []byte
	value    []byte
	pk       []byte
	indexKey []byte
	skip     bool
}

func fetchIndexRecords(store kv.Storage, tbl table.Table, reorg *reorgInfo, version uint64) chan *indexRecord {
	indexRecordChan := make(chan *indexRecord, 100)
	go iterateSnapshotRows(store, 0, tbl, version, reorg.StartHandle, reorg.EndHandle, true, false,
		func(pk []byte, rowKey kv.Key, rawRecord []byte) (more bool, err error) {
			ir := &indexRecord{
				key:   rowKey,
				value: rawRecord,
				pk:    pk,
			}

			select {
			case indexRecordChan <- ir:
			}
			if bytes.Compare(rowKey, reorg.EndHandle) == 0 {
				indexRecordChan <- nil
				return false, nil
			}
			return true, nil
		})
	return indexRecordChan
}

func allocateIndexID(tblInfo *model.TableMeta) int64 {
	tblInfo.MaxIndexID++
	return tblInfo.MaxIndexID
}

func onDropIndex(t *meta.Meta, job *model.Job) (ver int64, err error) {
	return
}

// updateVersionAndTableInfoWithCheck checks table info validate and updates the schema version and the table information
func updateVersionAndTableInfoWithCheck(t *meta.Meta, job *model.Job, tblInfo *model.TableMeta, shouldUpdateVer bool) (
	ver int64, err error) {
	err = checkTableInfoValid(tblInfo)
	if err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	return updateVersionAndTableInfo(t, job, tblInfo, shouldUpdateVer)
}

// recordIterFunc is used for low-level record iteration.
type recordIterFunc func(pk []byte, rowKey kv.Key, rawRecord []byte) (more bool, err error)

func iterateSnapshotRows(store kv.Storage, priority int, t table.Table, version uint64, startKey kv.Key,
	endKey kv.Key, endIncluded, reverse bool, fn recordIterFunc) error {
	ver := kv.Version{Ver: version}
	// Calculate the exclusive upper bound
	var (
		upperBound kv.Key
		it         kv.Iterator
		err        error
	)

	snap, err := store.GetSnapshot(ver)
	snap.SetOption(kv.Priority, priority)
	if err != nil {
		return errors.Trace(err)
	}

	if endIncluded {
		upperBound = endKey.PrefixNext()
	} else {
		upperBound = endKey
	}

	if reverse {
		it, err = snap.IterReverse(endKey)
	} else {
		it, err = snap.Iter(startKey, upperBound)
	}

	if err != nil {
		return errors.Trace(err)
	}
	defer it.Close()

	for it.Valid() {

		if !it.Key().HasPrefix(t.RecordPrefix()) {
			break
		}
		var pk []byte
		pk = tablecodec.ExtractEncodedPrimaryKey(it.Key())
		more, err := fn(pk, it.Key(), it.Value())
		if !more || err != nil {
			return errors.Trace(err)
		}
		rowkeyPrefix := append(t.RecordPrefix(), pk...)
		err = kv.NextUntil(it, util.RowKeyPrefixFilter(rowkeyPrefix))
		if err != nil {
			if kv.ErrNotExist.Equal(err) {
				break
			}
			return errors.Trace(err)
		}
	}

	return nil
}
