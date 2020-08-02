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
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"

	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/sessionctx/variable"
	tidbutil "github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/zhihu/zetta/pkg/meta"
	"github.com/zhihu/zetta/pkg/model"
	"go.uber.org/zap"
)

var (
	// RunWorker indicates if this TiDB server starts DDL worker and can run DDL job.
	RunWorker = true
	// ddlWorkerID is used for generating the next DDL worker ID.
	ddlWorkerID = int32(0)
	// WaitTimeWhenErrorOccured is waiting interval when processing DDL jobs encounter errors.
	WaitTimeWhenErrorOccured = 1 * time.Second
)

type workerType byte

const (
	// generalWorker is the worker who handles all DDL statements except “add index”.
	generalWorker workerType = 0
)

// worker is used for handling DDL jobs.
// We only use one kind of worker now.
type worker struct {
	id       int32
	tp       workerType
	ddlJobCh chan struct{}
	quitCh   chan struct{}
	wg       sync.WaitGroup

	logCtx context.Context
}

func newWorker(tp workerType, store kv.Storage) *worker {
	worker := &worker{
		id:       atomic.AddInt32(&ddlWorkerID, 1),
		tp:       tp,
		ddlJobCh: make(chan struct{}, 1),
		quitCh:   make(chan struct{}),
	}

	worker.logCtx = logutil.WithKeyValue(context.Background(), "worker", worker.String())
	return worker
}

func (w *worker) typeStr() string {
	var str string
	switch w.tp {
	case generalWorker:
		str = "general"
	default:
		str = "unknow"
	}
	return str
}

func (w *worker) String() string {
	return fmt.Sprintf("worker %d, tp %s", w.id, w.typeStr())
}

// start is used for async online schema changing, it will try to become the owner firstly,
// then wait or pull the job queue to handle a schema change job.
func (w *worker) start(d *ddlCtx) {
	logutil.Logger(w.logCtx).Info("[ddl] start DDL worker")
	defer w.wg.Done()

	// We use 4 * lease time to check owner's timeout, so here, we will update owner's status
	// every 2 * lease time. If lease is 0, we will use default 1s.
	// But we use etcd to speed up, normally it takes less than 1s now, so we use 1s as the max value.
	checkTime := chooseLeaseTime(2*d.lease, 1*time.Second)

	ticker := time.NewTicker(checkTime)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
		case <-w.ddlJobCh:
		case <-w.quitCh:
			return
		}

		err := w.handleDDLJobQueue(d)
		if err != nil {
			logutil.Logger(w.logCtx).Error("[ddl] handle DDL job failed", zap.Error(err))
		}
	}
}

// handleDDLJobQueue handles DDL jobs in DDL Job queue.
func (w *worker) handleDDLJobQueue(d *ddlCtx) error {
	once := true
	var t *meta.Meta
	getJob := func(txn kv.Transaction) (*model.Job, error) {
		t = newMetaWithQueueTp(txn, w.typeStr())
		return w.getFirstDDLJob(t)
	}
	for {
		// We are not owner, return and retry checking later.
		if isChanClosed(w.quitCh) || !d.isOwner() {
			return nil
		}

		var (
			job       *model.Job
			schemaVer int64
			runJobErr error
		)
		waitTime := 2 * d.lease
		err := kv.RunInNewTxn(d.store, false, func(txn kv.Transaction) error {
			var err error
			job, err = getJob(txn)
			if job == nil || err != nil {
				return errors.Trace(err)
			}
			logutil.Logger(ddlLogCtx).Info("[ddl] start DDL job in worker", zap.String("job", job.String()), zap.String("query", job.Query))
			//
			if once {
				w.waitSchemaSynced(d, job, waitTime)
				once = false
				return nil
			}

			if job.IsDone() || job.IsRollbackDone() {
				if !job.IsRollbackDone() {
					job.State = model.JobStateSynced
				}
				err = w.finishDDLJob(t, job)
				return errors.Trace(err)
			}

			// If running job meets error, we will save this error in job Error
			// and retry later if the job is not cancelled.
			tidbutil.WithRecovery(func() {
				schemaVer, runJobErr = w.runDDLJob(d, t, job)
			}, func(r interface{}) {
				if r != nil {
					// If run ddl job panic, just cancel the ddl jobs.
					job.State = model.JobStateCancelling
				}
			})

			if job.IsCancelled() {
				txn.Reset()
				err = w.finishDDLJob(t, job)
				return errors.Trace(err)
			}
			err = w.updateDDLJob(t, job, runJobErr != nil)
			if err = w.handleUpdateJobError(t, job, err); err != nil {
				return errors.Trace(err)
			}

			return nil
		})

		if runJobErr != nil {
			// wait a while to retry again. If we don't wait here, DDL will retry this job immediately,
			// which may act like a deadlock.
			logutil.Logger(w.logCtx).Info("[ddl] run DDL job error, sleeps a while then retries it.", zap.Duration("waitTime", WaitTimeWhenErrorOccured), zap.Error(runJobErr))
			time.Sleep(WaitTimeWhenErrorOccured)
		}

		if err != nil {
			return errors.Trace(err)
		} else if job == nil {
			// No job now, return and retry getting later.
			return nil
		}

		// Here means the job enters another state (delete only, write only, public, etc...) or is cancelled.
		// If the job is done or still running or rolling back, we will wait 2 * lease time to guarantee other servers to update
		// the newest schema.
		w.waitSchemaChanged(nil, d, waitTime, schemaVer, job)
		if job.IsSynced() || job.IsCancelled() {
			asyncNotify(d.ddlJobDoneCh)
		}
	}
}

// getFirstDDLJob gets the first DDL job form DDL queue.
func (w *worker) getFirstDDLJob(t *meta.Meta) (*model.Job, error) {
	job, err := t.GetDDLJobByIdx(0)
	return job, errors.Trace(err)
}

// runDDLJob runs a DDL job. It returns the current schema version in this transaction and the error.
func (w *worker) runDDLJob(d *ddlCtx, t *meta.Meta, job *model.Job) (ver int64, err error) {
	logutil.Logger(w.logCtx).Info("[ddl] run DDL job", zap.String("job", job.String()))
	timeStart := time.Now()
	defer func() {
		metrics.DDLWorkerHistogram.WithLabelValues(metrics.WorkerRunDDLJob, job.Type.String(), metrics.RetLabel(err)).Observe(time.Since(timeStart).Seconds())
	}()
	if job.IsFinished() {
		return
	}
	// The cause of this job state is that the job is cancelled by client.
	// Or the job failed too many times, more than variable.GetDDLErrorCountLimit()
	if job.IsCancelling() {
		return convertJob2RollbackJob(w, d, t, job)
	}

	if !job.IsRollingback() && !job.IsCancelling() {
		job.State = model.JobStateRunning
	}

	switch job.Type {
	case model.ActionCreateSchema:
		ver, err = onCreateSchema(d, t, job)
	case model.ActionDropSchema:
		ver, err = onDropSchema(t, job)
	case model.ActionCreateTable:
		ver, err = onCreateTable(d, t, job)
	case model.ActionDropTable:
		ver, err = onDropTableOrView(t, job)
	case model.ActionAddColumn:
		ver, err = onAddColumn(d, t, job)
	case model.ActionAddIndex:
		ver, err = onAddIndex(d, t, job)
	default:
		// Invalid job, cancel it.
		job.State = model.JobStateCancelled
		err = errInvalidDDLJob.GenWithStack("invalid ddl job type: %v", job.Type)
	}

	// Save errors in job, so that others can know errors happened.
	if err != nil {
		job.Error = toTError(err)
		job.ErrorCount++

		// If job is cancelled, we shouldn't return an error and shouldn't load DDL variables.
		if job.State == model.JobStateCancelled {
			logutil.Logger(w.logCtx).Info("[ddl] DDL job is cancelled normally", zap.Error(err))
			return ver, nil
		}
		logutil.Logger(w.logCtx).Error("[ddl] run DDL job error", zap.Error(err))

		// Check error limit to avoid falling into an infinite loop.
		if job.ErrorCount > variable.GetDDLErrorCountLimit() && job.State == model.JobStateRunning {
			logutil.Logger(w.logCtx).Warn("[ddl] DDL job error count exceed the limit, cancelling it now", zap.Int64("jobID", job.ID), zap.Int64("errorCountLimit", variable.GetDDLErrorCountLimit()))
			job.State = model.JobStateCancelling
		}
	}
	return
}

// handleUpdateJobError handles the too large DDL job.
func (w *worker) handleUpdateJobError(t *meta.Meta, job *model.Job, err error) error {
	if err == nil {
		return nil
	}
	if kv.ErrEntryTooLarge.Equal(err) {
		logutil.Logger(w.logCtx).Warn("[ddl] update DDL job failed", zap.String("job", job.String()), zap.Error(err))
		// Reduce this txn entry size.
		job.BinlogInfo.Clean()
		job.Error = toTError(err)
		job.SchemaState = model.StateNone
		job.State = model.JobStateCancelled
		err = w.finishDDLJob(t, job)
	}
	return errors.Trace(err)
}

// updateDDLJob updates the DDL job information.
// Every time we enter another state except final state, we must call this function.
func (w *worker) updateDDLJob(t *meta.Meta, job *model.Job, meetErr bool) error {
	updateRawArgs := true
	// If there is an error when running job and the RawArgs hasn't been decoded by DecodeArgs,
	// so we shouldn't replace RawArgs with the marshaling Args.
	if meetErr && (job.RawArgs != nil && job.Args == nil) {
		logutil.Logger(w.logCtx).Info("[ddl] meet error before update DDL job, shouldn't update raw args", zap.String("job", job.String()))
		updateRawArgs = false
	}
	return errors.Trace(t.UpdateDDLJob(0, job, updateRawArgs))
}

// waitSchemaChanged waits for the completion of updating all servers' schema. In order to make sure that happens,
// we wait 2 * lease time.
func (w *worker) waitSchemaChanged(ctx context.Context, d *ddlCtx, waitTime time.Duration, latestSchemaVersion int64, job *model.Job) {
	if !job.IsRunning() && !job.IsRollingback() && !job.IsDone() && !job.IsRollbackDone() {
		return
	}
	if waitTime == 0 {
		return
	}

	timeStart := time.Now()
	var err error
	defer func() {
		metrics.DDLWorkerHistogram.WithLabelValues(metrics.WorkerWaitSchemaChanged, job.Type.String(), metrics.RetLabel(err)).Observe(time.Since(timeStart).Seconds())
	}()

	if latestSchemaVersion == 0 {
		logutil.Logger(w.logCtx).Info("[ddl] schema version doesn't change")
		return
	}

	if ctx == nil {
		var cancelFunc context.CancelFunc
		ctx, cancelFunc = context.WithTimeout(context.Background(), waitTime)
		defer cancelFunc()
	}
	err = d.schemaSyncer.OwnerUpdateGlobalVersion(ctx, latestSchemaVersion)
	if err != nil {
		logutil.Logger(w.logCtx).Info("[ddl] update latest schema version failed", zap.Int64("ver", latestSchemaVersion), zap.Error(err))
		if terror.ErrorEqual(err, context.DeadlineExceeded) {
			// If err is context.DeadlineExceeded, it means waitTime(2 * lease) is elapsed. So all the schemas are synced by ticker.
			// There is no need to use etcd to sync. The function returns directly.
			return
		}
	}

	// OwnerCheckAllVersions returns only when context is timeout(2 * lease) or all TiDB schemas are synced.
	err = d.schemaSyncer.OwnerCheckAllVersions(ctx, latestSchemaVersion)
	if err != nil {
		logutil.Logger(w.logCtx).Info("[ddl] wait latest schema version to deadline", zap.Int64("ver", latestSchemaVersion), zap.Error(err))
		if terror.ErrorEqual(err, context.DeadlineExceeded) {
			return
		}
		d.schemaSyncer.NotifyCleanExpiredPaths()
		// Wait until timeout.
		select {
		case <-ctx.Done():
			return
		}
	}
	logutil.Logger(w.logCtx).Info("[ddl] wait latest schema version changed",
		zap.Int64("ver", latestSchemaVersion),
		zap.Duration("take time", time.Since(timeStart)),
		zap.String("job", job.String()))
}

// waitSchemaSynced handles the following situation:
// If the job enters a new state, and the worker crashs when it's in the process of waiting for 2 * lease time,
// Then the worker restarts quickly, we may run the job immediately again,
// but in this case we don't wait enough 2 * lease time to let other servers update the schema.
// So here we get the latest schema version to make sure all servers' schema version update to the latest schema version
// in a cluster, or to wait for 2 * lease time.
func (w *worker) waitSchemaSynced(d *ddlCtx, job *model.Job, waitTime time.Duration) {
	if !job.IsRunning() && !job.IsRollingback() && !job.IsDone() && !job.IsRollbackDone() {
		return
	}
	// TODO: Make ctx exits when the d is close.
	ctx, cancelFunc := context.WithTimeout(context.Background(), waitTime)
	defer cancelFunc()

	latestSchemaVersion, err := d.schemaSyncer.MustGetGlobalVersion(ctx)
	if err != nil {
		logutil.Logger(w.logCtx).Warn("[ddl] get global version failed", zap.Error(err))
		return
	}
	w.waitSchemaChanged(ctx, d, waitTime, latestSchemaVersion, job)
}

// finishDDLJob deletes the finished DDL job in the ddl queue and puts it to history queue.
// If the DDL job need to handle in background, it will prepare a background job.
func (w *worker) finishDDLJob(t *meta.Meta, job *model.Job) (err error) {
	startTime := time.Now()
	defer func() {
		metrics.DDLWorkerHistogram.WithLabelValues(metrics.WorkerFinishDDLJob, job.Type.String(), metrics.RetLabel(err)).Observe(time.Since(startTime).Seconds())
	}()

	_, err = t.DeQueueDDLJob()
	if err != nil {
		return errors.Trace(err)
	}

	job.BinlogInfo.FinishedTS = t.StartTS
	logutil.Logger(w.logCtx).Info("[ddl] finish DDL job", zap.String("job", job.String()))
	updateRawArgs := true
	if job.Type == model.ActionAddPrimaryKey && !job.IsCancelled() {
		// ActionAddPrimaryKey needs to check the warnings information in job.Args.
		// Notice: warnings is used to support non-strict mode.
		updateRawArgs = false
	}
	err = t.AddHistoryDDLJob(job, updateRawArgs)
	return errors.Trace(err)
}

func isChanClosed(quitCh chan struct{}) bool {
	select {
	case <-quitCh:
		return true
	default:
		return false
	}
}

func asyncNotify(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

func newMetaWithQueueTp(txn kv.Transaction, tp string) *meta.Meta {
	return meta.NewMeta(txn)
}

func toTError(err error) *terror.Error {
	originErr := errors.Cause(err)
	tErr, ok := originErr.(*terror.Error)
	if ok {
		return tErr
	}

	// TODO: Add the error code.
	return terror.ClassDDL.New(terror.CodeUnknown, err.Error())
}

// updateSchemaVersion increments the schema version by 1 and sets SchemaDiff.
func updateSchemaVersion(t *meta.Meta, job *model.Job) (int64, error) {
	schemaVersion, err := t.GenSchemaVersion()
	if err != nil {
		return 0, errors.Trace(err)
	}
	diff := &model.SchemaDiff{
		Version:  schemaVersion,
		Type:     job.Type,
		SchemaID: job.SchemaID,
	}
	if job.Type == model.ActionTruncateTable {
		// Truncate table has two table ID, should be handled differently.
		err = job.DecodeArgs(&diff.TableID)
		if err != nil {
			return 0, errors.Trace(err)
		}
		diff.OldTableID = job.TableID
	} else if job.Type == model.ActionRenameTable {
		err = job.DecodeArgs(&diff.OldSchemaID)
		if err != nil {
			return 0, errors.Trace(err)
		}
		diff.TableID = job.TableID
	} else {
		diff.TableID = job.TableID
	}
	err = t.SetSchemaDiff(diff)
	return schemaVersion, errors.Trace(err)
}
