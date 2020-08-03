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
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/ddl/util"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/owner"
	"github.com/pingcap/tidb/sessionctx"
	tidbutil "github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/logutil"
	tspb "github.com/zhihu/zetta-proto/pkg/tablestore"
	"github.com/zhihu/zetta/pkg/meta"
	"github.com/zhihu/zetta/pkg/model"
	"github.com/zhihu/zetta/tablestore/infoschema"
	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"
)

const (
	// currentVersion is for all new DDL jobs.
	currentVersion = 1
	// DDLOwnerKey is the ddl owner path that is saved to etcd, and it's exported for testing.
	DDLOwnerKey = "/tidb/ddl/fg/owner"
	ddlPrompt   = "ddl"
)

var (
	// ddlLogCtx uses for log.
	ddlLogCtx = context.Background()
)

// DDL is responsible for updating schema in data store and maintaining in-memory InfoSchema cache.
type DDL interface {
	CreateSchema(ctx sessionctx.Context, dbmeta *model.DatabaseMeta) error
	CreateTable(ctx sessionctx.Context, tbmeta *model.TableMeta) error
	DropTable(ctx sessionctx.Context, db, table string) error
	CreateIndex(ctx sessionctx.Context, db, table string, indexMeta *model.IndexMeta) error
	AddColumn(ctx sessionctx.Context, req *tspb.AddColumnRequest) error
	Stop() error
}

// ddl is used to handle the statements that define the structure or schema of the database.
type ddl struct {
	m      sync.RWMutex
	quitCh chan struct{}

	*ddlCtx
	workers map[workerType]*worker
}

// ddlCtx is the context when we use worker to handle DDL jobs.
type ddlCtx struct {
	uuid         string
	store        kv.Storage
	ownerManager owner.Manager
	schemaSyncer util.SchemaSyncer
	ddlJobDoneCh chan struct{}
	lease        time.Duration // lease is schema lease.
	infoHandle   *infoschema.Handler

	// hook may be modified.
	mu struct {
		sync.RWMutex
		hook        Callback
		interceptor Interceptor
	}
}

func (dc *ddlCtx) isOwner() bool {
	isOwner := dc.ownerManager.IsOwner()
	logutil.Logger(ddlLogCtx).Debug("[ddl] check whether is the DDL owner", zap.Bool("isOwner", isOwner), zap.String("selfID", dc.uuid))
	return isOwner
}

// NewDDL creates a new DDL.
func NewDDL(ctx context.Context, etcdCli *clientv3.Client, store kv.Storage,
	infoHandle *infoschema.Handler, hook Callback, lease time.Duration) DDL {
	return newDDL(ctx, etcdCli, store, infoHandle, hook, lease)
}

func newDDL(ctx context.Context, etcdCli *clientv3.Client, store kv.Storage,
	infoHandle *infoschema.Handler, hook Callback, lease time.Duration) *ddl {
	if hook == nil {
		hook = &BaseCallback{}
	}

	id := uuid.New().String()
	ctx, cancelFunc := context.WithCancel(ctx)
	var manager owner.Manager
	var syncer util.SchemaSyncer
	if etcdCli == nil {
		// The etcdCli is nil if the store is localstore which is only used for testing.
		// So we use mockOwnerManager and MockSchemaSyncer.
		manager = owner.NewMockManager(id, cancelFunc)
		syncer = NewMockSchemaSyncer()
	} else {
		manager = owner.NewOwnerManager(etcdCli, ddlPrompt, id, DDLOwnerKey, cancelFunc)
		syncer = util.NewSchemaSyncer(etcdCli, id, manager)
	}

	ddlCtx := &ddlCtx{
		uuid:         id,
		store:        store,
		lease:        lease,
		ddlJobDoneCh: make(chan struct{}, 1),
		ownerManager: manager,
		schemaSyncer: syncer,
		infoHandle:   infoHandle,
	}
	ddlCtx.mu.hook = hook
	ddlCtx.mu.interceptor = &BaseInterceptor{}
	d := &ddl{
		ddlCtx: ddlCtx,
	}
	d.start(ctx)
	return d
}

func (d *ddl) GetStore() kv.Storage {
	return d.ddlCtx.store
}

// start campaigns the owner and starts workers.
func (d *ddl) start(ctx context.Context) {
	logutil.Logger(ddlLogCtx).Info("[ddl] start DDL", zap.String("ID", d.uuid), zap.Bool("runWorker", RunWorker))
	d.quitCh = make(chan struct{})

	// If RunWorker is true, we need campaign owner and do DDL job.
	// Otherwise, we needn't do that.
	if RunWorker {
		err := d.ownerManager.CampaignOwner(ctx)
		terror.Log(errors.Trace(err))

		//We only support one worker type now for simpliness.
		//Maybe add more worker type later.
		d.workers = make(map[workerType]*worker, 1)
		d.workers[generalWorker] = newWorker(generalWorker, d.store)
		for _, worker := range d.workers {
			worker.wg.Add(1)
			w := worker
			go tidbutil.WithRecovery(
				func() { w.start(d.ddlCtx) },
				func(r interface{}) {
					if r != nil {
						logutil.Logger(w.logCtx).Error("[ddl] DDL worker meet panic", zap.String("ID", d.uuid))
					}
				})
			// When the start function is called, we will send a fake job to let worker
			// checks owner firstly and try to find whether a job exists and run.
			asyncNotify(worker.ddlJobCh)
		}

		go tidbutil.WithRecovery(
			func() { d.schemaSyncer.StartCleanWork() },
			func(r interface{}) {
				if r != nil {
					logutil.Logger(ddlLogCtx).Error("[ddl] DDL syncer clean worker meet panic",
						zap.String("ID", d.uuid), zap.Reflect("r", r), zap.Stack("stack trace"))
				}
			})
	}
}

func (d *ddl) doDDLJob(ctx sessionctx.Context, job *model.Job) error {
	// Get a global job ID and put the DDL job in the queue.
	err := d.addDDLJob(ctx, job)
	if err != nil {
		return errors.Trace(err)
	}

	logutil.Logger(ddlLogCtx).Info("[ddl] start DDL job", zap.String("job", job.String()), zap.String("query", job.Query))
	// Notice worker that we push a new job and wait the job done.
	d.asyncNotifyWorker(job.Type)

	var historyJob *model.Job
	jobID := job.ID
	// For a job from start to end, the state of it will be none -> delete only -> write only -> reorganization -> public
	// For every state changes, we will wait as lease 2 * lease time, so here the ticker check is 10 * lease.
	// But we use etcd to speed up, normally it takes less than 0.5s now, so we use 0.5s or 1s or 3s as the max value.
	ticker := time.NewTicker(chooseLeaseTime(10*d.lease, checkJobMaxInterval(job)))
	startTime := time.Now()
	metrics.JobsGauge.WithLabelValues(job.Type.String()).Inc()
	defer func() {
		ticker.Stop()
		metrics.JobsGauge.WithLabelValues(job.Type.String()).Dec()
		metrics.HandleJobHistogram.WithLabelValues(job.Type.String(), metrics.RetLabel(err)).Observe(time.Since(startTime).Seconds())
	}()
	for {
		select {
		case <-d.ddlJobDoneCh:
		case <-ticker.C:
		}

		historyJob, err = d.getHistoryDDLJob(jobID)
		if err != nil {
			logutil.Logger(ddlLogCtx).Error("[ddl] get history DDL job failed, check again", zap.Error(err))
			continue
		} else if historyJob == nil {
			logutil.Logger(ddlLogCtx).Debug("[ddl] DDL job is not in history, maybe not run", zap.Int64("jobID", jobID))
			continue
		}

		// If a job is a history job, the state must be JobStateSynced or JobStateRollbackDone or JobStateCancelled.
		if historyJob.IsSynced() {
			logutil.Logger(ddlLogCtx).Info("[ddl] DDL job is finished", zap.Int64("jobID", jobID))
			return nil
		}

		if historyJob.Error != nil {
			return errors.Trace(historyJob.Error)
		}
		panic("When the state is JobStateRollbackDone or JobStateCancelled, historyJob.Error should never be nil")
	}
}

// addDDLJob gets a global job ID and puts the DDL job in the DDL queue.
func (d *ddl) addDDLJob(ctx sessionctx.Context, job *model.Job) error {
	startTime := time.Now()
	job.Version = currentVersion
	job.Query, _ = ctx.Value(sessionctx.QueryString).(string)
	err := kv.RunInNewTxn(d.store, true, func(txn kv.Transaction) error {
		t := newMetaWithQueueTp(txn, job.Type.String())
		var err error
		job.ID, err = t.GenGlobalID()
		if err != nil {
			return errors.Trace(err)
		}
		job.StartTS = txn.StartTS()
		err = t.EnQueueDDLJob(job)
		return errors.Trace(err)
	})
	metrics.DDLWorkerHistogram.WithLabelValues(metrics.WorkerAddDDLJob, job.Type.String(), metrics.RetLabel(err)).Observe(time.Since(startTime).Seconds())
	return errors.Trace(err)
}

// getHistoryDDLJob gets a DDL job with job's ID from history queue.
func (d *ddl) getHistoryDDLJob(id int64) (*model.Job, error) {
	var job *model.Job
	err := kv.RunInNewTxn(d.store, false, func(txn kv.Transaction) error {
		t := meta.NewMeta(txn)
		var err1 error
		job, err1 = t.GetHistoryDDLJob(id)
		return errors.Trace(err1)
	})

	return job, errors.Trace(err)
}

func (d *ddl) asyncNotifyWorker(jobTp model.ActionType) {
	// If the workers don't run, we needn't to notify workers.
	if !RunWorker {
		return
	}

	asyncNotify(d.workers[generalWorker].ddlJobCh)
}

func (d *ddl) GetLease() time.Duration {
	return d.lease
}

// GetInfoSchemaWithInterceptor gets the infoschema binding to d. It's exported for testing.
// Please don't use this function, it is used by TestParallelDDLBeforeRunDDLJob to intercept the calling of d.infoHandle.Get(), use d.infoHandle.Get() instead.
// Otherwise, the TestParallelDDLBeforeRunDDLJob will hang up forever.
func (d *ddl) GetInfoSchemaWithInterceptor(ctx sessionctx.Context) infoschema.InfoSchema {
	is := d.infoHandle.Get()
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.mu.interceptor.OnGetInfoSchema(ctx, is)
}

func (d *ddl) Stop() error {
	return nil
}

func (d *ddl) genGlobalIDs(count int) ([]int64, error) {
	ret := make([]int64, count)
	err := kv.RunInNewTxn(d.store, true, func(txn kv.Transaction) error {
		var err error
		m := meta.NewMeta(txn)
		for i := 0; i < count; i++ {
			ret[i], err = m.GenGlobalID()
			if err != nil {
				return err
			}
		}
		return nil
	})

	return ret, err
}

func checkJobMaxInterval(job *model.Job) time.Duration {
	// The job of adding index takes more time to process.
	// So it uses the longer time.
	if job.Type == model.ActionAddIndex || job.Type == model.ActionAddPrimaryKey {
		return 3 * time.Second
	}
	if job.Type == model.ActionCreateTable || job.Type == model.ActionCreateSchema {
		return 500 * time.Millisecond
	}
	return 1 * time.Second
}

func chooseLeaseTime(t, max time.Duration) time.Duration {
	if t == 0 || t > max {
		return max
	}
	return t
}
