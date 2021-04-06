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
	"os"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/log"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/testleak"
	"github.com/zhihu/zetta/pkg/meta"
	"github.com/zhihu/zetta/pkg/model"
	"github.com/zhihu/zetta/tablestore/infoschema"
	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"
)

type DDLForTest interface {
	// SetHook sets the hook.
	SetHook(h Callback)
	// SetInterceptoror sets the interceptor.
	SetInterceptoror(h Interceptor)
}

// SetHook implements DDL.SetHook interface.
func (d *ddl) SetHook(h Callback) {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.mu.hook = h
}

// SetInterceptoror implements DDL.SetInterceptoror interface.
func (d *ddl) SetInterceptoror(i Interceptor) {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.mu.interceptor = i
}

// generalWorker returns the general worker.
func (d *ddl) generalWorker() *worker {
	return d.workers[generalWorker]
}

// restartWorkers is like the function of d.start. But it won't initialize the "workers" and create a new worker.
// It only starts the original workers.
func (d *ddl) restartWorkers(ctx context.Context) {
	d.quitCh = make(chan struct{})
	if !RunWorker {
		return
	}

	err := d.ownerManager.CampaignOwner()
	terror.Log(err)
	for _, worker := range d.workers {
		worker.wg.Add(1)
		worker.quitCh = make(chan struct{})
		w := worker
		go util.WithRecovery(func() { w.start(d.ddlCtx) },
			func(r interface{}) {
				if r != nil {
					log.Error("[ddl] restart DDL worker meet panic", zap.String("worker", w.String()), zap.String("ID", d.uuid))
				}
			})
		asyncNotify(worker.ddlJobCh)
	}
}

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	*CustomParallelSuiteFlag = true
	logLevel := os.Getenv("log_level")
	logutil.InitLogger(logutil.NewLogConfig(logLevel, "highlight", "", logutil.EmptyFileLogConfig, false))
	autoid.SetStep(5000)
	//ReorgWaitTimeout = 30 * time.Millisecond

	cfg := config.GetGlobalConfig()
	newCfg := *cfg
	newCfg.Log.SlowThreshold = 10000
	// Test for add/drop primary key.
	newCfg.AlterPrimaryKey = true
	config.StoreGlobalConfig(&newCfg)

	testleak.BeforeTest()
	TestingT(t)
	testleak.AfterTestT(t)()
}

func testCreateStore(c *C, name string) kv.Storage {
	store, err := mockstore.NewMockTikvStore()
	c.Assert(err, IsNil)
	return store
}

func testNewContext(d *ddl) sessionctx.Context {
	ctx := mock.NewContext()
	ctx.Store = d.store
	return ctx
}

func testNewDDL(ctx context.Context, etcdCli *clientv3.Client, store kv.Storage,
	infoHandle *infoschema.Handler, hook Callback, lease time.Duration) *ddl {
	return newDDL(ctx, etcdCli, store, infoHandle, hook, lease, nil)
}

func getSchemaVer(c *C, ctx sessionctx.Context) int64 {
	err := ctx.NewTxn(context.Background())
	c.Assert(err, IsNil)
	txn, err := ctx.Txn(true)
	c.Assert(err, IsNil)
	m := meta.NewMeta(txn)
	ver, err := m.GetSchemaVersion()
	c.Assert(err, IsNil)
	return ver
}

type historyJobArgs struct {
	ver    int64
	db     *model.DatabaseMeta
	tbl    *model.TableMeta
	tblIDs map[int64]struct{}
}

func checkEqualTable(c *C, t1, t2 *model.TableInfo) {
	c.Assert(t1.ID, Equals, t2.ID)
	c.Assert(t1.Name, Equals, t2.Name)
	c.Assert(t1.Charset, Equals, t2.Charset)
	c.Assert(t1.Collate, Equals, t2.Collate)
	c.Assert(t1.PKIsHandle, DeepEquals, t2.PKIsHandle)
	c.Assert(t1.Comment, DeepEquals, t2.Comment)
	c.Assert(t1.AutoIncID, DeepEquals, t2.AutoIncID)
}

func checkHistoryJob(c *C, job *model.Job) {
	c.Assert(job.State, Equals, model.JobStateSynced)
}

func checkHistoryJobArgs(c *C, ctx sessionctx.Context, id int64, args *historyJobArgs) {
	txn, err := ctx.Txn(true)
	c.Assert(err, IsNil)
	t := meta.NewMeta(txn)
	historyJob, err := t.GetHistoryDDLJob(id)
	c.Assert(err, IsNil)
	c.Assert(historyJob.BinlogInfo.FinishedTS, Greater, uint64(0))

	if args.tbl != nil {
		c.Assert(historyJob.BinlogInfo.SchemaVersion, Equals, args.ver)
		//checkEqualTable(c, historyJob.BinlogInfo.TableInfo, args.tbl)
		return
	}

	// for handling schema job
	c.Assert(historyJob.BinlogInfo.SchemaVersion, Equals, args.ver)
	c.Assert(historyJob.BinlogInfo.DBInfo, DeepEquals, args.db)
	// only for creating schema job
	if args.db != nil && len(args.tblIDs) == 0 {
		return
	}
}

func buildCreateIdxJob(dbid, tbid int64, idx *model.IndexMeta) *model.Job {
	return &model.Job{
		SchemaID:   dbid,
		TableID:    tbid,
		Type:       model.ActionAddIndex,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{idx},
	}
}

func testCreateIndex(c *C, ctx sessionctx.Context, d *ddl, dbid, tbid int64, idx *model.IndexMeta) *model.Job {
	job := buildCreateIdxJob(dbid, tbid, idx)
	err := d.doDDLJob(ctx, job)
	c.Assert(err, IsNil)
	//v := getSchemaVer(c, ctx)
	//checkHistoryJobArgs(c, ctx, job.ID, &historyJobArgs{ver: v, tbl: tblInfo})
	return job
}
