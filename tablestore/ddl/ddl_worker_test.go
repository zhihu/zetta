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

// Copyright 2015 PingCAP, Inc.
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
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/zhihu/zetta/pkg/meta"
	"github.com/zhihu/zetta/pkg/model"
)

var _ = Suite(&testDDLSuite{})

type testDDLSuite struct{}

const testLease = 5 * time.Millisecond

func (s *testDDLSuite) SetUpSuite(c *C) {
	WaitTimeWhenErrorOccured = 1 * time.Microsecond

	// We hope that this test is serially executed. So put it here.
	s.testRunWorker(c)
}

func (s *testDDLSuite) TearDownSuite(c *C) {
}

func (s *testDDLSuite) TestCheckOwner(c *C) {
	store := testCreateStore(c, "test_owner")
	defer store.Close()

	d1 := testNewDDL(context.Background(), nil, store, nil, nil, testLease)
	defer d1.Stop()
	time.Sleep(testLease)
	testCheckOwner(c, d1, true)

	c.Assert(d1.GetLease(), Equals, testLease)
}

// testRunWorker tests no job is handled when the value of RunWorker is false.
func (s *testDDLSuite) testRunWorker(c *C) {
	store := testCreateStore(c, "test_run_worker")
	defer store.Close()

	RunWorker = false
	d := testNewDDL(context.Background(), nil, store, nil, nil, testLease)
	testCheckOwner(c, d, false)
	defer d.Stop()

	// Make sure the DDL worker is nil.
	worker := d.generalWorker()
	c.Assert(worker, IsNil)
	// Make sure the DDL job can be done and exit that goroutine.
	RunWorker = true
	d1 := testNewDDL(context.Background(), nil, store, nil, nil, testLease)
	testCheckOwner(c, d1, true)
	defer d1.Stop()
	worker = d1.generalWorker()
	c.Assert(worker, NotNil)
}

func (s *testDDLSuite) TestSchemaError(c *C) {
	store := testCreateStore(c, "test_schema_error")
	defer store.Close()

	d := testNewDDL(context.Background(), nil, store, nil, nil, testLease)
	defer d.Stop()
	ctx := testNewContext(d)

	doDDLJobErr(c, 1, 0, model.ActionCreateSchema, []interface{}{1}, ctx, d)
}

func (s *testDDLSuite) TestInvalidDDLJob(c *C) {
	store := testCreateStore(c, "test_invalid_ddl_job_type_error")
	defer store.Close()
	d := testNewDDL(context.Background(), nil, store, nil, nil, testLease)
	defer d.Stop()
	ctx := testNewContext(d)

	job := &model.Job{
		SchemaID:   0,
		TableID:    0,
		Type:       model.ActionNone,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{},
	}
	err := d.doDDLJob(ctx, job)
	c.Assert(err.Error(), Equals, "[ddl:3]invalid ddl job type: none")
}

func testCheckOwner(c *C, d *ddl, expectedVal bool) {
	c.Assert(d.isOwner(), Equals, expectedVal)
}

func testCheckJobDone(c *C, d *ddl, job *model.Job, isAdd bool) {
	kv.RunInNewTxn(d.store, false, func(txn kv.Transaction) error {
		t := meta.NewMeta(txn)
		historyJob, err := t.GetHistoryDDLJob(job.ID)
		c.Assert(err, IsNil)
		checkHistoryJob(c, historyJob)
		if isAdd {
			c.Assert(historyJob.SchemaState, Equals, model.StatePublic)
		} else {
			c.Assert(historyJob.SchemaState, Equals, model.StateNone)
		}

		return nil
	})
}

func testCheckJobCancelled(c *C, d *ddl, job *model.Job, state *model.SchemaState) {
	kv.RunInNewTxn(d.store, false, func(txn kv.Transaction) error {
		t := meta.NewMeta(txn)
		historyJob, err := t.GetHistoryDDLJob(job.ID)
		c.Assert(err, IsNil)
		c.Assert(historyJob.IsCancelled() || historyJob.IsRollbackDone(), IsTrue, Commentf("history job %s", historyJob))
		if state != nil {
			c.Assert(historyJob.SchemaState, Equals, *state)
		}
		return nil
	})
}

func doDDLJobErrWithSchemaState(ctx sessionctx.Context, d *ddl, c *C, schemaID, tableID int64, tp model.ActionType,
	args []interface{}, state *model.SchemaState) *model.Job {
	job := &model.Job{
		SchemaID:   schemaID,
		TableID:    tableID,
		Type:       tp,
		Args:       args,
		BinlogInfo: &model.HistoryInfo{},
	}
	err := d.doDDLJob(ctx, job)
	// TODO: Add the detail error check.
	c.Assert(err, NotNil, Commentf("err:%v", err))
	testCheckJobCancelled(c, d, job, state)

	return job
}

func doDDLJobSuccess(ctx sessionctx.Context, d *ddl, c *C, schemaID, tableID int64, tp model.ActionType,
	args []interface{}) {
	job := &model.Job{
		SchemaID:   schemaID,
		TableID:    tableID,
		Type:       tp,
		Args:       args,
		BinlogInfo: &model.HistoryInfo{},
	}
	err := d.doDDLJob(ctx, job)
	c.Assert(err, IsNil)
}

func doDDLJobErr(c *C, schemaID, tableID int64, tp model.ActionType, args []interface{},
	ctx sessionctx.Context, d *ddl) *model.Job {
	return doDDLJobErrWithSchemaState(ctx, d, c, schemaID, tableID, tp, args, nil)
}
