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
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx"
	tspb "github.com/zhihu/zetta-proto/pkg/tablestore"
	"github.com/zhihu/zetta/pkg/meta"
	"github.com/zhihu/zetta/pkg/model"
)

func isDDLJobDone(c *C, t *meta.Meta) bool {
	job, err := t.GetDDLJobByIdx(0)
	c.Assert(err, IsNil)
	// Cannot find in job queue.
	if job == nil {
		return true
	}

	time.Sleep(testLease)
	return false
}

func testCheckSchemaState(c *C, d *ddl, dbInfo *model.DatabaseMeta, state model.SchemaState) {
	isDropped := true

	for {
		kv.RunInNewTxn(d.store, false, func(txn kv.Transaction) error {
			t := meta.NewMeta(txn)
			info, err := t.GetDatabase(dbInfo.Id)
			c.Assert(err, IsNil)

			if state == model.StateNone {
				isDropped = isDDLJobDone(c, t)
				if !isDropped {
					return nil
				}
				c.Assert(info, IsNil)
				return nil
			}

			c.Assert(info.Database, DeepEquals, dbInfo.Database)
			c.Assert(info.State, Equals, state)
			return nil
		})

		if isDropped {
			break
		}
	}
}

func testSchemaInfo(c *C, d *ddl, name string) *model.DatabaseMeta {
	dbInfo := &model.DatabaseMeta{
		DatabaseMeta: tspb.DatabaseMeta{
			Database: name,
		},
	}
	genIDs, err := d.genGlobalIDs(1)
	c.Assert(err, IsNil)
	dbInfo.Id = genIDs[0]
	return dbInfo
}

func buildDropSchemaJob(dbInfo *model.DatabaseMeta) *model.Job {
	return &model.Job{
		SchemaID:   dbInfo.Id,
		Type:       model.ActionDropSchema,
		BinlogInfo: &model.HistoryInfo{},
	}
}

func testDropSchema(c *C, ctx sessionctx.Context, d *ddl, dbInfo *model.DatabaseMeta) (*model.Job, int64) {
	job := buildDropSchemaJob(dbInfo)
	err := d.doDDLJob(ctx, job)
	c.Assert(err, IsNil)
	ver := getSchemaVer(c, ctx)
	return job, ver
}

func testCreateSchema(c *C, ctx sessionctx.Context, d *ddl, dbInfo *model.DatabaseMeta) *model.Job {
	job := &model.Job{
		SchemaID:   dbInfo.Id,
		Type:       model.ActionCreateSchema,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{dbInfo},
	}
	err := d.doDDLJob(ctx, job)
	c.Assert(err, IsNil)

	v := getSchemaVer(c, ctx)
	dbInfo.State = model.StatePublic
	checkHistoryJobArgs(c, ctx, job.ID, &historyJobArgs{ver: v, db: dbInfo})
	dbInfo.State = model.StateNone
	return job
}

func (s *testDDLSuite) TestCreateDatabase(c *C) {
	store := testCreateStore(c, "test_schema_ddl_job")
	defer store.Close()
	d := testNewDDL(context.Background(), nil, store, nil, nil, testLease)
	defer d.Stop()
	ctx := testNewContext(d)

	dbMeta := testSchemaInfo(c, d, "test")
	schemaJob := testCreateSchema(c, ctx, d, dbMeta)
	testCheckSchemaState(c, d, dbMeta, model.StatePublic)
	testCheckJobDone(c, d, schemaJob, true)

	job, _ := testDropSchema(c, ctx, d, dbMeta)
	testCheckSchemaState(c, d, dbMeta, model.StateNone)
	testCheckJobDone(c, d, job, false)
}
