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

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	parser_model "github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	tspb "github.com/zhihu/zetta-proto/pkg/tablestore"
	"github.com/zhihu/zetta/pkg/meta"
	"github.com/zhihu/zetta/pkg/model"
	"github.com/zhihu/zetta/tablestore/table"
	"github.com/zhihu/zetta/tablestore/table/tables"
)

var _ = Suite(&testTableSuite{})

type testTableSuite struct {
	store  kv.Storage
	dbInfo *model.DatabaseMeta

	d *ddl
}

// testTableInfo creates a test table with num int columns and with no index.
func testTableInfo(c *C, d *ddl, name string, num int) *model.TableMeta {
	tblInfo := &model.TableMeta{
		TableMeta: tspb.TableMeta{
			TableName: name,
		},
	}
	genIDs, err := d.genGlobalIDs(1)
	c.Assert(err, IsNil)
	tblInfo.Id = genIDs[0]

	cols := make([]*model.ColumnMeta, num)
	for i := range cols {
		col := &model.ColumnMeta{
			ColumnMeta: tspb.ColumnMeta{
				Name:       fmt.Sprintf("c%d", i+1),
				ColumnType: &tspb.Type{Code: tspb.TypeCode_INT64},
			},
			ColumnInfo: parser_model.ColumnInfo{
				Offset:       i,
				DefaultValue: i + 1,
				State:        parser_model.StatePublic,
			},
		}

		col.FieldType = *types.NewFieldType(mysql.TypeLong)
		col.Id = allocateColumnID(tblInfo)
		cols[i] = col
	}
	tblInfo.Columns = cols
	return tblInfo
}

func testCreateTable(c *C, ctx sessionctx.Context, d *ddl, dbInfo *model.DatabaseMeta, tblInfo *model.TableMeta) *model.Job {
	job := &model.Job{
		SchemaID:   dbInfo.Id,
		TableID:    tblInfo.Id,
		Type:       model.ActionCreateTable,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{tblInfo},
	}
	err := d.doDDLJob(ctx, job)
	c.Assert(err, IsNil)

	v := getSchemaVer(c, ctx)
	tblInfo.State = parser_model.SchemaState(model.StatePublic)
	checkHistoryJobArgs(c, ctx, job.ID, &historyJobArgs{ver: v, tbl: tblInfo})
	tblInfo.State = parser_model.SchemaState(model.StateNone)
	return job
}

func testDropTable(c *C, ctx sessionctx.Context, d *ddl, dbInfo *model.DatabaseMeta, tblInfo *model.TableMeta) *model.Job {
	job := &model.Job{
		SchemaID:   dbInfo.Id,
		TableID:    tblInfo.Id,
		Type:       model.ActionDropTable,
		BinlogInfo: &model.HistoryInfo{},
	}
	err := d.doDDLJob(ctx, job)
	c.Assert(err, IsNil)

	v := getSchemaVer(c, ctx)
	checkHistoryJobArgs(c, ctx, job.ID, &historyJobArgs{ver: v, tbl: tblInfo})
	return job
}

func testCheckTableState(c *C, d *ddl, dbInfo *model.DatabaseMeta, tblInfo *model.TableMeta, state model.SchemaState) {
	kv.RunInNewTxn(d.store, false, func(txn kv.Transaction) error {
		t := meta.NewMeta(txn)
		info, err := t.GetTable(dbInfo.Id, tblInfo.Id)
		c.Assert(err, IsNil)

		if state == model.StateNone {
			c.Assert(info, IsNil)
			return nil
		}

		c.Assert(info.TableName, DeepEquals, tblInfo.TableName)
		c.Assert(info.State, Equals, state)
		return nil
	})
}

func (s *testTableSuite) SetUpSuite(c *C) {
	s.store = testCreateStore(c, "test_table")
	s.d = testNewDDL(context.Background(), nil, s.store, nil, nil, testLease)

	s.dbInfo = testSchemaInfo(c, s.d, "test")
	testCreateSchema(c, testNewContext(s.d), s.d, s.dbInfo)
}

func (s *testTableSuite) TearDownSuite(c *C) {
	testDropSchema(c, testNewContext(s.d), s.d, s.dbInfo)
	s.d.Stop()
	s.store.Close()
}

func (s *testTableSuite) TestTable(c *C) {
	d := s.d

	ctx := testNewContext(d)

	tblInfo := testTableInfo(c, d, "t", 3)
	job := testCreateTable(c, ctx, d, s.dbInfo, tblInfo)
	testCheckTableState(c, d, s.dbInfo, tblInfo, model.StatePublic)
	testCheckJobDone(c, d, job, true)

	// Create an existing table.
	newTblInfo := testTableInfo(c, d, "t", 3)
	doDDLJobErr(c, s.dbInfo.Id, newTblInfo.Id, model.ActionCreateTable, []interface{}{newTblInfo}, ctx, d)

	job = testDropTable(c, ctx, d, s.dbInfo, tblInfo)
	testCheckJobDone(c, d, job, false)
}

func testGetTable(c *C, d *ddl, schemaID int64, tableID int64) table.Table {
	tbl, err := testGetTableWithError(d, schemaID, tableID)
	c.Assert(err, IsNil)
	return tbl
}

func testGetTableWithError(d *ddl, schemaID, tableID int64) (table.Table, error) {
	var tblInfo *model.TableMeta
	err := kv.RunInNewTxn(d.store, false, func(txn kv.Transaction) error {
		t := meta.NewMeta(txn)
		var err1 error
		tblInfo, err1 = t.GetTable(schemaID, tableID)
		if err1 != nil {
			return errors.Trace(err1)
		}
		return nil
	})
	if err != nil {
		return nil, errors.Trace(err)
	}
	if tblInfo == nil {
		return nil, errors.New("table not found")
	}
	tbl := tables.MakeTableFromMeta(tblInfo)
	if tbl == nil {
		return nil, errors.New("table from meta failed")
	}
	return tbl, nil
}
