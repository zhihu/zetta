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

// Copyright 2013 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

// Copyright 2016 PingCAP, Inc.
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
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/sessionctx"
	tspb "github.com/zhihu/zetta-proto/pkg/tablestore"
	"github.com/zhihu/zetta/pkg/model"
	"github.com/zhihu/zetta/tablestore/infoschema"
)

func (d *ddl) CreateSchema(ctx sessionctx.Context, dbMeta *model.DatabaseMeta) (err error) {
	if err = d.schemaValidate(ctx, dbMeta); err != nil {
		return errors.Trace(err)
	}
	genIDs, err := d.genGlobalIDs(1)
	if err != nil {
		return errors.Trace(err)
	}
	schemaID := genIDs[0]

	job := &model.Job{
		SchemaID:   schemaID,
		Type:       model.ActionCreateSchema,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{dbMeta},
	}

	err = d.doDDLJob(ctx, job)
	//err = d.callHookOnChanged(err)
	return errors.Trace(err)
}

func (d *ddl) schemaValidate(ctx sessionctx.Context, dbMeta *model.DatabaseMeta) error {
	var err error
	is := d.GetInfoSchemaWithInterceptor(ctx)
	_, ok := is.GetDatabaseMetaByName(dbMeta.Database)
	if ok {
		return infoschema.ErrDatabaseExists.GenWithStackByArgs(dbMeta.Database)
	}
	checkTooLongSchema := func(schema string) error {
		if len(schema) > mysql.MaxDatabaseNameLength {
			return ErrTooLongIdent.GenWithStackByArgs(schema)
		}
		return nil
	}
	if err = checkTooLongSchema(dbMeta.Database); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (d *ddl) indexValidate(ctx sessionctx.Context, db, table string, indexMeta *model.IndexMeta) (int64, int64, error) {
	var (
		dbid int64
		tbid int64
		err  error
	)
	is := d.GetInfoSchemaWithInterceptor(ctx)
	dbMeta, ok := is.GetDatabaseMetaByName(db)
	if !ok {
		return dbid, tbid, infoschema.ErrDatabaseNotExists.GenWithStackByArgs(dbMeta.Database)
	}
	tbMeta, err := is.GetTableMetaByName(db, table)
	if err != nil {
		return dbid, tbid, infoschema.ErrTableNotExists.GenWithStackByArgs()
	}
	if i := tbMeta.FindIndexByName(indexMeta.Name); i != nil {
		return dbid, tbid, ErrDupKeyName.GenWithStack("index already exist %s", indexMeta.Name)
	}
	return dbMeta.Id, tbMeta.Id, nil
}

func (d *ddl) CreateIndex(ctx sessionctx.Context, db, table string, indexMeta *model.IndexMeta) error {
	var (
		dbid int64
		tbid int64
		err  error
	)
	if dbid, tbid, err = d.indexValidate(ctx, db, table, indexMeta); err != nil {
		return errors.Trace(err)
	}
	job := &model.Job{
		SchemaID:   dbid,
		TableID:    tbid,
		Type:       model.ActionAddIndex,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{indexMeta},
	}
	err = d.doDDLJob(ctx, job)
	return errors.Trace(err)
}

func (d *ddl) AddColumn(ctx sessionctx.Context, req *tspb.AddColumnRequest) error {
	var (
		dbid int64
		tbid int64
		err  error
	)
	if dbid, tbid, err = d.columnValidate(ctx, req); err != nil {
		return errors.Trace(err)
	}
	job := &model.Job{
		State:      model.JobStateNone,
		SchemaID:   dbid,
		TableID:    tbid,
		Type:       model.ActionAddColumn,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{req.Columns},
	}

	return d.doDDLJob(ctx, job)
}

func (d *ddl) columnValidate(ctx sessionctx.Context, req *tspb.AddColumnRequest) (int64, int64, error) {
	var (
		dbid int64
		tbid int64
		err  error
	)
	is := d.infoHandle.Get()
	db, ok := is.GetDatabaseMetaByName(req.Database)
	if !ok {
		return dbid, tbid, infoschema.ErrDatabaseNotExists.GenWithStackByArgs(db.Database)
	}
	tbMeta, err := is.GetTableMetaByName(req.Database, req.Table)
	if err != nil {
		return dbid, tbid, infoschema.ErrTableNotExists.GenWithStackByArgs()
	}
	for _, c := range req.Columns {
		if col := tbMeta.FindColumnByName(c.Name); col != nil {
			return dbid, tbid, infoschema.ErrColumnExists.GenWithStackByArgs(c.Name)
		}
	}
	return db.Id, tbMeta.Id, nil
}

func (d *ddl) tableValidate(ctx sessionctx.Context, tbMeta *model.TableMeta) (int64, int64, error) {
	var (
		dbid int64
		tbid int64
		err  error
	)
	is := d.GetInfoSchemaWithInterceptor(ctx)
	db, ok := is.GetDatabaseMetaByName(tbMeta.Database)
	if !ok {
		return dbid, tbid, infoschema.ErrDatabaseNotExists.GenWithStackByArgs(db.Database)
	}
	if is.TableExists(tbMeta.Database, tbMeta.TableName) {
		return dbid, tbid, infoschema.ErrTableExists.GenWithStackByArgs()
	}

	tbMeta, err = buildTableInfoWithCheck(ctx, d, db, tbMeta)
	if err != nil {
		return dbid, tbid, errors.Trace(err)
	}
	err = checkTableInfoValid(tbMeta)
	return db.Id, tbMeta.Id, err
}

func (d *ddl) CreateTable(ctx sessionctx.Context, tbMeta *model.TableMeta) error {
	var (
		dbid int64
		tbid int64
		err  error
	)
	if dbid, tbid, err = d.tableValidate(ctx, tbMeta); err != nil {
		return errors.Trace(err)
	}
	tbMeta.State = model.StateNone
	job := &model.Job{
		SchemaID:   dbid,
		TableID:    tbid,
		Type:       model.ActionCreateTable,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{tbMeta},
	}

	err = d.doDDLJob(ctx, job)
	return errors.Trace(err)
}

func (d *ddl) DropTable(ctx sessionctx.Context, db, table string) error {
	is := d.GetInfoSchemaWithInterceptor(ctx)
	if !is.TableExists(db, table) {
		return infoschema.ErrTableDropExists.GenWithStackByArgs()
	}
	dbMeta, _ := is.GetDatabaseMetaByName(db)
	tbMeta, _ := is.GetTableMetaByName(db, table)
	job := &model.Job{
		SchemaID:   dbMeta.Id,
		TableID:    tbMeta.Id,
		Type:       model.ActionDropTable,
		BinlogInfo: &model.HistoryInfo{},
	}

	err := d.doDDLJob(ctx, job)
	return errors.Trace(err)
}
