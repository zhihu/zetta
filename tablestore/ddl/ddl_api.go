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
	is := d.GetInfoSchemaWithInterceptor(ctx)
	_, ok := is.GetDatabaseMetaByName(dbMeta.Database)
	if ok {
		return infoschema.ErrDatabaseExists.GenWithStackByArgs(dbMeta.Database)
	}

	if err = checkTooLongSchema(dbMeta.Database); err != nil {
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

func checkTooLongSchema(schema string) error {
	if len(schema) > mysql.MaxDatabaseNameLength {
		return ErrTooLongIdent.GenWithStackByArgs(schema)
	}
	return nil
}

func (d *ddl) CreateIndex(ctx sessionctx.Context, db, table string, indexMeta *model.IndexMeta) (err error) {
	is := d.GetInfoSchemaWithInterceptor(ctx)
	dbMeta, ok := is.GetDatabaseMetaByName(db)
	if !ok {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(dbMeta.Database)
	}
	tbMeta, err := is.GetTableMetaByName(db, table)
	if tbMeta == nil {
		return infoschema.ErrTableNotExists.GenWithStackByArgs()
	}
	if i := tbMeta.FindIndexByName(indexMeta.Name); i != nil {
		return ErrDupKeyName.GenWithStack("index already exist %s", indexMeta.Name)
	}

	job := &model.Job{
		SchemaID:   dbMeta.Id,
		TableID:    tbMeta.Id,
		Type:       model.ActionAddIndex,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{indexMeta},
	}

	err = d.doDDLJob(ctx, job)
	return errors.Trace(err)
}

func (d *ddl) AddColumn(ctx sessionctx.Context, req *tspb.AddColumnRequest) (err error) {
	return nil
}

func (d *ddl) CreateTable(ctx sessionctx.Context, tbMeta *model.TableMeta) (err error) {
	is := d.GetInfoSchemaWithInterceptor(ctx)
	db, ok := is.GetDatabaseMetaByName(tbMeta.Database)
	if !ok {
		return infoschema.ErrDatabaseNotExists.GenWithStackByArgs(db.Database)
	}
	if is.TableExists(tbMeta.Database, tbMeta.TableName) {
		return infoschema.ErrTableExists.GenWithStackByArgs()
	}

	tbMeta, err = buildTableInfoWithCheck(ctx, d, db, tbMeta)
	if err != nil {
		return errors.Trace(err)
	}
	tbMeta.State = model.StatePublic
	err = checkTableInfoValid(tbMeta)
	if err != nil {
		return err
	}
	tbMeta.State = model.StateNone

	job := &model.Job{
		SchemaID:   db.Id,
		TableID:    tbMeta.Id,
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
	//err = d.callHookOnChanged(err)
	return errors.Trace(err)
}
