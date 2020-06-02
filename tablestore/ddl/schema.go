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
	"github.com/pingcap/errors"
	"github.com/zhihu/zetta/pkg/meta"
	"github.com/zhihu/zetta/pkg/model"
	"github.com/zhihu/zetta/tablestore/infoschema"
)

func onCreateSchema(d *ddlCtx, t *meta.Meta, job *model.Job) (ver int64, _ error) {
	schemaID := job.SchemaID
	dbMeta := &model.DatabaseMeta{}
	if err := job.DecodeArgs(dbMeta); err != nil {
		// Invalid arguments, cancel this job.
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	dbMeta.Id = schemaID
	dbMeta.State = model.StateNone

	err := checkSchemaNotExists(d, t, schemaID, dbMeta)
	if err != nil {
		if infoschema.ErrDatabaseExists.Equal(err) {
			// The database already exists, can't create it, we should cancel this job now.
			job.State = model.JobStateCancelled
		}
		return ver, errors.Trace(err)
	}

	ver, err = updateSchemaVersion(t, job)
	if err != nil {
		return ver, errors.Trace(err)
	}

	switch dbMeta.State {
	case model.StateNone:
		// none -> public
		dbMeta.State = model.StatePublic
		err = t.CreateDatabase(dbMeta)
		if err != nil {
			return ver, errors.Trace(err)
		}
		// Finish this job.
		job.FinishDBJob(model.JobStateDone, model.StatePublic, ver, dbMeta)
		return ver, nil
	default:
		// We can't enter here.
		return ver, errors.Errorf("invalid db state %v", dbMeta.State)
	}
}

func onDropSchema(t *meta.Meta, job *model.Job) (ver int64, _ error) {
	dbInfo, err := checkSchemaExistAndCancelNotExistJob(t, job)
	if err != nil {
		return ver, errors.Trace(err)
	}

	ver, err = updateSchemaVersion(t, job)
	if err != nil {
		return ver, errors.Trace(err)
	}
	switch dbInfo.State {
	case model.StatePublic:
		// public -> write only
		job.SchemaState = model.StateWriteOnly
		dbInfo.State = model.StateWriteOnly
		err = t.UpdateDatabase(dbInfo)
	case model.StateWriteOnly:
		// write only -> delete only
		job.SchemaState = model.StateDeleteOnly
		dbInfo.State = model.StateDeleteOnly
		err = t.UpdateDatabase(dbInfo)
	case model.StateDeleteOnly:
		dbInfo.State = model.StateNone
		var tables []*model.TableMeta
		tables, err = t.ListTables(job.SchemaID)
		if err != nil {
			return ver, errors.Trace(err)
		}

		err = t.UpdateDatabase(dbInfo)
		if err != nil {
			return ver, errors.Trace(err)
		}
		if err = t.DropDatabase(dbInfo.Id); err != nil {
			break
		}

		// Finish this job.
		if len(tables) > 0 {
			job.Args = append(job.Args, getIDs(tables))
		}
		job.FinishDBJob(model.JobStateDone, model.StateNone, ver, dbInfo)
	default:
		// We can't enter here.
		err = errors.Errorf("invalid db state %v", dbInfo.State)
	}

	return ver, errors.Trace(err)
}

func checkSchemaExistAndCancelNotExistJob(t *meta.Meta, job *model.Job) (*model.DatabaseMeta, error) {
	dbInfo, err := t.GetDatabase(job.SchemaID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if dbInfo == nil {
		job.State = model.JobStateCancelled
		return nil, infoschema.ErrDatabaseDropExists.GenWithStackByArgs("")
	}
	return dbInfo, nil
}

func checkSchemaNotExists(d *ddlCtx, t *meta.Meta, schemaID int64, dbInfo *model.DatabaseMeta) error {
	// d.infoHandle maybe nil in some test.
	if d.infoHandle == nil {
		return checkSchemaNotExistsFromStore(t, schemaID, dbInfo)
	}
	// Try to use memory schema info to check first.
	currVer, err := t.GetSchemaVersion()
	if err != nil {
		return err
	}
	is := d.infoHandle.Get()
	if is.SchemaMetaVersion() == currVer {
		return checkSchemaNotExistsFromInfoSchema(is, schemaID, dbInfo)
	}
	return checkSchemaNotExistsFromStore(t, schemaID, dbInfo)
}

func checkSchemaNotExistsFromInfoSchema(is infoschema.InfoSchema, schemaID int64, dbInfo *model.DatabaseMeta) error {
	// Check database exists by name.
	if is.SchemaExists(dbInfo.Database) {
		return infoschema.ErrDatabaseExists.GenWithStackByArgs(dbInfo.Database)
	}
	// Check database exists by ID.
	//if _, ok := is.SchemaByID(schemaID); ok {
	//	return infoschema.ErrDatabaseExists.GenWithStackByArgs(dbInfo.Database)
	//}
	return nil
}

func checkSchemaNotExistsFromStore(t *meta.Meta, schemaID int64, dbInfo *model.DatabaseMeta) error {
	dbs, err := t.ListDatabases()
	if err != nil {
		return errors.Trace(err)
	}

	for _, db := range dbs {
		if db.Database == dbInfo.Database {
			if db.Id != schemaID {
				return infoschema.ErrDatabaseExists.GenWithStackByArgs(db.Database)
			}
			dbInfo = db
		}
	}
	return nil
}

func getIDs(tables []*model.TableMeta) []int64 {
	ids := make([]int64, 0, len(tables))
	for _, t := range tables {
		ids = append(ids, t.Id)
		//if t.GetPartitionInfo() != nil {
		//	ids = append(ids, getPartitionIDs(t)...)
		//}
	}

	return ids
}
