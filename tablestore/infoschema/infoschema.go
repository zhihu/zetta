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

package infoschema

import (
	"sync/atomic"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"

	"github.com/zhihu/zetta/pkg/meta"
	"github.com/zhihu/zetta/pkg/model"
	"github.com/zhihu/zetta/tablestore/table"
	"github.com/zhihu/zetta/tablestore/table/tables"
)

var (
	// ErrDatabaseDropExists returns for dropping a non-existent database.
	ErrDatabaseDropExists = terror.ClassSchema.New(codeDBDropExists, "Can't drop database '%s'; database doesn't exist")
	// ErrDatabaseNotExists returns for database not exists.
	ErrDatabaseNotExists = terror.ClassSchema.New(codeDatabaseNotExists, "Unknown database '%s'")
	// ErrTableNotExists returns for table not exists.
	ErrTableNotExists = terror.ClassSchema.New(codeTableNotExists, "Table '%s.%s' doesn't exist")
	// ErrColumnNotExists returns for column not exists.
	ErrColumnNotExists = terror.ClassSchema.New(codeColumnNotExists, "Unknown column '%s' in '%s'")
	// ErrForeignKeyNotMatch returns for foreign key not match.
	ErrForeignKeyNotMatch = terror.ClassSchema.New(codeWrongFkDef, "Incorrect foreign key definition for '%s': Key reference and table reference don't match")
	// ErrCannotAddForeign returns for foreign key exists.
	ErrCannotAddForeign = terror.ClassSchema.New(codeCannotAddForeign, "Cannot add foreign key constraint")
	// ErrForeignKeyNotExists returns for foreign key not exists.
	ErrForeignKeyNotExists = terror.ClassSchema.New(codeForeignKeyNotExists, "Can't DROP '%s'; check that column/key exists")
	// ErrDatabaseExists returns for database already exists.
	ErrDatabaseExists = terror.ClassSchema.New(codeDatabaseExists, "Can't create database '%s'; database exists")
	// ErrTableExists returns for table already exists.
	ErrTableExists = terror.ClassSchema.New(codeTableExists, "Table '%s' already exists")
	// ErrTableDropExists returns for dropping a non-existent table.
	ErrTableDropExists = terror.ClassSchema.New(codeBadTable, "Unknown table '%s'")
	// ErrUserDropExists returns for dropping a non-existent user.
	ErrUserDropExists = terror.ClassSchema.New(codeBadUser, "User %s does not exist.")
	// ErrColumnExists returns for column already exists.
	ErrColumnExists = terror.ClassSchema.New(codeColumnExists, "Duplicate column name '%s'")
	// ErrIndexExists returns for index already exists.
	ErrIndexExists = terror.ClassSchema.New(codeIndexExists, "Duplicate Index")
	// ErrKeyNameDuplicate returns for index duplicate when rename index.
	ErrKeyNameDuplicate = terror.ClassSchema.New(codeKeyNameDuplicate, "Duplicate key name '%s'")
	// ErrKeyNotExists returns for index not exists.
	ErrKeyNotExists = terror.ClassSchema.New(codeKeyNotExists, "Key '%s' doesn't exist in table '%s'")
	// ErrMultiplePriKey returns for multiple primary keys.
	ErrMultiplePriKey = terror.ClassSchema.New(codeMultiplePriKey, "Multiple primary key defined")
	// ErrTooManyKeyParts returns for too many key parts.
	ErrTooManyKeyParts = terror.ClassSchema.New(codeTooManyKeyParts, "Too many key parts specified; max %d parts allowed")
)

// InfoSchema to get schema info from memory.
type InfoSchema interface {
	GetDatabaseMetaByName(name string) (*model.DatabaseMeta, bool)
	ListDatabases() []*model.DatabaseMeta
	GetTableMetaByName(db, name string) (*model.TableMeta, error)
	GetTableByName(db, name string) (table.Table, error)
	ListTablesByDatabase(db string) []*model.TableMeta
	SchemaMetaVersion() int64
	SchemaExists(name string) bool
	TableExists(db, table string) bool
	SchemaByID(id int64) (*model.DatabaseMeta, bool)

	LoadAllFromMeta(m *meta.Meta) error
}

type infoschema struct {
	dbs   map[string]*model.DatabaseMeta
	tbs   map[string]map[string]table.Table
	dbsId map[int64]*model.DatabaseMeta
	tbsId map[int64]table.Table

	// schemaMetaVersion is the version of schema, and we should check version when change schema.
	schemaMetaVersion int64
}

func newInfoSchema() InfoSchema {
	return &infoschema{
		dbs:   make(map[string]*model.DatabaseMeta),
		tbs:   make(map[string]map[string]table.Table),
		dbsId: make(map[int64]*model.DatabaseMeta),
		tbsId: make(map[int64]table.Table),
	}
}

func (is *infoschema) GetDatabaseMetaByName(name string) (*model.DatabaseMeta, bool) {
	dbMeta, ok := is.dbs[name]
	return dbMeta, ok
}

func (is *infoschema) ListDatabases() []*model.DatabaseMeta {
	res := make([]*model.DatabaseMeta, 0)
	for _, d := range is.dbs {
		res = append(res, d)
	}
	return res
}

func (is *infoschema) ListTablesByDatabase(db string) []*model.TableMeta {
	res := make([]*model.TableMeta, 0)
	if tbls := is.tbs[db]; tbls != nil {
		for _, t := range tbls {
			res = append(res, t.Meta())
		}
	}
	return res
}

func (is *infoschema) GetTableMetaByName(db, name string) (*model.TableMeta, error) {
	var tbMap map[string]table.Table
	var ok bool
	if tbMap, ok = is.tbs[db]; !ok {
		return nil, meta.ErrDBNotExists
	}
	if tb, ok := tbMap[name]; ok {
		return tb.Meta(), nil
	}
	return nil, meta.ErrTableNotExists
}

func (is *infoschema) GetTableByName(db, name string) (table.Table, error) {
	var tbMap map[string]table.Table
	var ok bool
	if tbMap, ok = is.tbs[db]; !ok {
		return nil, meta.ErrDBNotExists
	}
	if tb, ok := tbMap[name]; ok {
		return tb, nil
	}
	return nil, meta.ErrTableNotExists
}

func (is *infoschema) SchemaMetaVersion() int64 {
	return is.schemaMetaVersion
}

func (is *infoschema) SchemaExists(name string) bool {
	_, ok := is.dbs[name]
	return ok
}

func (is *infoschema) TableExists(db, table string) bool {
	if db, ok := is.tbs[db]; ok {
		if _, ok := db[table]; ok {
			return true
		}
	}
	return false
}

func (is *infoschema) SchemaByID(id int64) (*model.DatabaseMeta, bool) {
	db, ok := is.dbsId[id]
	return db, ok
}

func (is *infoschema) LoadAllFromMeta(m *meta.Meta) error {
	dbs, err := m.ListDatabases()
	if err != nil {
		return errors.Trace(err)
	}
	for _, db := range dbs {
		is.dbs[db.Database] = db
		is.dbsId[db.Id] = db
		tbs, err := m.ListTables(db.Id)
		if err != nil {
			return errors.Trace(err)
		}
		tbMap := make(map[string]table.Table)
		for _, tb := range tbs {
			t := tables.MakeTableFromMeta(tb)
			tbMap[tb.TableName] = t
			is.tbsId[tb.Id] = t
		}
		is.tbs[db.Database] = tbMap
	}

	is.schemaMetaVersion, err = m.GetSchemaVersion()
	return err
}

// Handler is a wrapper for atomic get and set.
type Handler struct {
	current atomic.Value
	//new     InfoSchema
}

func NewHandler() *Handler {
	return &Handler{}
}

func (h *Handler) Set(is InfoSchema) {
	h.current.Store(is)
}

func (h *Handler) Get() InfoSchema {
	v := h.current.Load()
	if v == nil {
		return nil
	}
	is := v.(InfoSchema)
	return is
}

func (h *Handler) IsValid() bool {
	return h.current.Load() != nil
}

func (h *Handler) InitCurrentInfoSchema(m *meta.Meta) error {
	is := newInfoSchema()
	err := is.LoadAllFromMeta(m)
	if err != nil {
		return errors.Trace(err)
	}
	h.current.Store(is)
	return nil
}

// Schema error codes.
const (
	codeDBDropExists      terror.ErrCode = 1008
	codeDatabaseNotExists                = 1049
	codeTableNotExists                   = 1146
	codeColumnNotExists                  = 1054

	codeCannotAddForeign    = 1215
	codeForeignKeyNotExists = 1091
	codeWrongFkDef          = 1239

	codeDatabaseExists   = 1007
	codeTableExists      = 1050
	codeBadTable         = 1051
	codeBadUser          = 3162
	codeColumnExists     = 1060
	codeIndexExists      = 1831
	codeMultiplePriKey   = 1068
	codeTooManyKeyParts  = 1070
	codeKeyNameDuplicate = 1061
	codeKeyNotExists     = 1176
)

func init() {
	schemaMySQLErrCodes := map[terror.ErrCode]uint16{
		codeDBDropExists:        mysql.ErrDBDropExists,
		codeDatabaseNotExists:   mysql.ErrBadDB,
		codeTableNotExists:      mysql.ErrNoSuchTable,
		codeColumnNotExists:     mysql.ErrBadField,
		codeCannotAddForeign:    mysql.ErrCannotAddForeign,
		codeWrongFkDef:          mysql.ErrWrongFkDef,
		codeForeignKeyNotExists: mysql.ErrCantDropFieldOrKey,
		codeDatabaseExists:      mysql.ErrDBCreateExists,
		codeTableExists:         mysql.ErrTableExists,
		codeBadTable:            mysql.ErrBadTable,
		codeBadUser:             mysql.ErrBadUser,
		codeColumnExists:        mysql.ErrDupFieldName,
		codeIndexExists:         mysql.ErrDupIndex,
		codeMultiplePriKey:      mysql.ErrMultiplePriKey,
		codeTooManyKeyParts:     mysql.ErrTooManyKeyParts,
		codeKeyNameDuplicate:    mysql.ErrDupKeyName,
		codeKeyNotExists:        mysql.ErrKeyDoesNotExist,
	}
	terror.ErrClassToMySQLCodes[terror.ClassSchema] = schemaMySQLErrCodes
}
