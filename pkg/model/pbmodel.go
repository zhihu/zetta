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

package model

import (
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/types"
	tspb "github.com/zhihu/zetta-proto/pkg/tablestore"
)

var (
	AttrKeyMode   = "mode"
	AttrKeyLayout = "layout"

	AttrFlexible = "flexible"
	AttrFixed    = "fixed"
	AttrCompact  = "compact"
	AttrSparse   = "sparse"
)

type TableMeta struct {
	tspb.TableMeta
	State SchemaState
	// UpdateTS is used to record the timestamp of updating the table's schema information.
	// These changing schema operations don't include 'truncate table' and 'rename table'.
	UpdateTS          uint64              `json:"update_timestamp"`
	MaxColumnID       int64               `json:"max_col_id"`
	MaxColumnFamilyID int64               `json:"max_cf_id`
	MaxIndexID        int64               `json:"max_idx_id"`
	Columns           []*ColumnMeta       `json:"columns"`
	ColumnFamilies    []*ColumnFamilyMeta `json:"columnfamilies"`
	Indices           []*IndexMeta        `json:"indices"`
}

func NewTableMetaFromPb(tm *tspb.TableMeta) *TableMeta {
	ntm := &TableMeta{
		TableMeta: *tm,
	}
	ntm.transferColumns()
	ntm.transferColumnFamilies()
	return ntm
}

func NewTableMetaFromPbReq(tmr *tspb.CreateTableRequest) *TableMeta {
	tm := NewTableMetaFromPb(tmr.GetTableMeta())
	indices := tmr.GetIndexes()
	tm.addIndices(indices)
	return tm
}

func (tm *TableMeta) GetIndexes() []*IndexMeta {
	return tm.Indices
}

func (tm *TableMeta) addIndices(indices []*tspb.IndexMeta) {
	nindices := make([]*IndexMeta, 0)
	for _, i := range indices {
		nindices = append(nindices, NewIndexMetaFromPb(i))
	}
	tm.Indices = nindices
}

func (tm *TableMeta) GetPrimaryFieldTypes() []*types.FieldType {
	res := make([]*types.FieldType, 0)
	for _, p := range tm.TableMeta.GetPrimaryKey() {
		for _, c := range tm.Columns {
			if p == c.Name {
				res = append(res, c.FieldType)
			}
		}
	}
	return res
}

func (tm *TableMeta) GetColumnMap() (map[string]*ColumnMeta, map[int64]*ColumnMeta) {
	resName := make(map[string]*ColumnMeta)
	resId := make(map[int64]*ColumnMeta)
	for _, c := range tm.Columns {
		resName[c.Name] = c
		resId[c.Id] = c
	}
	return resName, resId
}

func (tm *TableMeta) transferColumns() {
	clms := make([]*ColumnMeta, 0)
	for _, c := range tm.TableMeta.Columns {
		clms = append(clms, NewColumnMetaFromPb(c))
	}
	tm.Columns = clms
}

func (tm *TableMeta) transferColumnFamilies() {
	cfs := make([]*ColumnFamilyMeta, 0)
	for _, cf := range tm.TableMeta.ColumnFamilies {
		ncf := &ColumnFamilyMeta{
			ColumnFamilyMeta: *cf,
		}
		cfs = append(cfs, ncf)
	}
	tm.ColumnFamilies = cfs
}

func (tm *TableMeta) FindIndexByName(name string) *IndexMeta {
	for _, idx := range tm.Indices {
		if idx.Name == name {
			return idx
		}
	}
	return nil
}

// TODO: Partition table.
type PartitionInfoZ struct{}

// GetPartitionInfo returns the partition information.
func (t *TableMeta) GetPartitionInfo() *PartitionInfo {
	return nil
}

type DatabaseMeta struct {
	tspb.DatabaseMeta
	State SchemaState `json:"state"`
}

func NewDatabaseMetaFromPb(dbmeta *tspb.DatabaseMeta) *DatabaseMeta {
	return &DatabaseMeta{
		DatabaseMeta: *dbmeta,
	}
}

func NewDatabaseMetaFromPbReq(dbreq *tspb.CreateDatabaseRequest) *DatabaseMeta {
	return &DatabaseMeta{
		DatabaseMeta: tspb.DatabaseMeta{
			Database: dbreq.Database,
		},
	}
}

type ColumnFamilyMeta struct {
	tspb.ColumnFamilyMeta
	State SchemaState `json:"state"`
}

func DefaultColumnFamilyMeta() *ColumnFamilyMeta {
	return &ColumnFamilyMeta{
		ColumnFamilyMeta: tspb.ColumnFamilyMeta{
			Id:   0,
			Name: "default",
			Attributes: map[string]string{
				AttrKeyMode:   AttrFixed,
				AttrKeyLayout: AttrCompact,
			},
		},
	}
}

type ColumnMeta struct {
	tspb.ColumnMeta
	*types.FieldType   `json:"type"`
	Offset             int         `json:"offset"`
	OriginDefaultValue interface{} `json:"origin_default"`
	DefaultValue       interface{} `json:"default"`
	State              SchemaState `json:"state"`
}

func (c *ColumnMeta) TypeTransfer() {
	switch c.ColumnType.Code {
	case tspb.TypeCode_BOOL:
		c.FieldType = types.NewFieldType(mysql.TypeTiny)
	case tspb.TypeCode_INT64:
		c.FieldType = types.NewFieldType(mysql.TypeLonglong)
	case tspb.TypeCode_FLOAT64:
		c.FieldType = types.NewFieldType(mysql.TypeDouble)
	case tspb.TypeCode_TIMESTAMP:
		c.FieldType = types.NewFieldType(mysql.TypeTimestamp)
	case tspb.TypeCode_DATE:
		c.FieldType = types.NewFieldType(mysql.TypeDate)
	case tspb.TypeCode_STRING:
		c.FieldType = types.NewFieldType(mysql.TypeVarchar)
	case tspb.TypeCode_BYTES:
		c.FieldType = types.NewFieldType(mysql.TypeBit)
		// c.FieldType = types.NewFieldType(mysql.TypeMediumBlob)
	case tspb.TypeCode_ARRAY:
		c.FieldType = types.NewFieldType(mysql.TypeSet)
	case tspb.TypeCode_STRUCT:
		c.FieldType = types.NewFieldType(mysql.TypeJSON)
	}
}

func NewColumnMetaFromPb(cm *tspb.ColumnMeta) *ColumnMeta {
	ncm := &ColumnMeta{
		ColumnMeta: *cm,
	}
	ncm.TypeTransfer()
	return ncm
}

type IndexMeta struct {
	tspb.IndexMeta
	State SchemaState `json:"state"`
}

func NewIndexMetaFromPb(idxm *tspb.IndexMeta) *IndexMeta {
	nidxm := &IndexMeta{
		IndexMeta: *idxm,
	}
	return nidxm
}

func NewIndexMetaFromPbReq(idxreq *tspb.CreateIndexRequest) *IndexMeta {
	idx := &IndexMeta{
		IndexMeta: *idxreq.Indexes,
	}
	return idx
}
