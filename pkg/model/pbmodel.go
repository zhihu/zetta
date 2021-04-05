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
	"encoding/json"

	"github.com/pingcap/parser/ast"
	parser_model "github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/types"
	"github.com/pingcap/pd/v4/server/schedule/placement"
	tspb "github.com/zhihu/zetta-proto/pkg/tablestore"
	"github.com/zhihu/zetta/tablestore/rpc"
)

const (
	AttrKeyMode   = "mode"
	AttrKeyLayout = "layout"

	AttrFlexible = "flexible"
	AttrFixed    = "fixed"
	AttrCompact  = "compact"
	AttrSparse   = "sparse"
)

type TableMeta struct {
	Id int64 `json:"id"`
	tspb.TableMeta
	parser_model.TableInfo

	MaxColumnFamilyID int64               `json:"max_cf_id`
	Columns           []*ColumnMeta       `json:"columns"`
	ColumnFamilies    []*ColumnFamilyMeta `json:"columnfamilies"`
	Indices           []*IndexMeta        `json:"indices"`
	Rules             []*placement.Rule   `json:"rules"`
}

func NewTableMetaFromPb(tm *tspb.TableMeta) *TableMeta {
	ntm := &TableMeta{
		TableMeta: *tm,
		TableInfo: parser_model.TableInfo{
			Name: parser_model.NewCIStr(tm.TableName),
		},
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

func (tm *TableMeta) GetColumnIDMap() map[string]int {
	res := make(map[string]int, len(tm.Columns))
	for i, col := range tm.Columns {
		res[col.Name] = i
	}
	return res
}

func (tm *TableMeta) ToTableInfo() *parser_model.TableInfo {
	return &tm.TableInfo
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
			if p == c.ColumnMeta.Name {
				res = append(res, &c.FieldType)
			}
		}
	}
	return res
}

func (tm *TableMeta) GetColumnMap() (map[string]*ColumnMeta, map[int64]*ColumnMeta) {
	resName := make(map[string]*ColumnMeta)
	resId := make(map[int64]*ColumnMeta)
	for _, c := range tm.Columns {
		resName[c.ColumnMeta.Name] = c
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

func (tm *TableMeta) FindColumnByName(name string) *ColumnMeta {
	for _, col := range tm.Columns {
		if col.ColumnMeta.Name == name {
			return col
		}
	}
	return nil
}

func (tm *TableMeta) FindIndexByName(name string) *IndexMeta {
	for _, idx := range tm.Indices {
		if idx.Name == name {
			return idx
		}
	}
	return nil
}

//TODO: One column may be in multiple index. Now just return the first.
func (tm *TableMeta) GetIndexByColumnName(name string) (int, *IndexMeta) {
	for _, idx := range tm.Indices {
		for i, col := range idx.DefinedColumns {
			if col == name {
				return i, idx
			}
		}
	}

	for i, col := range tm.PrimaryKey {
		if col == name {
			return i, nil
		}
	}
	return -1, nil
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
	Name string `json:"name"`
	Id   int64  `json:"id"`
	tspb.ColumnMeta
	parser_model.ColumnInfo
}

type ColumnMetaDummy struct {
	Name string `json:"name"`
	Id   int64  `json:"id"`
	tspb.ColumnMeta
	parser_model.ColumnInfo
}

func (cm *ColumnMeta) UnmarshalJSON(b []byte) error {
	cmd := ColumnMetaDummy{}
	err := json.Unmarshal(b, &cmd)
	if err != nil {
		return err
	}
	cm.Name = cmd.Name
	cm.ColumnMeta = cmd.ColumnMeta
	cm.ColumnInfo = cmd.ColumnInfo
	cm.ColumnInfo.Name = parser_model.NewCIStr(cm.Name)
	cm.ColumnMeta.Name = cm.Name
	cm.ColumnMeta.Id = cmd.Id
	cm.ColumnInfo.ID = cmd.Id
	cm.Id = cmd.Id
	return nil
}

func NewColumnMetaFromColumnDef(columnDef *ast.ColumnDef) *ColumnMeta {
	cm := &ColumnMeta{}
	cm.ColumnMeta.Name = columnDef.Name.Name.L
	cm.ColumnInfo.Name = columnDef.Name.Name
	cm.Name = cm.ColumnMeta.Name
	cm.FieldType = *columnDef.Tp
	return cm
}

func removeOnUpdateNowFlag(c *ColumnMeta) {
	// For timestamp Col, if it is set null or default value,
	// OnUpdateNowFlag should be removed.
	if mysql.HasTimestampFlag(c.Flag) {
		c.Flag &= ^mysql.OnUpdateNowFlag
	}
}

func (c *ColumnMeta) ToColumnInfo() *parser_model.ColumnInfo {
	return &c.ColumnInfo
}

func GetPrimaryKeysFromConstraints(cons *ast.Constraint) []string {
	pkeys := make([]string, 0)
	for _, key := range cons.Keys {
		pkeys = append(pkeys, key.Column.Name.L)
	}
	return pkeys
}

func FieldTypeToProtoType(ft *types.FieldType) *tspb.Type {
	switch ft.Tp {
	case mysql.TypeTiny:
		return rpc.BoolType()
	case mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong, mysql.TypeShort:
		return rpc.IntType()
	case mysql.TypeFloat, mysql.TypeDouble, mysql.TypeDecimal:
		return rpc.FloatType()
	case mysql.TypeTimestamp, mysql.TypeDuration:
		return rpc.TimeType()
	case mysql.TypeDate, mysql.TypeDatetime:
		return rpc.DateType()
	case mysql.TypeVarString, mysql.TypeVarchar, mysql.TypeString:
		return rpc.StringType()
	case mysql.TypeBit, mysql.TypeBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob, mysql.TypeTinyBlob:
		return rpc.BytesType()
	case mysql.TypeSet:
		return rpc.ListType(rpc.StringType())
	case mysql.TypeJSON:
		return &tspb.Type{Code: tspb.TypeCode_STRUCT}
	default:
		return &tspb.Type{Code: tspb.TypeCode_TYPE_CODE_UNSPECIFIED}
	}
}

func (c *ColumnMeta) TypeTransfer() {
	switch c.ColumnType.Code {
	case tspb.TypeCode_BOOL:
		c.FieldType = *types.NewFieldType(mysql.TypeTiny)
	case tspb.TypeCode_INT64:
		c.FieldType = *types.NewFieldType(mysql.TypeLonglong)
	case tspb.TypeCode_FLOAT64:
		c.FieldType = *types.NewFieldType(mysql.TypeDouble)
	case tspb.TypeCode_TIMESTAMP:
		c.FieldType = *types.NewFieldType(mysql.TypeTimestamp)
	case tspb.TypeCode_DATE:
		c.FieldType = *types.NewFieldType(mysql.TypeDate)
	case tspb.TypeCode_STRING:
		c.FieldType = *types.NewFieldType(mysql.TypeVarchar)
	case tspb.TypeCode_BYTES:
		c.FieldType = *types.NewFieldType(mysql.TypeBlob)
	case tspb.TypeCode_ARRAY:
		c.FieldType = *types.NewFieldType(mysql.TypeSet)
	case tspb.TypeCode_STRUCT:
		c.FieldType = *types.NewFieldType(mysql.TypeJSON)
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
	IsPrimary bool
	State     SchemaState `json:"state"`
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

func NewIndexMetaFromConstraits(cons *ast.Constraint) *IndexMeta {
	idx := &IndexMeta{}
	idx.DefinedColumns = make([]string, len(cons.Keys))
	idx.Name = cons.Name
	switch cons.Tp {
	case ast.ConstraintIndex:
		idx.Unique = false
	case ast.ConstraintUniq:
		idx.Unique = true
	}
	for i, key := range cons.Keys {
		idx.DefinedColumns[i] = key.Column.Name.L
	}
	return idx
}
