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

package tables

import (
	"errors"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx/stmtctx"

	"github.com/pingcap/tidb/types"

	tspb "github.com/zhihu/zetta-proto/pkg/tablestore"
	"github.com/zhihu/zetta/pkg/model"
	"github.com/zhihu/zetta/pkg/tablecodec"
	"github.com/zhihu/zetta/tablestore/rpc"
	"github.com/zhihu/zetta/tablestore/table"
)

const (
	DefaultColumnFamily = "default"

	AttrKeyMode   = "mode"
	AttrKeyLayout = "layout"

	AttrFlexible = "flexible"
	AttrFixed    = "fixed"
	AttrCompact  = "compact"
	AttrSparse   = "sparse"
)

var ()

type tableCommon struct {
	Database string
	tableID  int64
	// physicalTableID is a unique int64 to identify a physical table.
	physicalTableID int64
	recordPrefix    kv.Key
	indexPrefix     kv.Key

	Columns []*model.ColumnMeta
	Indices map[string]*IndexInfo
	meta    *model.TableMeta
	CFs     []*model.ColumnFamilyMeta

	// primarykey map: dict<primary-key-name, column index in column list>
	pkeyMap map[string]int
	pkeys   []*model.ColumnMeta
	pkCols  int

	// column map: dict<column-name, column-id in column list >
	colIndex map[string]int
	// reverse index for columns
	revIndex map[int]int
	// columnId -> fieldType map
	colFtMap map[int64]*types.FieldType

	colCfMap map[string]string
	cfIDMap  map[string]*model.ColumnFamilyMeta
	// recordPrefix and indexPrefix are generated using physicalTableID.
	defaultCFID int64

	txn kv.Transaction
}

type Table struct {
	tableCommon
}

func (t *Table) GetPhysicalID() int64 {
	return t.physicalTableID
}

var _ table.Table = &Table{}

func MakeTableFromMeta(tableMeta *model.TableMeta) table.Table {
	t := tableCommon{
		Database: tableMeta.GetDatabase(),
		tableID:  tableMeta.Id,
		Columns:  tableMeta.Columns,
		Indices:  make(map[string]*IndexInfo),
		CFs:      tableMeta.ColumnFamilies,

		pkeyMap: make(map[string]int),
		pkeys:   make([]*model.ColumnMeta, 0),
		pkCols:  len(tableMeta.GetPrimaryKey()),

		colIndex: make(map[string]int),
		revIndex: make(map[int]int),

		colFtMap: make(map[int64]*types.FieldType),

		colCfMap: make(map[string]string),
		cfIDMap:  make(map[string]*model.ColumnFamilyMeta),

		meta: tableMeta,
	}
	for i, col := range t.Columns {
		if col.IsPrimary {
			t.pkeyMap[col.Name] = i
		}

		t.colIndex[col.Name] = i
		t.colFtMap[col.Id] = col.FieldType
		t.colCfMap[col.Name] = col.Family
	}

	for _, pkey := range tableMeta.PrimaryKey {
		colMeta := t.Columns[t.colIndex[pkey]]
		t.pkeys = append(t.pkeys, colMeta)
		t.pkeyMap[pkey] = t.colIndex[pkey]
	}

	for _, index := range tableMeta.Indices {
		ii := NewIndexInfo(index)
		ii.FromTableMeta(tableMeta, t.colIndex)
		t.Indices[index.Name] = ii
	}

	for _, cf := range tableMeta.ColumnFamilies {
		t.cfIDMap[cf.Name] = cf

	}
	t.defaultCFID = t.cfIDMap[DefaultColumnFamily].GetId()

	initTableCommon(&t, tableMeta)

	return &Table{t}
}

func initTableCommon(t *tableCommon, tableMeta *model.TableMeta) {
	t.physicalTableID = tableMeta.Id
	t.recordPrefix = tablecodec.GenRecordKeyPrefix(tableMeta.Id)
	t.indexPrefix = tablecodec.GenIndexKeyPrefix(tableMeta.Id)
}

func TableFromMeta(tableMeta *model.TableMeta, txn kv.Transaction) table.Table {
	t := tableCommon{
		Database: tableMeta.GetDatabase(),
		tableID:  tableMeta.Id,
		txn:      txn,
		Columns:  tableMeta.Columns,
		Indices:  make(map[string]*IndexInfo),
		CFs:      tableMeta.ColumnFamilies,

		pkeyMap: make(map[string]int),
		pkeys:   make([]*model.ColumnMeta, 0),
		pkCols:  len(tableMeta.GetPrimaryKey()),

		colIndex: make(map[string]int),
		revIndex: make(map[int]int),

		colFtMap: make(map[int64]*types.FieldType),

		colCfMap: make(map[string]string),
		cfIDMap:  make(map[string]*model.ColumnFamilyMeta),

		meta: tableMeta,
	}
	for i, col := range t.Columns {
		t.colIndex[col.Name] = i
		t.colFtMap[col.Id] = col.FieldType
		t.colCfMap[col.Name] = col.Family
	}

	for _, pkey := range tableMeta.PrimaryKey {
		colMeta := t.Columns[t.colIndex[pkey]]
		t.pkeys = append(t.pkeys, colMeta)
		t.pkeyMap[pkey] = t.colIndex[pkey]
	}

	for _, index := range tableMeta.Indices {
		ii := NewIndexInfo(index)
		ii.FromTableMeta(tableMeta, t.colIndex)
		t.Indices[index.Name] = ii
	}

	for _, cf := range tableMeta.ColumnFamilies {
		t.cfIDMap[cf.Name] = cf
	}

	return &Table{t}
}

func (t *tableCommon) Meta() *model.TableMeta {
	return t.meta
}

func (t *tableCommon) RecordPrefix() kv.Key {
	return t.recordPrefix
}

func (t *tableCommon) IndexPrefix() kv.Key {
	return t.indexPrefix
}

func (t *tableCommon) getCF(cfID int64) *model.ColumnFamilyMeta {
	for _, cf := range t.meta.ColumnFamilies {
		if cf.Id == cfID {
			return cf
		}
	}
	return nil
}

func (t *tableCommon) prepareCFColumns(rowkeys *tspb.ListValue, family string, columns []string) (cols []*columnEntry, pks []types.Datum, cfMeta *model.ColumnFamilyMeta, colmap map[string]*columnEntry, err error) {
	_, err = CheckOnce(columns)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	cols = []*columnEntry{}
	colmap = make(map[string]*columnEntry)
	if family == "" {
		family = DefaultColumnFamily
	}
	cf, ok := t.cfIDMap[family]
	if !ok {
		return nil, nil, nil, nil, status.Errorf(codes.FailedPrecondition, "cf %s not in table %s", family, t.meta.TableName)
	}
	cfMeta = cf
	for j, col := range columns {
		var entry *columnEntry
		if !isFlexible(cf) {
			i, ok := t.colIndex[col]
			if !ok {
				return nil, nil, nil, nil, status.Errorf(codes.InvalidArgument, "column %s not in table %s", col, t.meta.TableName)
			}
			entry = &columnEntry{
				id:   t.Columns[i].Id,
				name: t.Columns[i].Name,
				t:    t.Columns[i].ColumnType,
				cfID: t.cfIDMap[t.Columns[i].Family].Id,
				cf:   t.Columns[i].Family,
				ft:   t.Columns[i].FieldType,
				idx:  j,
			}
		} else {
			// Dynamic ColumnFamily
			entry = &columnEntry{
				name: col,
				t:    &tspb.Type{Code: tspb.TypeCode_TYPE_CODE_UNSPECIFIED},
				cfID: cf.Id,
				cf:   cf.Name,
				idx:  j,
			}
		}
		cols = append(cols, entry)
		colmap[col] = entry
	}
	pks, err = t.buildPrimaryKeyValues(rowkeys)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	return cols, pks, cfMeta, colmap, nil
}

//assert columns valid
func (t *tableCommon) prepareColumns(columns []string) (cols []*columnEntry, pks []*columnEntry, cfmap map[string]map[string]*columnEntry, err error) {
	_, err = CheckOnce(columns)
	if err != nil {
		return nil, nil, nil, err
	}
	cols = []*columnEntry{}
	cfmap = make(map[string]map[string]*columnEntry)

	for j, column := range columns {
		var entry *columnEntry
		cfname, col, err := resolveColumn(column)

		if err != nil {
			return nil, nil, nil, status.Error(codes.InvalidArgument, err.Error())
		}
		cf, ok := t.cfIDMap[cfname]
		if !ok {
			return nil, nil, nil, status.Errorf(codes.FailedPrecondition, "cf %s not in table %s", cfname, t.meta.TableName)
		}
		if !isFlexible(cf) {
			i, ok := t.colIndex[col]
			if !ok {
				return nil, nil, nil, status.Errorf(codes.InvalidArgument, "column %s not in table %s", col, t.meta.TableName)
			}
			entry = &columnEntry{
				id:   t.Columns[i].Id,
				name: t.Columns[i].Name,
				t:    t.Columns[i].ColumnType,
				cfID: t.cfIDMap[t.Columns[i].Family].Id,
				cf:   t.Columns[i].Family,
				ft:   t.Columns[i].FieldType,
				idx:  j,
			}
		} else {
			// Dynamic ColumnFamily
			entry = &columnEntry{
				name: col,
				t:    &tspb.Type{Code: tspb.TypeCode_TYPE_CODE_UNSPECIFIED},
				cfID: cf.Id,
				cf:   cf.Name,
				idx:  j,
			}
		}
		cols = append(cols, entry)

		if colmap, ok := cfmap[entry.cf]; !ok {
			cfmap[entry.cf] = map[string]*columnEntry{entry.name: entry}
		} else {
			colmap[entry.name] = entry
		}

	}

	pks, err = t.primaryKeyCols(cols)
	if err != nil {
		return nil, nil, nil, err
	}
	return cols, pks, cfmap, nil
}

func (t *tableCommon) isColumnFlexsible(col *columnEntry) bool {
	cf := t.cfIDMap[col.cf]
	return isFlexible(cf)
}

func (t *tableCommon) prefetchDataCache(cols []*columnEntry, lvs []*tspb.ListValue) (rows [][]types.Datum, err error) {
	for _, lv := range lvs {
		if len(cols) != len(lv.Values) {
			return nil, status.Errorf(codes.FailedPrecondition, "cols num not match value")
		}
		row := []types.Datum{}
		for i, v := range lv.GetValues() {
			var datum types.Datum
			if !t.isColumnFlexsible(cols[i]) {
				datum, err = datumFromTypeValue(cols[i].t, v)
				if err != nil {
					return nil, err
				}
			} else {
				datum, err = flexibleDatumFromProtoValue(v)
				if err != nil {
					return nil, err
				}
			}
			row = append(row, datum)
		}
		rows = append(rows, row)
	}
	return rows, nil
}

// build primary-key index list in tableMeta.Columns from input column string list
func (t *tableCommon) primaryKeyCols(cols []*columnEntry) ([]*columnEntry, error) {
	var entrys []*columnEntry
	for _, pkey := range t.pkeys {
		var i int
		for i = 0; i < len(cols); i++ {
			if pkey.Name == cols[i].name {
				break
			}
		}
		if i == len(cols) {
			return nil, status.Errorf(codes.InvalidArgument, "primary key column %v not included in write", pkey)
		}
		entrys = append(entrys, cols[i])
	}
	return entrys, nil
}

// func (t *tableCommon) buildPrimaryKeyValues

func (t *tableCommon) getCFColumns(cf *model.ColumnFamilyMeta) []string {
	cols := []string{}
	if !isFlexible(cf) {
		for _, col := range t.Columns {
			if col.GetFamily() == cf.Name {
				cols = append(cols, col.Name)
			}
		}
	}
	return cols
}

func genRecordKey(tableID int64, pkdats []types.Datum, cf *model.ColumnFamilyMeta, column string) (kv.Key, error) {
	var (
		sc = &stmtctx.StatementContext{TimeZone: time.Local}
	)
	if isLayoutCompact(cf) {
		recKey, err := tablecodec.EncodeRecordKeyWide(sc, tableID, pkdats, cf.Id)
		if err != nil {
			return nil, err
		}
		return recKey, nil
	}
	recKey, err := tablecodec.EncodeRecordKeyHigh(sc, tableID, pkdats, cf.Id, []byte(column))
	if err != nil {
		return nil, err
	}
	return recKey, nil
}

func buildRecordKeyCompact(tableID int64, pkdats []types.Datum, cf *model.ColumnFamilyMeta) (kv.Key, error) {
	return genRecordKey(tableID, pkdats, cf, "")
}

func buildRecordKeySparse(tableID int64, pkdats []types.Datum, cf *model.ColumnFamilyMeta, qualifiers ...string) ([]kv.Key, error) {
	recKeys := []kv.Key{}
	for _, col := range qualifiers {
		recKey, err := genRecordKey(tableID, pkdats, cf, col)
		if err != nil {
			return nil, err
		}
		recKeys = append(recKeys, recKey)
	}
	return recKeys, nil
}

func genRecordPrimaryCFPrefix(tableID int64, rows []types.Datum, cfID int64) (kv.Key, error) {
	var sc = &stmtctx.StatementContext{TimeZone: time.Local}
	rec, err := tablecodec.EncodeRecordPrimaryKeyCFPrefix(sc, tableID, rows, cfID)
	if err != nil {
		return nil, err
	}
	return kv.Key(rec), nil

}

func (t *tableCommon) datumsFromListValue(colTypes []*tspb.Type, vals []*tspb.Value) ([]types.Datum, error) {
	var colvals = make([]types.Datum, len(vals))
	for i, v := range vals {
		datum, err := datumFromTypeValue(colTypes[i], v)
		if err != nil {
			return nil, err
		}
		colvals[i] = datum
	}
	return colvals, nil
}

func (t *tableCommon) buildIndexKeyValues(index string, lsv *tspb.ListValue) ([]types.Datum, error) {
	var (
		colTypes = make([]*tspb.Type, 0)
		vals     = lsv.GetValues()
	)

	indexMeta, ok := t.Indices[index]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "index %s not found", index)
	}

	for _, col := range indexMeta.Keys() {
		colType := col.ColumnType
		colTypes = append(colTypes, colType)
	}

	delta := len(colTypes) - len(lsv.GetValues())
	if delta < 0 {
		return nil, status.Errorf(codes.FailedPrecondition, "key values (%v) exceeds column types (%v) in index: %v", len(colTypes), len(lsv.Values), index)
	}

	for c := 0; c < delta; c++ {
		vals = append(vals, rpc.NullProto())
	}
	return t.datumsFromListValue(colTypes, vals)
}

func (t *tableCommon) buildPrimaryKeyValues(lv *tspb.ListValue) ([]types.Datum, error) {
	var (
		colTypes = make([]*tspb.Type, 0)
		vals     = lv.GetValues()
	)

	for _, pkey := range t.pkeys {
		colType := pkey.ColumnType
		colTypes = append(colTypes, colType)
	}

	delta := len(colTypes) - len(lv.GetValues())
	if delta < 0 {
		return nil, status.Errorf(codes.FailedPrecondition, "key values (%v) exceeds column types (%v) in build primary key", len(colTypes), len(lv.Values))
	}

	for c := 0; c < delta; c++ {
		vals = append(vals, rpc.NullProto())
	}
	return t.datumsFromListValue(colTypes, vals)
}

func (t *tableCommon) buildPrimaryKeyFieldTypes() []*types.FieldType {
	var ftypes = make([]*types.FieldType, 0)
	for _, pkey := range t.pkeys {
		ftypes = append(ftypes, pkey.FieldType)
	}
	return ftypes
}

func CheckOnce(cols []string) (map[string]struct{}, error) {
	m := map[string]struct{}{}
	for _, col := range cols {
		_, ok := m[col]
		if ok {
			return nil, table.ErrDuplicateColumn.GenWithStack("column specified twice - %s", col)
		}

		m[col] = struct{}{}
	}
	return m, nil
}

func resolveColumn(column string) (string, string, error) {
	tokens := strings.Split(column, ":")
	if len(tokens) > 2 {
		return "", "", errors.New("invaild column name")
	}

	if len(tokens) == 2 {
		return tokens[0], tokens[1], nil
	}
	return DefaultColumnFamily, tokens[0], nil
}

func isFlexible(cf *model.ColumnFamilyMeta) bool {
	if mode, ok := cf.Attributes[AttrKeyMode]; ok {
		if mode == AttrFlexible {
			return true
		}
	}
	return false
}

func isLayoutCompact(cf *model.ColumnFamilyMeta) bool {
	if encoding, ok := cf.Attributes[AttrKeyLayout]; ok {
		return encoding == AttrCompact
	}
	return true
}
