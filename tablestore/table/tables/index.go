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
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/zhihu/zetta/pkg/model"
	"github.com/zhihu/zetta/pkg/tablecodec"
)

type IndexInfo struct {
	ID          int64
	IndexName   string
	cols        []string
	keys        []*model.ColumnMeta
	fieldTypes  []*types.FieldType
	meta        *model.IndexMeta
	IndexPrefix kv.Key
}

func NewIndexInfo(meta *model.IndexMeta) *IndexInfo {
	return &IndexInfo{
		ID:         meta.Id,
		cols:       meta.DefinedColumns,
		keys:       make([]*model.ColumnMeta, 0),
		fieldTypes: make([]*types.FieldType, 0),
		meta:       meta,
	}

}

func (ii *IndexInfo) Keys() []*model.ColumnMeta {
	return ii.keys
}

func (ii *IndexInfo) FieldTypes() []*types.FieldType {
	return ii.fieldTypes
}

func (ii *IndexInfo) FromTableMeta(tbl *model.TableMeta, colIndex map[string]int) {
	for _, col := range ii.cols {
		colMeta := tbl.Columns[colIndex[col]]
		ii.keys = append(ii.keys, colMeta)
		ii.fieldTypes = append(ii.fieldTypes, colMeta.FieldType)
	}
	ii.IndexPrefix = tablecodec.EncodeTableIndexPrefix(tbl.Id, ii.ID)
}

// BuildIndex build the index based on recordKey
func (ii *IndexInfo) BuildIndex(tbMeta *model.TableMeta, recordKey, recordValue kv.Key) ([]byte, []byte, error) {
	var (
		indexKey          kv.Key
		encodedPrimaryKey kv.Key
		err               error
	)
	fieldTypeMap := ii.getColumnTypeMap(tbMeta)
	// Decode the index datums from record value
	// Result is a map[colID]datum
	indexDatums, err := tablecodec.DecodeRowWide(recordValue, fieldTypeMap, time.Local, nil)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	primaryFieldTypes := tbMeta.GetPrimaryFieldTypes()
	// Decode the primary datums from record key, or we can extract encoded primary key from recordkey.
	_, primaryDatums, _, _, err := tablecodec.DecodeRecordKey(recordKey, primaryFieldTypes, time.Local)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	sc := &stmtctx.StatementContext{TimeZone: time.Local}
	if ii.meta.IndexMeta.Unique {
		indexKey, err = tablecodec.EncodeIndexUnique(sc, tbMeta.Id, ii.ID, ii.flattenIndexDatums(indexDatums))
	} else {
		indexKey, err = tablecodec.EncodeIndex(sc, tbMeta.Id, ii.ID, ii.flattenIndexDatums(indexDatums), primaryDatums)
	}
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	encodedPrimaryKey = tablecodec.ExtractEncodedPrimaryKey(recordKey)
	return indexKey, encodedPrimaryKey, nil
}

func (ii *IndexInfo) flattenIndexDatums(datumMap map[int64]types.Datum) []types.Datum {
	res := make([]types.Datum, 0)
	for _, c := range ii.keys {
		if d, ok := datumMap[c.Id]; ok {
			res = append(res, d)
		}
	}
	return res
}

func (ii *IndexInfo) Init(tbMeta *model.TableMeta) {
	nameMap, _ := tbMeta.GetColumnMap()
	for _, col := range ii.cols {
		ii.keys = append(ii.keys, nameMap[col])
	}
}

func (ii *IndexInfo) getColumnTypeMap(tbMeta *model.TableMeta) map[int64]*types.FieldType {
	var res = make(map[int64]*types.FieldType)
	for _, c := range tbMeta.Columns {
		for _, ic := range ii.cols {
			if ic == c.Name {
				res[c.Id] = c.FieldType
			}
		}
	}
	return res
}
