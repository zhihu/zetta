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
	parser_model "github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/zhihu/zetta/pkg/codec"
	"github.com/zhihu/zetta/pkg/model"
	"github.com/zhihu/zetta/pkg/tablecodec"
	"github.com/zhihu/zetta/tablestore/table"
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
		ii.fieldTypes = append(ii.fieldTypes, &colMeta.FieldType)
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
			if ic == c.ColumnMeta.Name {
				res[c.Id] = &c.FieldType
			}
		}
	}
	return res
}

/*------- Below is the zetta implementation of Index interface in tidb ---------*/

// zettaIndex is the datastructure for index data of zetta in the KV store.
type zettaIndex struct {
	idxInfo                *parser_model.IndexInfo
	tblInfo                *parser_model.TableInfo
	prefix                 kv.Key
	containNonBinaryString bool
}

func NewZettaIndex(physicalID int64, tblInfo *parser_model.TableInfo, indexInfo *parser_model.IndexInfo) table.Index {
	index := &zettaIndex{
		idxInfo: indexInfo,
		tblInfo: tblInfo,
		prefix:  tablecodec.EncodeTableIndexPrefix(physicalID, indexInfo.ID),
	}
	index.containNonBinaryString = index.checkContainNonBinaryString()
	return index
}

/*------- helper functions -------*/

func (c *zettaIndex) checkContainNonBinaryString() bool {
	for _, idxCol := range c.idxInfo.Columns {
		col := c.tblInfo.Columns[idxCol.Offset]
		if col.EvalType() == types.ETString && !mysql.HasBinaryFlag(col.Flag) {
			return true
		}
	}
	return false
}

func (c *zettaIndex) getIndexKeyBuf(buf []byte, defaultCap int) []byte {
	if buf != nil {
		return buf[:0]
	}

	return make([]byte, 0, defaultCap)
}

/*------- interface implementation -------*/

func (c *zettaIndex) Meta() *parser_model.IndexInfo {
	return c.idxInfo
}

// GenIndexKey generates storage key for index values. Returned distinct indicates whether the
// indexed values should be distinct in storage (i.e. whether handle is encoded in the key).
func (c *zettaIndex) GenIndexKey(sc *stmtctx.StatementContext, indexedValues []types.Datum,
	h table.Handle, buf []byte) (key []byte, distinct bool, err error) {
	key = c.getIndexKeyBuf(buf, len(c.prefix)+len(indexedValues)*9+9)
	key = append(key, []byte(c.prefix)...)
	key, err = codec.EncodeKey(sc, key, indexedValues...)
	if err != nil {
		return nil, false, err
	}
	if !distinct && h != nil {
		if h.IsInt() {
			key, err = codec.EncodeKey(sc, key, types.NewDatum(h.IntValue()))
		} else {
			key = append(key, h.Encoded()...)
		}
	}
	return
}

func (c *zettaIndex) Create(ctx sessionctx.Context, rm kv.RetrieverMutator, indexedValues []types.Datum,
	h table.Handle, opts ...table.CreateIdxOptFunc) (table.Handle, error) {
	return nil, nil
}

func (c *zettaIndex) Delete(sc *stmtctx.StatementContext, m kv.Mutator, indexedValues []types.Datum, h table.Handle) error {
	return nil
}

func (c *zettaIndex) Drop(rm kv.RetrieverMutator) error {
	return nil
}

// Exist supports check index exists or not.
func (c *zettaIndex) Exist(sc *stmtctx.StatementContext, rm kv.RetrieverMutator,
	indexedValues []types.Datum, h table.Handle) (bool, table.Handle, error) {
	return false, nil, nil
}

// Seek supports where clause.
func (c *zettaIndex) Seek(sc *stmtctx.StatementContext, r kv.Retriever, indexedValues []types.Datum) (iter table.IndexIterator, hit bool, err error) {
	return nil, false, nil
}

// SeekFirst supports aggregate min and ascend order by.
func (c *zettaIndex) SeekFirst(r kv.Retriever) (iter table.IndexIterator, err error) {
	return nil, nil
}

// FetchValues fetched index column values in a row.
// Param columns is a reused buffer, if it is not nil, FetchValues will fill the index values in it,
// and return the buffer, if it is nil, FetchValues will allocate the buffer instead.
func (c *zettaIndex) FetchValues(row []types.Datum, columns []types.Datum) ([]types.Datum, error) {
	return nil, nil
}
