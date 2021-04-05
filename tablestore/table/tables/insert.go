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
	"bytes"
	"context"
	"time"

	"github.com/pingcap/tidb/kv"

	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/logutil"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/zhihu/zetta/pkg/model"
	"github.com/zhihu/zetta/pkg/tablecodec"
	"github.com/zhihu/zetta/tablestore/mysql/sctx"
	"github.com/zhihu/zetta/tablestore/sessionctx"
	"github.com/zhihu/zetta/tablestore/table"

	tspb "github.com/zhihu/zetta-proto/pkg/tablestore"
)

type mutationContext struct {
	pks    []types.Datum
	pkmap  map[string]int
	family string
	cols   []*columnEntry
	cf     *model.ColumnFamilyMeta
	colmap map[string]*columnEntry
}

func (t *tableCommon) Insert(ctx context.Context, pkey *tspb.KeySet, family string, columns []string, values []*tspb.ListValue) error {
	return status.Errorf(codes.Unimplemented, "insert not implemented yet")
}

func (t *tableCommon) InsertOrUpdate(ctx context.Context, pkeyset *tspb.KeySet, family string, columns []string, values []*tspb.ListValue) error {
	if pkeyset == nil {
		return status.Errorf(codes.Canceled, "invalid pkeyset")
	}
	mutCtx, err := t.prepareCFColumns(pkeyset.Keys[0], family, columns)
	if err != nil {
		return err
	}

	rows, err := t.prefetchDataCache(mutCtx, values)
	if err != nil {
		logutil.Logger(ctx).Error("construct datum slice err", zap.Error(err))
		return err
	}

	for _, row := range rows {
		if err := t.insertCFRow(ctx, mutCtx, row); err != nil {
			logutil.Logger(ctx).Error("insert single row error", zap.Error(err))
			return err
		}
	}
	return nil
}

func (t *tableCommon) Update(ctx context.Context, pkey *tspb.KeySet, family string, columns []string, values []*tspb.ListValue) error {
	return status.Errorf(codes.Unimplemented, "update not implemented yet")
}

func (t *tableCommon) Replace(ctx context.Context, pkey *tspb.KeySet, family string, columns []string, values []*tspb.ListValue) error {
	return status.Errorf(codes.Unimplemented, "replace not implemented yet")
}

func (t *tableCommon) insertCFRow(ctx context.Context, mutCtx *mutationContext, row []types.Datum) error {
	// pks []types.Datum, cf *model.ColumnFamilyMeta, colmap map[string]*columnEntry,
	var (
		txn        = ctx.Value(sessionctx.TxnIDKey).(kv.Transaction)
		isFlexible = isFlexible(mutCtx.cf)
	)
	keys, vals, err := t.buildKeyValues(mutCtx, row)
	if err != nil {
		logutil.Logger(ctx).Error("build key values error", zap.Error(err))
		return err
	}

	for i, key := range keys {
		val := vals[i]
		if mutCtx.cf.Name == DefaultColumnFamily {
			oldVal, err := txn.Get(ctx, key)
			if err != nil && err != kv.ErrNotExist {
				logutil.Logger(ctx).Error("try to get record value error", zap.Error(err))
				return err
			}
			if bytes.Equal(val, oldVal) {
				continue
			}
		}
		if err := txn.Set(key, val); err != nil {
			logutil.Logger(ctx).Error("build record row data error while insertRow", zap.Error(err))
			return err
		}
	}

	if !isFlexible {
		if err := t.buildIndexForRow(ctx, mutCtx, row); err != nil {
			logutil.Logger(ctx).Error("build record row data error while insertRow", zap.Error(err))
		}
	}
	return nil
}

func (t *tableCommon) buildKeyValues(mutCtx *mutationContext, row []types.Datum) ([]kv.Key, [][]byte, error) {
	// pks []types.Datum, cf *model.ColumnFamilyMeta, colmap map[string]*columnEntry
	if isLayoutCompact(mutCtx.cf) {
		key, val, err := t.buildKeyRowCompact(mutCtx, row)
		if err != nil {
			return nil, nil, err
		}
		return []kv.Key{key}, [][]byte{val}, nil
	}
	return t.buildKeyRowHigh(mutCtx, row)
}

func (t *tableCommon) makeColumnEntry(column string) *columnEntry {
	colMeta := t.Columns[t.colIndex[column]]
	colEntry := &columnEntry{
		id:   colMeta.Id,
		name: colMeta.ColumnMeta.Name,
		t:    colMeta.ColumnType,
		cfID: t.cfIDMap[colMeta.Family].Id,
		cf:   colMeta.Family,
		ft:   &colMeta.FieldType,
		idx:  -1,
	}
	return colEntry
}

func (t *tableCommon) buildKeyRowHigh(mutCtx *mutationContext, row []types.Datum) ([]kv.Key, [][]byte, error) {
	// pks []types.Datum, cf *model.ColumnFamilyMeta, colmap map[string]*columnEntry
	var (
		rkeys = make([]kv.Key, 0)
		vals  = make([][]byte, 0)
		sc    = &stmtctx.StatementContext{TimeZone: time.Local}
	)

	if !isFlexible(mutCtx.cf) {
		columns := t.getCFColumns(mutCtx.cf)
		for _, column := range columns {
			if _, ok := mutCtx.colmap[column]; ok {
				continue
			}
			colEntry := t.makeColumnEntry(column)
			mutCtx.colmap[column] = colEntry
		}
	}

	for col, entry := range mutCtx.colmap {
		recKey, err := tablecodec.EncodeRecordKeyHigh(sc, t.tableID, mutCtx.pks, mutCtx.cf.Id, []byte(col))
		if err != nil {
			return nil, nil, err
		}
		var rowData = types.NewDatum(nil)
		if entry.idx != -1 {
			rowData = row[entry.idx]
		}
		valBody, err := tablecodec.EncodeRowHigh(sc, rowData, nil)
		if err != nil {
			return nil, nil, err
		}
		rkeys = append(rkeys, recKey)
		vals = append(vals, valBody)
	}
	return rkeys, vals, nil
}

func (t *tableCommon) buildKeyRowCompact(mutCtx *mutationContext, row []types.Datum) (kv.Key, []byte, error) {
	// pks []types.Datum, cf *model.ColumnFamilyMeta, colmap map[string]*columnEntry,
	var (
		sc      = &stmtctx.StatementContext{TimeZone: time.Local}
		valBody []byte
		i       = 0
	)

	recKey, err := tablecodec.EncodeRecordKeyWide(sc, t.tableID, mutCtx.pks, mutCtx.cf.Id)
	if err != nil {
		logutil.Logger(context.TODO()).Error("encode wide error", zap.Error(err))
		return nil, nil, err
	}
	if !isFlexible(mutCtx.cf) {
		columns := t.getCFColumns(mutCtx.cf)
		for _, column := range columns {
			if _, ok := mutCtx.colmap[column]; ok {
				continue
			}
			colEntry := t.makeColumnEntry(column)
			mutCtx.colmap[column] = colEntry
		}

		rowData := make([]types.Datum, len(mutCtx.colmap))
		colIDs := make([]int64, len(mutCtx.colmap))
		for _, col := range mutCtx.colmap {
			colIDs[i] = col.id
			if col.idx == -1 {
				if mutCtx.cf.Name == DefaultColumnFamily {
					if pkIdx, ok := mutCtx.pkmap[col.name]; ok {
						rowData[i] = mutCtx.pks[pkIdx]
					}
				} else {
					rowData[i] = types.NewDatum(nil)
				}
			} else {
				rowData[i] = row[col.idx]
			}
			i++
		}
		valBody, err = tablecodec.EncodeRow(sc, rowData, colIDs, nil, nil)
		if err != nil {
			return nil, nil, err
		}
	} else {
		rowMap := map[string]types.Datum{}
		for _, col := range mutCtx.colmap {
			rowMap[col.name] = row[col.idx]
		}
		valBody, err = tablecodec.EncodeRowFlexible(sc, rowMap, nil, nil)
		if err != nil {
			logutil.Logger(context.TODO()).Error("encode wide error", zap.Error(err))
			return nil, nil, err
		}
	}
	return recKey, valBody, nil

}

func (t *tableCommon) buildIndexForRow(ctx context.Context, mutCtx *mutationContext, row []types.Datum) error {

	// pks []types.Datum, colMap map[string]*columnEntry
	for _, index := range t.Indices {
		if err := t.addIndexRow(ctx, index, mutCtx, row); err != nil {
			return err
		}
	}
	return nil
}

func (t *tableCommon) addIndexRow(ctx context.Context, index *IndexInfo, mutCtx *mutationContext, row []types.Datum) error {
	// pks []types.Datum, colMap map[string]*columnEntry,
	var (
		sc         = &stmtctx.StatementContext{TimeZone: time.Local}
		values     = make([]types.Datum, len(index.keys))
		colIDs     = make([]int64, len(mutCtx.pks))
		indexKey   kv.Key
		valueCodec []byte
		err        error
		txn        = ctx.Value(sessionctx.TxnIDKey).(kv.Transaction)
	)

	for _, indexKey := range index.keys {
		var val types.Datum
		colEntry, ok := mutCtx.colmap[indexKey.ColumnMeta.Name]
		if !ok {
			// this time no input value for this index key, just insert a nullval
			val = types.NewDatum(nil)
			val, err = val.ConvertTo(sc, t.colFtMap[indexKey.Id])
			if err != nil {
				logutil.Logger(ctx).Error("datum convert fieldType err ", zap.Error(err))
			}
			continue
		}
		val = row[colEntry.idx]
		values = append(values, val)
	}

	for i, pk := range t.pkeys {
		colIDs[i] = pk.Id
	}

	if !index.meta.Unique {
		keycodec, err := tablecodec.EncodeIndex(sc, t.tableID, index.ID, values, mutCtx.pks)
		if err != nil {
			logutil.Logger(ctx).Error("encode index error", zap.Error(err))
			return err
		}
		indexKey = kv.Key(keycodec)
		valueCodec = []byte{'0'}

	} else {
		keycodec, err := tablecodec.EncodeIndexUnique(sc, t.tableID, index.ID, values)
		if err != nil {
			logutil.Logger(ctx).Error("encode index error", zap.Error(err))
			return err
		}
		indexKey = kv.Key(keycodec)
		valueCodec, err = tablecodec.EncodeRow(sc, mutCtx.pks, colIDs, nil, nil)
		if err != nil {
			logutil.Logger(ctx).Error("encode index row value error", zap.Error(err))
			return err
		}
	}

	if err := txn.Set(indexKey, valueCodec); err != nil {
		return err
	}
	return nil
}

/******** Mysql **********/

func (t *tableCommon) collectColID() []int64 {
	colsID := make([]int64, len(t.Columns))
	for i, col := range t.Columns {
		colsID[i] = col.Id
	}
	return colsID
}

func (t *tableCommon) collectPkID() []int64 {
	pkIDs := make([]int64, len(t.pkeys))
	for i, pk := range t.pkeys {
		pkIDs[i] = pk.Id
	}
	return pkIDs
}

func (t *tableCommon) collectPrimaryKeyDatums(row []types.Datum) []types.Datum {
	pkDatums := make([]types.Datum, len(t.pkeys))
	for i, pkey := range t.pkeys {
		pkDatums[i] = row[t.pkeyMap[pkey.ColumnMeta.Name]]
	}
	return pkDatums
}

func (t *tableCommon) buildIndex(ctx sctx.Context, row, pkDatums []types.Datum, idxInfo *IndexInfo) ([]byte, error) {
	var (
		key []byte
		err error
	)
	indexValueDatums := make([]types.Datum, len(idxInfo.keys))
	tableID := t.Meta().Id
	idxID := idxInfo.meta.Id
	for i, col := range idxInfo.keys {
		indexValueDatums[i] = row[col.Offset]
	}
	if idxInfo.meta.Unique {
		key, err = tablecodec.EncodeIndexUnique(nil, tableID, idxID, indexValueDatums)
	} else {
		key, err = tablecodec.EncodeIndex(nil, tableID, idxID, indexValueDatums, pkDatums)
	}
	return key, err
}

func (t *tableCommon) addIndex(ctx sctx.Context, row, pkDatums []types.Datum, txn kv.Transaction) error {
	pkRow, err := tablecodec.EncodePrimaryKey(nil, pkDatums)
	if err != nil {
		return err
	}

	for _, idx := range t.Indices {
		key, err := t.buildIndex(ctx, row, pkDatums, idx)
		if err != nil {
			return err
		}
		if idx.meta.Unique {
			if err = txn.Set(key, pkRow); err != nil {
				return err
			}
		} else {
			if err = txn.Set(key, []byte{'0'}); err != nil {
				return err
			}
		}
	}

	return nil
}

// AddRecord only support write wide-row in default column family now.
func (t *tableCommon) AddRecord(ctx sctx.Context, row []types.Datum) error {
	txn, err := ctx.GetTxn(true, t.isRaw())
	if err != nil {
		return err
	}
	colsID := t.collectColID()
	pkeyDatums := t.collectPrimaryKeyDatums(row)

	if err = t.addIndex(ctx, row, pkeyDatums, txn); err != nil {
		return err
	}

	rowKey, err := tablecodec.EncodeRecordKeyWide(nil, t.Meta().Id, pkeyDatums, 0)
	if err != nil {
		return err
	}
	rowValue, err := tablecodec.EncodeRow(nil, row, colsID, nil, nil)
	if err != nil {
		return err
	}
	err = txn.Set(rowKey, rowValue)
	if err != nil {
		return err
	}
	return nil
}

func (t *tableCommon) buildCFMapFromCols(cols []*table.Column) map[string][]*table.Column {
	cfMap := make(map[string][]*table.Column)
	for _, col := range cols {
		cfMap[col.Family] = append(cfMap[col.Family], col)
	}
	return cfMap
}

//Insert none default column family.
//High is simple, just extract pk part from datums, iterator the left datums, and set.
//Wide is sophiscated. First read the column record, iterator the qualifiers to decide
//whether to append or update.
/*
func (t *tableCommon) AddRecordWithCF(ctx sctx.Context, values []types.Datum, cols []*table.Column) error {
	pkLen := len(t.pkeys)
	pkDatums := values[:pkLen]
	cfDatums := values[pkLen:]
	//colFamilyName -> list(cols from same cf)
	cfMap := t.buildCFMapFromCols(cols[pkLen:])
	keys, values, err := t.buildKeyValuesWithCF(pkDatums, cfDatums, cfMap)
	return nil
}
*/

//func (t *tableCommon) buildKeyValuesWithCF()
