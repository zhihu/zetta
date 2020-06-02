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
	"github.com/zhihu/zetta/tablestore/sessionctx"

	tspb "github.com/zhihu/zetta-proto/pkg/tablestore"
)

func (t *tableCommon) Insert(ctx context.Context, columns []string, values []*tspb.ListValue) error {
	return status.Errorf(codes.Unimplemented, "insert not implemented yet")

}

func (t *tableCommon) InsertOrUpdate(ctx context.Context, columns []string, values []*tspb.ListValue) error {
	cols, pkeys, cfmap, err := t.prepareColumns(columns)
	if err != nil {
		return err
	}
	rows, err := t.prefetchDataCache(cols, values)
	if err != nil {
		logutil.Logger(ctx).Error("construct datum slice err", zap.Error(err))
		return err
	}
	for _, row := range rows {
		for cf, colmap := range cfmap {
			cfMeta := t.cfIDMap[cf]
			if err := t.insertCFRow(ctx, pkeys, cfMeta, colmap, row); err != nil {
				logutil.Logger(ctx).Error("insert single row error", zap.Error(err))
				return err
			}
		}
	}
	return nil
}

func (t *tableCommon) Update(ctx context.Context, cols []string, values []*tspb.ListValue) error {
	return status.Errorf(codes.Unimplemented, "update not implemented yet")
}

func (t *tableCommon) Replace(ctx context.Context, cols []string, values []*tspb.ListValue) error {
	return status.Errorf(codes.Unimplemented, "replace not implemented yet")
}

func (t *tableCommon) insertCFRow(ctx context.Context, pks []*columnEntry, cf *model.ColumnFamilyMeta, colmap map[string]*columnEntry, row []types.Datum) error {
	var (
		txn        = ctx.Value(sessionctx.TxnIDKey).(kv.Transaction)
		isFlexible = isFlexible(cf)
	)
	keys, vals, err := t.buildKeyValues(pks, cf, colmap, row)
	if err != nil {
		logutil.Logger(ctx).Error("build key values error", zap.Error(err))
		return err
	}
	for i, key := range keys {

		val := vals[i]
		if cf.Name == DefaultColumnFamily {
			oldVal, err := txn.Get(key)
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
		if err := t.buildIndexForRow(ctx, pks, colmap, row); err != nil {
			logutil.Logger(ctx).Error("build record row data error while insertRow", zap.Error(err))
		}
	}
	return nil
}

func (t *tableCommon) buildKeyValues(pks []*columnEntry, cf *model.ColumnFamilyMeta, colmap map[string]*columnEntry, row []types.Datum) ([]kv.Key, [][]byte, error) {

	if isLayoutCompact(cf) {
		key, val, err := t.buildKeyRowCompact(t.tableID, pks, cf, colmap, row)
		if err != nil {
			return nil, nil, err
		}
		return []kv.Key{key}, [][]byte{val}, nil
	}
	return t.buildKeyRowHigh(t.tableID, pks, cf, colmap, row)
}

func (t *tableCommon) makeColumnEntry(column string) *columnEntry {
	colMeta := t.Columns[t.colIndex[column]]
	colEntry := &columnEntry{
		id:   colMeta.Id,
		name: colMeta.Name,
		t:    colMeta.ColumnType,
		cfID: t.cfIDMap[colMeta.Family].Id,
		cf:   colMeta.Family,
		idx:  -1,
	}
	return colEntry
}

func (t *tableCommon) buildKeyRowHigh(tableID int64, pks []*columnEntry, cf *model.ColumnFamilyMeta, colmap map[string]*columnEntry, row []types.Datum) ([]kv.Key, [][]byte, error) {
	var (
		pkRow = make([]types.Datum, len(pks))
		rkeys = make([]kv.Key, 0)
		vals  = make([][]byte, 0)
		sc    = &stmtctx.StatementContext{TimeZone: time.Local}
	)

	for i, pk := range pks {
		pkRow[i] = row[pk.idx]
	}
	if !isFlexible(cf) {
		columns := t.getCFColumns(cf)
		for _, column := range columns {
			if _, ok := colmap[column]; ok {
				continue
			}
			colEntry := t.makeColumnEntry(column)
			colmap[column] = colEntry
		}
	}

	for col, entry := range colmap {
		recKey, err := tablecodec.EncodeRecordKeyHigh(sc, tableID, pkRow, cf.Id, []byte(col))
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

func (t *tableCommon) buildKeyRowCompact(tableID int64, pks []*columnEntry, cf *model.ColumnFamilyMeta, colmap map[string]*columnEntry, row []types.Datum) (kv.Key, []byte, error) {
	var (
		pkRow   = make([]types.Datum, len(pks))
		sc      = &stmtctx.StatementContext{TimeZone: time.Local}
		valBody []byte
		i       = 0
	)

	for i, pk := range pks {
		pkRow[i] = row[pk.idx]
	}
	recKey, err := tablecodec.EncodeRecordKeyWide(sc, tableID, pkRow, cf.Id)
	if err != nil {
		logutil.Logger(context.TODO()).Error("encode wide error", zap.Error(err))
		return nil, nil, err
	}
	if !isFlexible(cf) {
		columns := t.getCFColumns(cf)
		for _, column := range columns {
			if _, ok := colmap[column]; ok {
				continue
			}
			colEntry := t.makeColumnEntry(column)
			colmap[column] = colEntry
		}

		rowData := make([]types.Datum, len(colmap))
		colIDs := make([]int64, len(colmap))
		for _, col := range colmap {
			colIDs[i] = col.id
			if col.idx == -1 {
				rowData[i] = types.NewDatum(nil)
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
		for _, col := range colmap {
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

func (t *tableCommon) buildIndexForRow(ctx context.Context, pks []*columnEntry, colMap map[string]*columnEntry, row []types.Datum) error {
	for _, index := range t.Indices {
		if err := t.addIndexRow(ctx, index, pks, colMap, row); err != nil {
			return err
		}
	}
	return nil
}

func (t *tableCommon) addIndexRow(ctx context.Context, index *IndexInfo, pks []*columnEntry, colMap map[string]*columnEntry, row []types.Datum) error {
	var (
		sc         = &stmtctx.StatementContext{TimeZone: time.Local}
		values     = make([]types.Datum, len(index.keys))
		pkRow      = make([]types.Datum, len(pks))
		colIDs     = make([]int64, len(pks))
		indexKey   kv.Key
		valueCodec []byte
		err        error
		txn        = ctx.Value(sessionctx.TxnIDKey).(kv.Transaction)
	)

	for _, indexKey := range index.keys {
		var val types.Datum
		colEntry, ok := colMap[indexKey.Name]
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

	for i, pk := range pks {
		pkRow[i] = row[pk.idx]
		colIDs[i] = pk.id
	}

	if !index.meta.Unique {
		keycodec, err := tablecodec.EncodeIndex(sc, t.tableID, index.ID, values, pkRow)
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
		valueCodec, err = tablecodec.EncodeRow(sc, pkRow, colIDs, nil, nil)
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
