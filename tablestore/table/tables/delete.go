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
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/pingcap/tidb/util/logutil"
	tspb "github.com/zhihu/zetta-proto/pkg/tablestore"
	"github.com/zhihu/zetta/pkg/model"
	"github.com/zhihu/zetta/pkg/tablecodec"
	"github.com/zhihu/zetta/tablestore/sessionctx"
	"go.uber.org/zap"
)

type deleteOptions struct {
	RowDelete bool
	Columns   []string
	fields    []*columnEntry
	cfColMap  map[string]map[string]int
	cfmap     map[string]*model.ColumnFamilyMeta
	pkfts     []*types.FieldType
}

func (t *tableCommon) Delete(ctx context.Context, keySet *tspb.KeySet, family string, columns []string) error {

	// delOpts, err := t.prepareDeleteOptions(ctx, columns)
	delOpts, err := t.prepareDeleteCFOptions(ctx, family, columns)
	if err != nil {
		return err
	}

	if all := keySet.GetAll(); all {
		return t.dropTableData(ctx, delOpts)
	}

	if len(keySet.GetRanges()) > 0 {
		keyRangeList := makeKeyRangeList(keySet.Ranges)
		if err := t.removeKeyRanges(ctx, keyRangeList, delOpts); err != nil {
			return err
		}
	}

	if len(keySet.Keys) > 0 {
		if err := t.deleteBatchKeys(ctx, keySet.Keys, delOpts); err != nil {
			return err
		}
	}
	return nil
}

func (t *tableCommon) prepareDeleteCFOptions(ctx context.Context, family string, columns []string) (*deleteOptions, error) {
	var (
		fields   = make([]*columnEntry, 0)
		cfmap    = make(map[string]*model.ColumnFamilyMeta)
		cfColMap = make(map[string]map[string]int)
	)
	if family == "" {
		family = DefaultColumnFamily
	}
	cf, ok := t.cfIDMap[family]
	if !ok {
		return nil, status.Errorf(codes.FailedPrecondition, "cf %s not in table %s", family, t.meta.TableName)
	}
	cfmap[family] = cf
	cfColMap[family] = make(map[string]int)

	delOpts := &deleteOptions{
		pkfts:    t.buildPrimaryKeyFieldTypes(),
		cfmap:    cfmap,
		cfColMap: cfColMap,
	}

	if len(columns) == 0 {
		delOpts.RowDelete = true
		return delOpts, nil
	}

	if isFlexible(cf) {
		for _, col := range columns {
			colEntry := &columnEntry{
				name: col,
				t:    &tspb.Type{Code: tspb.TypeCode_TYPE_CODE_UNSPECIFIED},
				ft:   nil,
				cfID: cf.Id,
				cf:   cf.Name,
			}
			fields = append(fields, colEntry)
		}
	}
	delOpts.fields = fields
	return delOpts, nil
}

func (t *tableCommon) prepareDeleteOptions(ctx context.Context, columns []string) (*deleteOptions, error) {
	var (
		fields   = make([]*columnEntry, 0)
		cfmap    = make(map[string]*model.ColumnFamilyMeta)
		cfColMap = make(map[string]map[string]int)
	)

	delOpts := &deleteOptions{
		RowDelete: true,
		pkfts:     t.buildPrimaryKeyFieldTypes(),
	}

	if len(columns) == 0 {
		return delOpts, nil
	}
	for _, col := range columns {
		cfn, coln, err := resolveColumn(col)
		if err != nil {
			continue
		}

		cf, ok := t.cfIDMap[cfn]
		if !ok {
			continue
		}
		if !isFlexible(cf) {
			continue
		}

		if _, ok := cfmap[cfn]; !ok {
			cfmap[cfn] = cf
			cfColMap[cfn] = map[string]int{}
		}
		colEntry := &columnEntry{
			name: coln,
			t:    &tspb.Type{Code: tspb.TypeCode_TYPE_CODE_UNSPECIFIED},
			ft:   nil,
			cfID: cf.Id,
			cf:   cf.Name,
		}
		fields = append(fields, colEntry)
	}
	delOpts.cfColMap = cfColMap
	delOpts.fields = fields
	delOpts.cfmap = cfmap
	return delOpts, nil
}

func (t *tableCommon) removeKeyRanges(ctx context.Context, krl keyRangeList, delOpts *deleteOptions) error {
	var (
		wg sync.WaitGroup
	)
	for _, kr := range krl {
		wg.Add(1)
		go func(kr *keyRange) {
			defer wg.Done()
			for _, cf := range t.cfIDMap {
				startVals, err := t.buildPrimaryKeyValues(kr.start)
				if err != nil {
					logutil.Logger(ctx).Error("build start primary key datums error", zap.Error(err))
					return
				}
				endVals, err := t.buildPrimaryKeyValues(kr.end)
				if err != nil {
					logutil.Logger(ctx).Error("build end primary key datums error", zap.Error(err))
					return
				}

				startKey, err := genRecordPrimaryCFPrefix(t.tableID, startVals, cf.Id)
				if err != nil {
					logutil.Logger(ctx).Error("generate record key error", zap.Error(err))
					return
				}

				endKey, err := genRecordPrimaryCFPrefix(t.tableID, endVals, cf.Id)
				if err != nil {
					logutil.Logger(ctx).Error("generate record key error", zap.Error(err))
					return
				}

				if !kr.startClosed {
					startKey = startKey.Next()
				}
				if kr.endClosed {
					endKey = endKey.Next()
				}
				if err := t.removeRange(ctx, cf, startKey, endKey, delOpts); err != nil {
					return
				}
			}
		}(kr)

	}
	wg.Wait()
	return nil
}

func (t *tableCommon) removeRange(ctx context.Context, cf *model.ColumnFamilyMeta, start, upperBound kv.Key, delOpts *deleteOptions) error {
	iter, err := t.txn.Iter(start, upperBound)
	if err != nil {
		logutil.Logger(ctx).Error("locate key range error", zap.Error(err))
		return err
	}
	defer iter.Close()
	if isLayoutCompact(cf) {
		for iter.Valid() {
			if err := t.removeRowCompact(ctx, cf, iter.Key()); err != nil {
				return err
			}
			iter.Next()
		}
	} else {
		if isFlexible(cf) {
			// for iter.Valid() {
			// 	if err := t.removeFlexibleSparse(ctx, iter.Key()); err != nil {
			// 		return err
			// 	}
			// 	iter.Next()
			// }
		} else {
			var (
				keyPrefix = kv.Key(tablecodec.ExtractEncodedPrimaryKeyCFPrefix(start))
				keyMap    = map[string]struct{}{}
			)

			for iter.Valid() {
				if iter.Key().HasPrefix(keyPrefix) {
					if _, ok := keyMap[string(iter.Key())]; ok {
						continue
					} else {
						keyMap[string(iter.Key())] = struct{}{}
					}
				} else { // new recordKey appears
					keyPrefix = tablecodec.ExtractEncodedPrimaryKeyCFPrefix(iter.Key())
					keyMap[string(iter.Key())] = struct{}{}
				}
				iter.Next()
			}

			for reckey := range keyMap {
				if err := t.removeRowSparse(ctx, kv.Key(reckey), cf, delOpts); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func (t *tableCommon) removeRowCompact(ctx context.Context, cf *model.ColumnFamilyMeta, recordKey kv.Key) error {
	txn := ctx.Value(sessionctx.TxnIDKey).(kv.Transaction)
	if isFlexible(cf) {
		if err := txn.Delete(recordKey); err != nil {
			logutil.Logger(ctx).Error("delete Flexsible recordkey error", zap.Error(err))
			return err
		}
		return nil
	}
	pkfts := t.buildPrimaryKeyFieldTypes()
	_, pKeys, _, _, err := tablecodec.DecodeRecordKey(recordKey, pkfts, time.Local)
	valBody, err := txn.Get(recordKey)
	if err != nil {
		logutil.Logger(ctx).Error("get record key value error", zap.Error(err))
		return err
	}
	row, err := tablecodec.DecodeRowWide(valBody, t.colFtMap, time.Local, nil)
	if err != nil {
		logutil.Logger(ctx).Error("decode rows column value error", zap.Error(err))
		return err
	}
	if err := t.removeIndexRecord(ctx, pKeys, row); err != nil {
		return err
	}
	if err := txn.Delete(recordKey); err != nil {
		logutil.Logger(ctx).Error("delete Flexsible recordkey error", zap.Error(err))
		return err
	}
	return nil
}

func (t *tableCommon) removeRowSparse(ctx context.Context, recordKey kv.Key, cf *model.ColumnFamilyMeta, delOpts *deleteOptions) error {

	_, pkeys, _, _, err := tablecodec.DecodeRecordKey(recordKey, delOpts.pkfts, time.Local)
	if err != nil {
		logutil.Logger(ctx).Error("decode recordkey Sparse error", zap.Error(err))
		return err
	}

	if err := t.removeRecordWithIndex(ctx, pkeys, cf); err != nil {
		logutil.Logger(ctx).Error("remove row Sparse with index error", zap.Error(err))
		return err
	}
	return nil
}

func (t *tableCommon) getPrimaryKeyData(ctx context.Context, lv *tspb.ListValue) ([]types.Datum, error) {
	pKeys, err := t.buildPrimaryKeyValues(lv)
	if err != nil {
		logutil.Logger(ctx).Error("build values error", zap.Error(err))
		return nil, err
	}
	return pKeys, nil
}

func (t *tableCommon) scanRecordKeySparseColumns(ctx context.Context, pKeys []types.Datum, cf *model.ColumnFamilyMeta) (map[string]*colSparseEntry, error) {
	var (
		sc  = &stmtctx.StatementContext{TimeZone: time.Local}
		txn = ctx.Value(sessionctx.TxnIDKey).(kv.Transaction)
	)
	if isLayoutCompact(cf) {
		return nil, fmt.Errorf("cf: %s not encoding Sparse", cf.GetName())
	}
	recKeyPrefix, err := tablecodec.EncodeRecordPrimaryKeyCFPrefix(sc, t.tableID, pKeys, cf.Id)
	if err != nil {
		logutil.Logger(ctx).Error("build record-key with cf error", zap.Error(err))
		return nil, err
	}
	it, err := txn.Iter(recKeyPrefix, kv.Key(recKeyPrefix).PrefixNext())
	if err != nil {
		logutil.Logger(ctx).Error("scan record-key with cf error", zap.Error(err))
		return nil, err
	}
	defer it.Close()
	colDatMap := make(map[string]*colSparseEntry)
	pkfts := t.buildPrimaryKeyFieldTypes()
	for it.Valid() {
		_, _, _, col, err := tablecodec.DecodeRecordKey(it.Key(), pkfts, time.Local)
		if err != nil {
			logutil.Logger(ctx).Error("decode record key Sparse error", zap.Error(err))
			return nil, err
		}
		colID, colFt, err := t.getColumnInfo(string(col), cf)
		if err != nil {
			return nil, err
		}
		colDatum, err := tablecodec.DecodeRowHigh(sc, it.Value(), colFt, time.Local)
		if err != nil {
			return nil, err
		}
		entry := &colSparseEntry{
			id:     colID,
			key:    it.Key(),
			column: string(col),
			data:   colDatum,
		}
		colDatMap[entry.column] = entry
		it.Next()
	}
	return colDatMap, nil
}

func (t *tableCommon) getColumnInfo(column string, cf *model.ColumnFamilyMeta) (int64, *types.FieldType, error) {
	if isFlexible(cf) {
		return -1, nil, nil
	}
	if colIdx, ok := t.colIndex[column]; ok {
		colMeta := t.Columns[colIdx]
		return colMeta.Id, colMeta.FieldType, nil
	}
	return -1, nil, status.Errorf(codes.NotFound, "no such column %s ", column)
}

func (t *tableCommon) removeRecordWithIndex(ctx context.Context, pKeys []types.Datum, cf *model.ColumnFamilyMeta) (err error) {
	var txn = ctx.Value(sessionctx.TxnIDKey).(kv.Transaction)

	if isLayoutCompact(cf) {
		recordKey, err := buildRecordKeyCompact(t.tableID, pKeys, cf)
		if err != nil {
			logutil.Logger(ctx).Error("build record-key Compact error", zap.Error(err))
			return err
		}
		valBody, err := txn.Get(recordKey)
		if err != nil {
			logutil.Logger(ctx).Error("get record-key val error", zap.Error(err))
			return err
		}
		if !isFlexible(cf) {
			row, err := tablecodec.DecodeRowWide(valBody, t.colFtMap, time.Local, nil)
			if err != nil {
				logutil.Logger(ctx).Error("decode rows column value error", zap.Error(err))
				return err
			}
			if err := t.removeIndexRecord(ctx, pKeys, row); err != nil {
				return err
			}
		}
		if err := txn.Delete(recordKey); err != nil {
			logutil.Logger(ctx).Error("delete index key error", zap.Error(err))
			return err
		}
		return nil
	}

	colMap, err := t.scanRecordKeySparseColumns(ctx, pKeys, cf)
	if err != nil {
		return err
	}

	rowMap := map[int64]types.Datum{}
	for _, entry := range colMap {
		rowMap[entry.id] = entry.data
		if err := txn.Delete(entry.key); err != nil {
			logutil.Logger(ctx).Error("delete record key Sparse error", zap.Error(err))
			return err
		}
	}
	if !isFlexible(cf) {
		if err := t.removeIndexRecord(ctx, pKeys, rowMap); err != nil {
			return err
		}
	}
	return nil
}

func (t *tableCommon) removeIndexRecord(ctx context.Context, pkeys []types.Datum, row map[int64]types.Datum) error {
	txn := ctx.Value(sessionctx.TxnIDKey).(kv.Transaction)
	for _, idxInfo := range t.Indices {
		indexKey, err := t.createIndexKeyFromRecord(ctx, idxInfo, pkeys, row)
		if err != nil {
			return err
		}
		if err := txn.Delete(indexKey); err != nil {
			logutil.Logger(ctx).Error("delete index key error", zap.Error(err))
			return err
		}
	}
	return nil
}

func (t *tableCommon) createIndexKeyFromRecord(ctx context.Context, indexInfo *IndexInfo, pkeys []types.Datum, row map[int64]types.Datum) (kv.Key, error) {
	var sc = &stmtctx.StatementContext{TimeZone: time.Local}

	index := indexInfo.meta
	values := t.genIndexColValue(index, row)
	if index.GetUnique() {
		indexKey, err := tablecodec.EncodeIndexUnique(sc, t.tableID, index.Id, values)
		if err != nil {
			logutil.Logger(ctx).Error("encode unique index key error", zap.Error(err))
			return nil, err
		}
		return kv.Key(indexKey), nil
	}
	indexKey, err := tablecodec.EncodeIndex(sc, t.tableID, index.Id, values, pkeys)
	if err != nil {
		logutil.Logger(ctx).Error("encode index key error", zap.Error(err))
		return nil, err
	}

	return kv.Key(indexKey), nil
}

func (t *tableCommon) genIndexColValue(index *model.IndexMeta, row map[int64]types.Datum) []types.Datum {
	var values = make([]types.Datum, 0)
	for _, col := range index.GetDefinedColumns() {
		colID := t.Columns[t.colIndex[col]].Id
		values = append(values, row[colID])
	}
	return values
}

/*----------------------------------------------batch key delete scope -------------------------------------------*/

func (t *tableCommon) deleteBatchKeys(ctx context.Context, lvs []*tspb.ListValue, delOpts *deleteOptions) error {
	for _, lv := range lvs {
		pkeys, err := t.getPrimaryKeyData(ctx, lv)
		if err != nil {
			return err
		}
		if delOpts.RowDelete {
			for _, cf := range delOpts.cfmap {
				if err := t.removeRecordWithIndex(ctx, pkeys, cf); err != nil {
					return err
				}
			}
			return nil
		}
		if err := t.removeFlexibleSparseColumn(ctx, pkeys, delOpts); err != nil {
			return err
		}
	}
	return nil
}

func (t *tableCommon) removeFlexibleSparseColumn(ctx context.Context, pkeys []types.Datum, delOpts *deleteOptions) error {
	var (
		txn = ctx.Value(sessionctx.TxnIDKey).(kv.Transaction)
		sc  = &stmtctx.StatementContext{TimeZone: time.Local}
	)
	for _, cf := range delOpts.cfmap {
		for _, col := range delOpts.fields {
			if col.cf == cf.Name {
				recordKey, err := tablecodec.EncodeRecordKeyHigh(sc, t.tableID, pkeys, cf.Id, []byte(col.name))
				if err != nil {
					logutil.Logger(ctx).Error("encode record key error", zap.Error(err))
					return err
				}
				if err := txn.Delete(recordKey); err != nil {
					logutil.Logger(ctx).Error("delete record key error", zap.Error(err))
					return err
				}
			}
		}
	}
	// _, pkeys, _, _, err := tablecodec.DecodeRecordKey(recordKey, pkfts, time.Local)
	// if err != nil {
	// 	logutil.Logger(ctx).Error("decode recordkey Sparse error", zap.Error(err))
	// 	return err
	// }

	// if err := t.removeRecordWithIndex(ctx, pkeys, cf); err != nil {
	// 	logutil.Logger(ctx).Error("remove row Sparse with index error", zap.Error(err))
	// 	return err
	// }
	// return nil
	return nil
}

func (t *tableCommon) deleteSingleKey(ctx context.Context, recordKey kv.Key) error {
	txn := ctx.Value(sessionctx.TxnIDKey).(kv.Transaction)
	if err := txn.Delete(recordKey); err != nil {
		logutil.Logger(ctx).Error("delete Flexsible recordkey Sparse error", zap.Error(err))
		return err
	}
	return nil
}

// ---------------------------- global table drop option --------------------------------------------

func (t *tableCommon) dropTableData(ctx context.Context, delOpts *deleteOptions) error {
	if delOpts.RowDelete {
		return t.dropSpecificCFRow(ctx, delOpts)
	}
	return t.dropSpecificFlexibleColumn(ctx, delOpts)
}

func (t *tableCommon) dropSpecificFlexibleColumn(ctx context.Context, delOpts *deleteOptions) error {
	var (
		txn         = ctx.Value(sessionctx.TxnIDKey).(kv.Transaction)
		tablePrefix = tablecodec.EncodeTablePrefix(t.tableID)
	)
	iter, err := txn.Iter(tablePrefix, tablePrefix.PrefixNext())
	if err != nil {
		logutil.Logger(ctx).Error("locate key range error", zap.Error(err))
		return err
	}
	defer iter.Close()
	for iter.Valid() {
		_, _, cfID, column, err := tablecodec.DecodeRecordKey(iter.Key(), delOpts.pkfts, time.Local)
		if err != nil {
			logutil.Logger(ctx).Error("decode record key error", zap.Error(err), zap.String("table", t.Meta().TableName))
			return err
		}
		for _, col := range delOpts.fields {
			if col.cfID == cfID && col.name == string(column) {
				if err := txn.Delete(iter.Key()); err != nil {
					logutil.Logger(ctx).Error("remove key error", zap.Error(err), zap.String("key", string(iter.Key())))
					return err
				}
			}
		}
		iter.Next()
	}
	return nil
}

func (t *tableCommon) dropSpecificCFRow(ctx context.Context, delOpts *deleteOptions) error {
	var (
		txn         = ctx.Value(sessionctx.TxnIDKey).(kv.Transaction)
		tablePrefix = tablecodec.EncodeTablePrefix(t.tableID)
	)
	iter, err := txn.Iter(tablePrefix, tablePrefix.PrefixNext())
	if err != nil {
		logutil.Logger(ctx).Error("locate key range error", zap.Error(err))
		return err
	}
	defer iter.Close()
	for iter.Valid() {
		_, _, cfID, _, err := tablecodec.DecodeRecordKey(iter.Key(), delOpts.pkfts, time.Local)
		if err != nil {
			logutil.Logger(ctx).Error("decode record key error", zap.Error(err), zap.String("table", t.Meta().TableName))
			return err
		}
		for _, cfMeta := range delOpts.cfmap {
			if cfMeta.Id == cfID {
				if err := txn.Delete(iter.Key()); err != nil {
					logutil.Logger(ctx).Error("remove key error", zap.Error(err), zap.String("key", string(iter.Key())), zap.String("cf", cfMeta.Name))
					return err
				}
			}
		}
		iter.Next()
	}
	return nil
}

func (t *tableCommon) deleteKeys(ctx context.Context, k, upperBound kv.Key) error {
	txn := ctx.Value(sessionctx.TxnIDKey).(kv.Transaction)
	iter, err := txn.Iter(k, upperBound)
	if err != nil {
		logutil.Logger(ctx).Error("locate key range error", zap.Error(err))
		return err
	}
	defer iter.Close()
	for iter.Valid() {
		//TODO: verify this delete procedure

		if err := txn.Delete(iter.Key()); err != nil {
			logutil.Logger(ctx).Error("remove key error", zap.Error(err), zap.String("key", string(iter.Key())))
			return err
		}
		iter.Next()
	}
	return nil
}
