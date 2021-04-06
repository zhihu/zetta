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
	"time"

	structpb "github.com/gogo/protobuf/types"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/logutil"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	tspb "github.com/zhihu/zetta-proto/pkg/tablestore"
	"github.com/zhihu/zetta/pkg/metrics"
	"github.com/zhihu/zetta/pkg/model"
	"github.com/zhihu/zetta/pkg/tablecodec"
	"github.com/zhihu/zetta/tablestore/mysql/sctx"
	"github.com/zhihu/zetta/tablestore/sessionctx"
	"github.com/zhihu/zetta/tablestore/table"
)

var (
	fetchRowsCounterOK   = metrics.FetchRowsCounter.WithLabelValues(metrics.LblOK)
	fetchRowsCounterErr  = metrics.FetchRowsCounter.WithLabelValues(metrics.LblError)
	fetchRowsDurationOK  = metrics.FetchRowsDuration.WithLabelValues(metrics.LblOK)
	fetchRowsDurationErr = metrics.FetchRowsDuration.WithLabelValues(metrics.LblError)
)

//Result map: Key is the index in pkeys, value is the index in cols.
//To decode primary key.
func (t *tableCommon) getPKcolIndexes(cols []*table.Column) map[int]int {
	res := make(map[int]int)
	for i, col := range cols {
		for j, pkcol := range t.pkeys {
			if pkcol.ColumnMeta.Name == col.ColumnMeta.ColumnMeta.Name {
				res[j] = i
			}
		}
	}
	return res
}

//Default column family.
//To decode the none-primary-key col in default family.
func (t *tableCommon) getTypeMap(cols []*table.Column) (map[int64]*types.FieldType, map[int64]int) {
	resType := make(map[int64]*types.FieldType)
	resOffset := make(map[int64]int)
	for i, col := range cols {
		if _, ok := t.pkeyMap[col.ColumnMeta.ColumnMeta.Name]; !ok &&
			(col.Family == "default" || col.Family == "" || col.Family == t.Meta().TableName) {
			resType[col.Id] = &col.FieldType
			resOffset[col.Id] = i
		}
	}
	return resType, resOffset
}

//To decode none-default column family col.
func (t *tableCommon) getColumnFamilyIndex(cols []*table.Column) (int64, int) {
	for i, col := range cols {
		if col.ColumnMeta.ColumnMeta.Name == "_qualifier" {
			return t.cfIDMap[col.Family].Id, i
		}
	}
	return -1, -1
}

func (t *tableCommon) BatchGet(ctx sctx.Context, idxVals [][]types.Datum, cols []*table.Column,
	idxID int64, primary bool, fn table.RecordIterFunc) error {
	var (
		resMap map[string][]byte
		err    error
	)
	txn, err := ctx.GetTxn(true, t.isRaw())
	if err != nil {
		return err
	}
	tableID := t.Meta().Id
	cctx := context.Background()
	keys := make([]kv.Key, 0, len(idxVals))
	fetchRecords := func() error {
		for key, value := range resMap {
			if value == nil {
				continue
			}
			resDatums, _, err := t.handleKeyValue([]byte(key), value, cols)
			if err != nil {
				return err
			}
			_, err = fn(resDatums, cols)
			if err != nil {
				return err
			}
		}
		return nil
	}
	if primary {
		for _, idxVal := range idxVals {
			rowKey, err := tablecodec.EncodeRecordKeyWide(nil, tableID, idxVal, 0)
			if err != nil {
				return err
			}
			keys = append(keys, kv.Key(rowKey))
		}
		// resMap: map[string][]byte
		resMap, err = txn.BatchGet(cctx, keys)
		if err != nil {
			return err
		}
		if err = fetchRecords(); err != nil {
			return err
		}
	} else {
		for _, idxVal := range idxVals {
			indexKey, err := tablecodec.EncodeIndexUnique(nil, tableID, idxID, idxVal)
			if err != nil {
				return err
			}
			keys = append(keys, kv.Key(indexKey))
		}
		resMap, err = txn.BatchGet(cctx, keys)
		if err != nil {
			return err
		}
		recordKeys := make([]kv.Key, 0, len(idxVals))
		for _, value := range resMap {
			recordKey := tablecodec.EncodeRecordKeyWideWithEncodedPK(tableID, value, 0)
			recordKeys = append(recordKeys, recordKey)
		}
		resMap, err = txn.BatchGet(cctx, recordKeys)
		if err != nil {
			return err
		}
		if err = fetchRecords(); err != nil {
			return err
		}
	}
	return nil
}

// Scan scan the table by index or primary.
// We do not provide a end bound here, use limit.
func (t *tableCommon) Scan(ctx sctx.Context, idxVals []types.Datum, idxMeta *model.IndexMeta, isPrimay bool, lower, upper []byte,
	cols []*table.Column, fn table.RecordIterFunc, limit int64) error {
	var (
		err      error
		startKey kv.Key
	)
	if isPrimay {
		/*
			startKey, err = tablecodec.EncodeRecordPrimaryKey(nil, t.Meta().Id, idxVals)
			if err != nil {
				return err
			}
			if !included {
				startKey = startKey.PrefixNext()
			}
		*/
		startKey = lower
		reverse := lower == nil
		if lower == nil && upper == nil {
			startKey = tablecodec.GenRecordKeyPrefix(t.Meta().Id)
		}
		if upper == nil {
			upper = tablecodec.GenRecordKeyPrefix(t.Meta().Id).PrefixNext()
		}
		err = t.IterRecords(ctx, startKey, upper, cols, reverse, fn)
		return err
	}
	return t.IndexScan(ctx, idxMeta, lower, upper, cols, fn, limit)
}

// check if the needed cols are all in idx and primary.
func (t *tableCommon) indexCover(idxMeta *model.IndexMeta, cols []*table.Column) bool {
	for _, col := range cols {
		for _, defCol := range idxMeta.DefinedColumns {
			if col.Name != defCol {
				return false
			}
		}
	}
	return true
}

//func (t *tableCommon) IndexScan(ctx sctx.Context, idxVals []types.Datum, idxMeta *model.IndexMeta, reverse bool,
//cols []*table.Column, fn table.RecordIterFunc, limit int64) error {
func (t *tableCommon) IndexScan(ctx sctx.Context, idxMeta *model.IndexMeta, lower, upper []byte,
	cols []*table.Column, fn table.RecordIterFunc, limit int64) error {
	var (
		err      error
		startKey kv.Key
		iter     kv.Iterator
		datums   []types.Datum
	)
	txn, err := ctx.GetTxn(true, t.isRaw())
	if err != nil {
		return err
	}

	idxID := idxMeta.Id
	tbID := t.Meta().Id
	indexCovered := t.indexCover(idxMeta, cols)
	idxPrefix := tablecodec.EncodeTableIndexPrefix(tbID, idxID)
	startKey = lower
	reverse := (lower == nil && upper != nil)

	if reverse {
		iter, err = txn.IterReverse(startKey)
	} else {
		if upper == nil {
			upper = idxPrefix.PrefixNext()
		}
		iter, err = txn.Iter(startKey, upper)
	}
	if err != nil {
		return err
	}
	defer iter.Close()

	if !iter.Valid() {
		return nil
	}

	//rowkeys for double reading
	doubleBatchGetKeys := make([]kv.Key, 0)
	//keep index order for double reading.
	resultCardinal := make(map[string]int)
	cardinal := 0

	for iter.Valid() && iter.Key().HasPrefix(idxPrefix) {
		if indexCovered {
			datums, err = t.decodeKeyValue(iter.Key(), iter.Value(), idxMeta, cols)
			if err != nil {
				return err
			}
			more, err := fn(datums, cols)
			if !more || err != nil {
				return err
			}
		} else {
			if limit != 0 && cardinal >= int(limit) {
				break
			}
			var rowKey []byte
			if idxMeta.Unique {
				rowKey = tablecodec.EncodeRecordKeyWideWithEncodedPK(tbID, iter.Value(), 0)
			} else {
				idxColFieldTypes := t.getIdxColFieldTypes(idxMeta)
				_, _, _, pkDatums, err := tablecodec.DecodeIndexKey(iter.Key(), idxColFieldTypes, t.pkColTypes, time.Local)
				if err != nil {
					return err
				}

				rowKey, err = tablecodec.EncodeRecordKeyWide(nil, tbID, pkDatums, 0)
				if err != nil {
					return err
				}
			}
			resultCardinal[string(rowKey)] = cardinal
			doubleBatchGetKeys = append(doubleBatchGetKeys, rowKey)
			cardinal++
		}
		if err = iter.Next(); err != nil {
			return err
		}
	}

	//No need for double reading.
	if indexCovered {
		return nil
	}

	//BatchGet keys, the results are not in order.
	recordsMap, err := txn.BatchGet(context.Background(), doubleBatchGetKeys)
	if err != nil {
		return err
	}

	resDatums := make([][]types.Datum, cardinal)
	for key, record := range recordsMap {
		datums, err := t.decodeKeyValue([]byte(key), record, nil, cols)
		if err != nil {
			return err
		}
		resDatums[resultCardinal[string(key)]] = datums
	}
	for _, dt := range resDatums {
		_, err := fn(dt, cols)
		if err != nil {
			return err
		}
	}
	return nil
}

func (t *tableCommon) getIdxColFieldTypes(idxMeta *model.IndexMeta) []*types.FieldType {
	res := make([]*types.FieldType, len(idxMeta.DefinedColumns))
	for i, colName := range idxMeta.DefinedColumns {
		colMeta := t.Meta().FindColumnByName(colName)
		res[i] = &colMeta.FieldType
	}
	return res
}

//1. idxMeta == nil, we decode the record key and value.
//2. idxMeta != nil
/////a) not uniq: we decode the record key only, if the cols are all in index and primary.
/////b) uniq: we decode the index cols from key, and pk cols from value.
func (t *tableCommon) decodeKeyValue(key, value []byte, idxMeta *model.IndexMeta, cols []*table.Column) ([]types.Datum, error) {
	var (
		res []types.Datum
		err error
	)

	if idxMeta == nil {
		res, _, err = t.handleKeyValue(key, value, cols)
		return res, err
	}

	idxColFieldTypes := t.getIdxColFieldTypes(idxMeta)
	resDatums := make([]types.Datum, len(cols))
	idxDatums := make([]types.Datum, len(cols))
	noPkColTypeMap, noPkColOffsetMap := t.getTypeMap(cols)

	if idxMeta.Unique {
		_, _, idxDatums, err = tablecodec.DecodeUniqIndexKey(key, idxColFieldTypes, time.Local)
		if err != nil {
			return resDatums, err
		}
	} else {
		_, _, idxDatums, _, err = tablecodec.DecodeIndexKey(key, idxColFieldTypes, t.pkColTypes, time.Local)
		if err != nil {
			return resDatums, err
		}
	}
	if len(noPkColTypeMap) != 0 {
		for i, datum := range idxDatums {
			resDatums[noPkColOffsetMap[int64(i)]] = datum
		}
	}
	return resDatums, nil
}

//FetchRecordsByIndex gets records by index. When uniq is true, we do a PointGet or BatchGet on tikv, based on
//number of keys. When uniq is false, we do a scan on tikv.
func (t *tableCommon) FetchRecordsByIndex(ctx sctx.Context, idxVals []types.Datum, cols []*table.Column,
	idxID int64, uniq bool, fn table.RecordIterFunc) error {
	txn, err := ctx.GetTxn(true, t.isRaw())
	if err != nil {
		return err
	}
	tableID := t.Meta().Id
	//TODO: context as a param.
	cctx := context.Background()
	if uniq {
		indexKey, err := tablecodec.EncodeIndexUnique(nil, tableID, idxID, idxVals)
		if err != nil {
			return err
		}
		pkeyRow, err := txn.Get(cctx, indexKey)
		if err != nil {
			return err
		}
		//pkeyRow is not a exactly row encoding format, it is a primary key record format.
		//Can be used to get related row directly.
		recordKey := tablecodec.EncodeRecordKeyWideWithEncodedPK(tableID, pkeyRow, 0)
		recordVal, err := txn.Get(cctx, recordKey)
		if err != nil {
			return err
		}
		resDatums, _, err := t.handleKeyValue(recordKey, recordVal, cols)
		if err != nil {
			return err
		}
		more, err := fn(resDatums, cols)
		if !more || err != nil {
			return err
		}
	} else {
		indexPrefix, err := tablecodec.EncodeIndexPrefix(nil, tableID, idxID, idxVals)
		if err != nil {
			return err
		}
		it, err := txn.Iter(indexPrefix, kv.Key(indexPrefix).PrefixNext())
		if err != nil {
			return err
		}
		recordKeys := make([]kv.Key, 0)
		for it.Valid() && it.Key().HasPrefix(indexPrefix) {
			indexKey := it.Key()
			encodedPK := indexKey[len(indexPrefix):]
			recordKey := tablecodec.EncodeRecordKeyWideWithEncodedPK(tableID, encodedPK, 0)
			recordKeys = append(recordKeys, recordKey)
			if err = it.Next(); err != nil {
				return err
			}
		}
		valMaps, err := txn.BatchGet(cctx, recordKeys)
		if err != nil {
			return err
		}
		for k, v := range valMaps {
			resDatums, skip, err := t.handleKeyValue([]byte(k), v, cols)
			if skip {
				continue
			}
			more, err := fn(resDatums, cols)
			if !more || err != nil {
				return err
			}
		}
	}
	return nil
}

func (t *tableCommon) handleKeyValue(key, value []byte, cols []*table.Column) ([]types.Datum, bool, error) {
	var skip bool
	pkColOffsets := t.getPKcolIndexes(cols)
	resDatums := make([]types.Datum, len(cols))
	noPkColTypeMap, noPkColOffsetMap := t.getTypeMap(cols)
	colFamilyID, qualifierOffsets := t.getColumnFamilyIndex(cols)

	_, pkDatums, cfID, qualifier, err := tablecodec.DecodeRecordKey(key, t.pkColTypes, time.Local)
	if err != nil {
		return resDatums, skip, err
	}

	if colFamilyID != -1 && colFamilyID != cfID {
		skip = true
		return resDatums, skip, err
	}

	if colFamilyID == cfID {
		resDatums[qualifierOffsets] = types.NewBytesDatum(qualifier)
		// qualifier value is always the last col.
		valueOffset := len(cols) - 1
		value, err := tablecodec.DecodeRowHigh(nil, value, &cols[valueOffset].FieldType, time.Local)
		if err != nil {
			return resDatums, skip, err
		}
		resDatums[valueOffset] = value
	}

	if len(pkColOffsets) != 0 {
		for i, j := range pkColOffsets {
			resDatums[j] = pkDatums[i]
		}
	}

	if len(noPkColTypeMap) != 0 {
		rowDatums, err := tablecodec.DecodeRowWide(value, noPkColTypeMap, time.Local, nil)
		if err != nil {
			return resDatums, skip, err
		}
		for i, datum := range rowDatums {
			resDatums[noPkColOffsetMap[i]] = datum
		}
	}
	return resDatums, skip, err
}

//Only support directly scan and primary key index scan.
func (t *tableCommon) IterRecords(ctx sctx.Context, startKey, endKey kv.Key, cols []*table.Column, reverse bool, fn table.RecordIterFunc) error {
	txn, err := ctx.GetTxn(true, t.isRaw())
	if err != nil {
		return err
	}
	var it kv.Iterator
	recordKeyPrefix := tablecodec.GenRecordKeyPrefix(t.Meta().Id)
	if endKey == nil {
		endKey = recordKeyPrefix.PrefixNext()
	}
	if reverse {
		it, err = txn.IterReverse(startKey)
	} else {
		it, err = txn.Iter(startKey, endKey)
	}
	if err != nil {
		return err
	}
	defer it.Close()

	if !it.Valid() {
		return nil
	}
	for it.Valid() && it.Key().HasPrefix(recordKeyPrefix) {
		resDatums, skip, err := t.handleKeyValue(it.Key(), it.Value(), cols)
		if !skip {
			more, err := fn(resDatums, cols)
			if !more || err != nil {
				return err
			}
		}
		if err = it.Next(); err != nil {
			return err
		}
	}
	return nil
}

func (t *tableCommon) searchIndex(index string) *model.IndexMeta {
	if ii, ok := t.Indices[index]; ok {
		return ii.meta
	}
	return nil
}

func (t *tableCommon) ReadStore(ctx context.Context, rr *tspb.ReadRequest) (*resultIter, error) {

	cols, cfColMap, _, err := t.prepareFields(ctx, rr.Columns)
	if err != nil {
		return nil, err
	}
	colMetas := makeFields(cols)
	ri := NewResultIter(rr.Limit)
	ri.columns = colMetas

	go func() {
		defer func() {
			ri.Close()
		}()
		keySet := rr.KeySet
		if keySet.GetAll() {
			if err := t.allRecord(ctx, cols, cfColMap, ri); err != nil {
				ri.lastErr = err
				logutil.Logger(ctx).Error("scan all rows err", zap.Error(err))
				return
			}
			return
		}
		if len(keySet.GetKeys()) > 0 {
			startTS := time.Now()
			err := t.fetchRows(ctx, keySet.Keys, cols, cfColMap, ri)
			durFetchRows := time.Since(startTS)
			if err != nil {
				ri.lastErr = err
				fetchRowsCounterErr.Inc()
				fetchRowsDurationErr.Observe(durFetchRows.Seconds())
				logutil.Logger(ctx).Error("fetch rows err", zap.Error(err))
				return
			}
			fetchRowsDurationOK.Observe(durFetchRows.Seconds())
			fetchRowsCounterOK.Inc()
		}
		if len(keySet.GetRanges()) > 0 {
			krl := makeKeyRangeList(keySet.GetRanges())
			if err := t.readRangeRows(ctx, krl, cols, cfColMap, ri); err != nil {
				logutil.Logger(ctx).Error("fetch rows for ranges err", zap.Error(err))
				return
			}
		}
		logutil.Logger(ctx).Debug("finish readRecord")

	}()
	return ri, nil
}

func (t *tableCommon) allRecord(ctx context.Context, cols []*columnEntry, cfmap map[string]map[string]int, ri *resultIter) error {
	var (
		txn = ctx.Value(sessionctx.TxnIDKey).(kv.Transaction)
		sc  = &stmtctx.StatementContext{TimeZone: time.Local}
		// fieldsMap = map[string]map[string]*tspb.Cell{}
		pkfts = t.buildPrimaryKeyFieldTypes()
	)

	tablePrefix := tablecodec.EncodeTablePrefix(t.tableID)
	iter, err := txn.Iter(tablePrefix, tablePrefix.PrefixNext())
	if err != nil {
		logutil.Logger(context.Background()).Error("get table prefix iterator error", zap.Error(err))
		return err
	}
	var (
		rowKeyPrefix = tablePrefix.Next()
		rowCells     *tspb.SliceCell
	)
	for iter.Valid() {
		_, pkeys, cfID, column, err := tablecodec.DecodeRecordKey(iter.Key(), pkfts, time.Local)
		if err != nil {
			logutil.Logger(context.Background()).Error("decode iterator record key error", zap.Error(err))
			return err
		}
		pkeyPrefix, err := tablecodec.EncodeRecordPrimaryKey(sc, t.tableID, pkeys)
		if err != nil {
			logutil.Logger(context.Background()).Error("encode primary key prefix error", zap.Error(err))
			return err
		}
		if !kv.Key(pkeyPrefix).HasPrefix(rowKeyPrefix) {
			rowKeyPrefix = pkeyPrefix
			pkeyValues, err := t.pKeyValuesFromDatum(pkeys)
			if err != nil {
				logutil.Logger(context.Background()).Error("decode primary key values error", zap.Error(err))
				return err
			}
			if rowCells != nil {
				ri.sendData(rowCells)
			}
			rowCells = &tspb.SliceCell{
				PrimaryKeys: pkeyValues,
				Cells:       []*tspb.Cell{},
			}
		}
		cf := t.getCF(cfID)
		if cf == nil {
			iter.Next()
			continue
		}
		colmap := cfmap[cf.Name]
		collect := func() error {
			if !isFlexible(cf) { // fixed column family
				colDatums, err := tablecodec.DecodeRowWide(iter.Value(), t.colFtMap, time.Local, nil)
				if err != nil {
					logutil.Logger(context.Background()).Error("decode val wide error", zap.Error(err))
					return err
				}

				if len(colmap) == 0 { // read all columns
					for _, col := range t.Columns {
						datum, ok := colDatums[col.Id]
						if !ok {
							cell := &tspb.Cell{
								Family: cf.Name,
								Column: col.ColumnMeta.Name,
								Type:   col.ColumnType,
								Value:  &tspb.Value{Kind: &tspb.Value_NullValue{}},
							}
							rowCells.Cells = append(rowCells.Cells, cell)
							continue
						}
						pv, err := protoValueFromDatum(datum, col.ColumnType)
						if err != nil {
							logutil.Logger(ctx).Error("get proto value from datum error", zap.Error(err))
							return err
						}
						cell := &tspb.Cell{
							Family: cf.Name,
							Column: col.ColumnMeta.Name,
							Type:   col.ColumnType,
							Value:  pv,
						}
						rowCells.Cells = append(rowCells.Cells, cell)
					}
					return nil
				}

				for _, idx := range colmap {
					colEntry := cols[idx]
					datum, ok := colDatums[colEntry.id]
					if !ok {
						cell := &tspb.Cell{
							Family: cf.Name,
							Column: colEntry.name,
							Type:   colEntry.t,
							Value:  &tspb.Value{Kind: &tspb.Value_NullValue{}},
						}
						rowCells.Cells = append(rowCells.Cells, cell)
						continue
					}
					pv, err := protoValueFromDatum(datum, colEntry.t)
					if err != nil {
						logutil.Logger(ctx).Error("get proto value from datum error", zap.Error(err))
						return err
					}
					cell := &tspb.Cell{
						Family: cf.Name,
						Column: colEntry.name,
						Type:   colEntry.t,
						Value:  pv,
					}
					rowCells.Cells = append(rowCells.Cells, cell)
				}

			} else {
				_, ok := t.filterFlexibleColumnEntry(cols, cfmap[cf.Name], cf, string(column))
				if !ok {
					return nil
				}
				// assume flexible cf should be sparse encoding
				colDatum, err := tablecodec.DecodeRowHigh(sc, iter.Value(), nil, time.Local)
				if err != nil {
					logutil.Logger(context.Background()).Error("decode val wide error", zap.Error(err))
					return err
				}
				pv, _, err := flexibleProtoValueFromDatum(colDatum)
				if err != nil {
					logutil.Logger(ctx).Error("gen proto value error", zap.Error(err))
					return err
				}
				cell := &tspb.Cell{
					Family: cf.Name,
					Column: string(column),
					Type:   &tspb.Type{Code: tspb.TypeCode_TYPE_CODE_UNSPECIFIED},
					Value:  pv,
				}
				rowCells.Cells = append(rowCells.Cells, cell)
			}
			return nil
		}
		if err := collect(); err != nil {
			return err
		}
		iter.Next()
	}
	if rowCells != nil {
		ri.sendData(rowCells)
	}

	return nil
}

func (t *tableCommon) filterFlexibleColumnEntry(cols []*columnEntry, colmap map[string]int, cf *model.ColumnFamilyMeta, columns ...string) ([]*columnEntry, bool) {
	rCols := []*columnEntry{}
	flag := false
	if len(colmap) == 0 {
		if isFlexible(cf) {
			return nil, true
		}
		return cols, true
	}
	for _, column := range columns {
		if idx, ok := colmap[column]; ok {
			rCols = append(rCols, cols[idx])
			flag = true
		}
	}
	return rCols, flag
}

func (t *tableCommon) readFieldsCompact2(ctx context.Context, pkeysList []kv.Key, cols []*columnEntry, colmap map[string]int, cf *model.ColumnFamilyMeta) (map[string]map[string]*tspb.Cell, error) {
	var (
		txn     = ctx.Value(sessionctx.TxnIDKey).(kv.Transaction)
		recKeys = make([]kv.Key, len(pkeysList))
		sc      = &stmtctx.StatementContext{TimeZone: time.Local}
		// fieldsMap = PrimaryKeyPrefix: {column : cell_value}
		fieldsMap = map[string]map[string]*tspb.Cell{}
	)

	for i, keyPrefix := range pkeysList {
		rkey := tablecodec.EncodePkCF(keyPrefix, cf.Id)
		fieldsMap[string(keyPrefix)] = map[string]*tspb.Cell{}
		recKeys[i] = rkey
	}

	valsMap, err := txn.BatchGet(ctx, recKeys)
	if err != nil {
		logutil.Logger(ctx).Error("batch get row value error", zap.Error(err))
		return nil, err
	}

	for keyPrefix := range fieldsMap {
		fields := fieldsMap[keyPrefix]
		rkey := tablecodec.EncodePkCF([]byte(keyPrefix), cf.Id)
		rowBody, ok := valsMap[string(rkey)]
		if !ok {
			fieldsMap[string(rkey)] = nil
			continue
		}
		if isFlexible(cf) {
			colDatums, _, err := tablecodec.DecodeRowFlexible(sc, rowBody)
			if err != nil {
				logutil.Logger(ctx).Error("decode val flexible error", zap.Error(err))
				return nil, err
			}
			for col, datum := range colDatums {
				pv, _, err := flexibleProtoValueFromDatum(datum)
				if err != nil {
					logutil.Logger(ctx).Error("get flexsible proto value from datum error", zap.Error(err))
					return nil, err
				}
				if len(colmap) == 0 {
					fields[col] = &tspb.Cell{
						Family: cf.Name,
						Column: col,
						Type:   &tspb.Type{Code: tspb.TypeCode_TYPE_CODE_UNSPECIFIED},
						Value:  pv,
					}
					continue
				}
				idx, ok := colmap[col]
				if !ok {
					continue
				}
				colEntry := cols[idx]
				fields[col] = &tspb.Cell{
					Column: colEntry.ColumnName(),
					Family: cf.Name,
					Type:   &tspb.Type{Code: tspb.TypeCode_TYPE_CODE_UNSPECIFIED},
					Value:  pv,
				}
			}
		} else {
			colDatums, err := tablecodec.DecodeRowWide(rowBody, t.colFtMap, time.Local, nil)
			if err != nil {
				logutil.Logger(ctx).Error("decode val wide error", zap.Error(err))
				return nil, err
			}
			if len(colmap) == 0 {
				for _, col := range t.Columns {
					datum, ok := colDatums[col.Id]
					if !ok {
						fields[col.ColumnMeta.Name] = &tspb.Cell{
							Family: cf.Name,
							Column: col.ColumnMeta.Name,
							Type:   col.ColumnType,
							Value:  &tspb.Value{Kind: &tspb.Value_NullValue{}},
						}
						continue
					}
					pv, err := protoValueFromDatum(datum, col.ColumnType)
					if err != nil {
						logutil.Logger(ctx).Error("get proto value from datum error", zap.Error(err))
						return nil, err
					}
					fields[col.ColumnMeta.Name] = &tspb.Cell{
						Family: cf.Name,
						Column: col.ColumnMeta.Name,
						Type:   col.ColumnType,
						Value:  pv,
					}
				}
				continue
			}
			for col, idx := range colmap {

				colEntry := cols[idx]
				datum, ok := colDatums[colEntry.id]
				if !ok {
					fields[col] = &tspb.Cell{
						Family: cf.Name,
						Column: colEntry.ColumnName(),
						Type:   colEntry.t,
						Value:  &tspb.Value{Kind: &tspb.Value_NullValue{}},
					}
					continue
				}
				pv, err := protoValueFromDatum(datum, colEntry.t)
				if err != nil {
					logutil.Logger(ctx).Error("get proto value from datum error", zap.Error(err))
					return nil, err
				}
				fields[col] = &tspb.Cell{
					Family: cf.Name,
					Column: colEntry.ColumnName(),
					Type:   colEntry.t,
					Value:  pv,
				}
			}
		}
	}
	return fieldsMap, nil
}

func (t *tableCommon) readFieldsSparse2(ctx context.Context, pkeysList []kv.Key, cols []*columnEntry, colmap map[string]int, cf *model.ColumnFamilyMeta) (map[string]map[string]*tspb.Cell, error) {
	var fieldsMap = map[string]map[string]*tspb.Cell{}

	//TODO: Controll limit
	for _, pkeys := range pkeysList {
		colCells, err := t.scanSparse(ctx, pkeys, cf)
		if err != nil {
			return nil, err
		}
		if len(colmap) == 0 {
			fieldsMap[string(pkeys)] = colCells
			continue
		}
		cellMap := map[string]*tspb.Cell{}

		for col, cell := range colCells {
			if _, ok := colmap[col]; ok {
				cellMap[col] = cell
			}
		}
		fieldsMap[string(pkeys)] = cellMap
	}
	return fieldsMap, nil
}

func (t *tableCommon) fetchRows(ctx context.Context, lvs []*tspb.ListValue, cols []*columnEntry, cfmap map[string]map[string]int, ri *resultIter) error {
	var (
		pkeyList = make([]kv.Key, len(lvs))
		sc       = &stmtctx.StatementContext{TimeZone: time.Local}
	)

	for i, lv := range lvs {
		pKeys, err := t.getPrimaryKeyData(ctx, lv)
		if err != nil {
			return err
		}
		keyPrefix, err := tablecodec.EncodeRecordPrimaryKey(sc, t.tableID, pKeys)
		if err != nil {
			return err
		}
		pkeyList[i] = keyPrefix
	}
	// cf_fields { cf : pkey_encoded: column : cell }
	cfFields := map[string]map[string]map[string]*tspb.Cell{}
	for cfn, colmap := range cfmap {
		var (
			cf        = t.cfIDMap[cfn]
			fieldsMap map[string]map[string]*tspb.Cell
			err       error
		)

		if isLayoutCompact(cf) {
			fieldsMap, err = t.readFieldsCompact2(ctx, pkeyList, cols, colmap, cf)
			if err != nil {
				return err
			}
		} else {
			fieldsMap, err = t.readFieldsSparse2(ctx, pkeyList, cols, colmap, cf)
			if err != nil {
				return err
			}
		}

		cfFields[cf.Name] = fieldsMap
	}

	for i, pkeys := range pkeyList {
		rowCells := &tspb.SliceCell{
			PrimaryKeys: lvs[i].Values,
			Cells:       []*tspb.Cell{},
		}

		if len(cols) == 0 { // the entire row
			for _, cf := range t.CFs {
				rkeyMap := cfFields[cf.Name]
				cellsMap := rkeyMap[string(pkeys)]
				if !isFlexible(cf) {
					for _, col := range t.Columns {
						if col.Family != cf.Name {
							continue
						}
						cell, ok := cellsMap[col.ColumnMeta.Name]
						if !ok {
							continue
						}
						rowCells.Cells = append(rowCells.Cells, cell)
					}
				} else {
					for _, cell := range cellsMap {
						rowCells.Cells = append(rowCells.Cells, cell)
					}
				}
			}
		} else {
			for _, col := range cols {
				rkeyMap, ok := cfFields[col.cf]
				if !ok {
					continue
				}
				cellsMap := rkeyMap[string(pkeys)]
				cell, ok := cellsMap[col.name]
				if !ok {
					continue
				}
				rowCells.Cells = append(rowCells.Cells, cell)
			}
		}
		if err := ri.sendData(rowCells); err != nil {
			return err
		}
	}

	return nil
}

func (t *tableCommon) genRow(pkeys []types.Datum, colMap map[int64]types.Datum, fields []*columnEntry) (*tspb.SliceCell, error) {
	pkvalMap := map[string]*tspb.Value{}
	prikeys := make([]*tspb.Value, 0)
	cells := make([]*tspb.Cell, len(fields))

	for i, col := range fields {
		val, err := protoValueFromDatum(colMap[col.id], col.t)
		if err != nil {
			return nil, err
		}
		cells[i] = &tspb.Cell{}
		cells[i].Column = col.name
		cells[i].Family = col.cf
		cells[i].Type = col.t
		cells[i].Value = val
		if col.IsPrimary {
			pkvalMap[col.name] = val
		}
	}
	for _, pk := range t.pkeys {
		prikeys = append(prikeys, pkvalMap[pk.ColumnMeta.Name])
	}
	return &tspb.SliceCell{
		PrimaryKeys: prikeys,
		Cells:       cells,
	}, nil
}

func (t *tableCommon) readRangeRows(ctx context.Context, krl keyRangeList, cols []*columnEntry, cfmap map[string]map[string]int, ri *resultIter) error {
	var (
		sc         = &stmtctx.StatementContext{TimeZone: time.Local}
		cfIDMap    = make(map[int64]*model.ColumnFamilyMeta)
		colEntries []*columnEntry
	)

	for cfname := range cfmap {
		if cf, ok := t.cfIDMap[cfname]; ok {
			cfIDMap[cf.Id] = cf
		}
	}
	colEntries = cols
	if len(cols) == 0 {
		colEntries = make([]*columnEntry, 0)
		for _, cf := range cfIDMap {
			coles := t.getCFColEntries(cf)
			colEntries = append(colEntries, coles...)
		}
	}

	pkfts := t.buildPrimaryKeyFieldTypes()

	readFunc := func(kr *keyRange) {
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
		//cfID := 0
		// defaultCF := t.cfIDMap[DefaultColumnFamily]
		startKey, err := tablecodec.EncodeRecordPrimaryKey(sc, t.tableID, startVals)
		if err != nil {
			logutil.Logger(ctx).Error("generate record key error", zap.Error(err))
			return
		}
		//cfID := 0
		endKey, err := tablecodec.EncodeRecordPrimaryKey(sc, t.tableID, endVals)
		if err != nil {
			logutil.Logger(ctx).Error("generate record key error", zap.Error(err))
			return
		}
		if !kr.startClosed {
			startKey = kv.Key(startKey).Next()
		}
		if kr.endClosed {
			endKey = kv.Key(endKey).PrefixNext()
		}
		kvpairs, err := t.scanRange(ctx, startKey, endKey)
		if err != nil {
			logutil.Logger(ctx).Error("scan range error", zap.Error(err))
		}

		for _, kvpair := range kvpairs {
			_, pKeys, cfID, _, err := tablecodec.DecodeRecordKey(kvpair.Key, pkfts, time.Local)
			if err != nil {
				logutil.Logger(ctx).Error("decode primary key error", zap.Error(err))
				ri.lastErr = err
				return
			}
			cfmeta, ok := cfIDMap[cfID]
			if !ok {
				continue
			}
			if isFlexible(cfmeta) {
				continue
			}
			row, err := tablecodec.DecodeRowWide(kvpair.Val, t.colFtMap, time.Local, nil)
			if err != nil {
				logutil.Logger(ctx).Error("decode rows column value error", zap.Error(err))
				ri.lastErr = err
				return
			}
			entry, err := t.genRow(pKeys, row, colEntries)
			if err != nil {
				logutil.Logger(ctx).Error("generate row values error", zap.Error(err))
				ri.lastErr = err
				return
			}
			ri.sendData(entry)
		}
	}
	for _, kr := range krl {
		readFunc(kr)
	}
	return nil
}

func (t *tableCommon) scanSparse(ctx context.Context, rowKeyPrefix kv.Key, cf *model.ColumnFamilyMeta) (map[string]*tspb.Cell, error) {
	var (
		txn    = ctx.Value(sessionctx.TxnIDKey).(kv.Transaction)
		colSet = map[string]*tspb.Cell{}
		sc     = &stmtctx.StatementContext{TimeZone: time.Local}
	)
	recKeyPrefix := tablecodec.EncodePkCF(rowKeyPrefix, cf.Id)
	iter, err := txn.Iter(recKeyPrefix, recKeyPrefix.PrefixNext())
	if err != nil {
		logutil.Logger(ctx).Error("get iter failed", zap.Error(err))
		return nil, err
	}
	for iter.Valid() {
		col := iter.Key()[len(recKeyPrefix):]
		if len(col) == 0 {
			iter.Next()
			continue
		}
		datum, err := tablecodec.DecodeRowHigh(sc, iter.Value(), nil, time.Local)
		if err != nil {
			logutil.Logger(ctx).Error("decode row high error", zap.Error(err))
			return nil, err
		}
		val, _, err := flexibleProtoValueFromDatum(datum)
		if err != nil {
			logutil.Logger(ctx).Error("convert datum to tspb.Value error", zap.Error(err))
			return nil, err
		}
		cell := &tspb.Cell{
			Column: string(col),
			Family: cf.Name,
			Type:   &tspb.Type{Code: tspb.TypeCode_TYPE_CODE_UNSPECIFIED},
			Value:  val,
		}
		colSet[string(col)] = cell
		iter.Next()
	}
	return colSet, nil
}

func (t *tableCommon) readIndex(ctx context.Context, index *model.IndexMeta, keySet *tspb.KeySet) (*resultIter, error) {
	// if keySet.GetAll() {
	// 	return t.allIndexKeys(ctx, index, ri)
	// }
	// if len(keySet.GetKeys()) > 0 {
	// 	if err := t.fetchIndexRows(ctx, index, keySet.Keys, ri); err != nil {
	// 		logutil.Logger(ctx).Error("fetch rows err", zap.Error(err))
	// 		return err
	// 	}
	// }
	// if len(keySet.GetRanges()) > 0 {
	// 	if err := t.scanIndexRange(ctx, index, makeKeyRangeList(keySet.GetRanges()), ri); err != nil {
	// 		logutil.Logger(ctx).Error("scan index range error", zap.Error(err))
	// 		return err
	// 	}
	// }
	return nil, nil
}

func (t *tableCommon) allIndexKeys(ctx context.Context, index *model.IndexMeta, ri *resultIter) error {
	ri.Close()
	return nil
}

func (t *tableCommon) fetchIndexRows(ctx context.Context, index *model.IndexMeta, lvs []*structpb.ListValue, ri *resultIter) error {
	// txn := ctx.Value(sessionctx.TxnIDKey).(kv.Transaction)
	// for _, lv := range lvs {
	// 	if index.Unique {
	// 		values, err := t.genIndexDatum(index, lv)
	// 		if err != nil {
	// 			logutil.Logger(ctx).Error("gen Index datum error", zap.Error(err))
	// 			return err
	// 		}
	// 		indexKey, err := genUniqueIndex(t.tableID, index.Id, values)
	// 		if err != nil {
	// 			logutil.Logger(ctx).Error("gen Unique index error", zap.Error(err))
	// 			return err
	// 		}
	// 		valBody, err := txn.Get(indexKey)
	// 		if err != nil {
	// 			logutil.Logger(ctx).Error("get Unique index value error", zap.Error(err))
	// 			return err
	// 		}
	// 		colMap := t.genPrimaryKeyFieldType()
	// 		pkeyMap, err := tablecodec.DecodeRowWide(valBody, colMap, time.Local, nil)
	// 		if err != nil {
	// 			logutil.Logger(ctx).Error("get Unique index value error", zap.Error(err))
	// 			return err
	// 		}
	// 		pKeys := t.genPrimaryKeyDatum(pkeyMap)
	// 		recKey, err := genRecordKey(t.tableID, pKeys, 0)
	// 		if err != nil {
	// 			logutil.Logger(ctx).Error("get reckey error", zap.Error(err))
	// 			return err
	// 		}
	// 		valBody, err = t.txn.Get(recKey)
	// 		if err != nil {
	// 			logutil.Logger(ctx).Error("get encode rows val error", zap.Error(err))
	// 			return err
	// 		}

	// 		row, err := tablecodec.DecodeRowWide(valBody, t.colFtMap, time.Local, nil)
	// 		if err != nil {
	// 			logutil.Logger(ctx).Error("decode rows column value error", zap.Error(err))
	// 			return err
	// 		}
	// 		entry, err := t.genRow(pKeys, row, ri.columns)
	// 		if err != nil {
	// 			logutil.Logger(ctx).Error("generate row values error", zap.Error(err))
	// 			ri.lastErr = err
	// 			return err
	// 		}
	// 		ri.sendData(entry)
	// 		continue
	// 	}
	// 	//tablecodec.EncodeTableIndexPrefix()
	// }
	return nil
}

func (t *tableCommon) genPrimaryKeyFieldType() map[int64]*types.FieldType {
	var ftypes = make(map[int64]*types.FieldType)
	for _, pkey := range t.meta.GetPrimaryKey() {
		colMeta := t.Columns[t.colIndex[pkey]]
		ftypes[colMeta.GetId()] = &colMeta.FieldType
	}
	return ftypes
}

func (t *tableCommon) genPrimaryKeyDatum(pkeys map[int64]types.Datum) []types.Datum {
	var values = make([]types.Datum, len(pkeys))
	for i, pkey := range t.meta.GetPrimaryKey() {
		colMeta := t.Columns[t.colIndex[pkey]]
		values[i] = pkeys[colMeta.Id]
	}
	return values
}

func (t *tableCommon) genIndexDatum(index *model.IndexMeta, lv *tspb.ListValue) ([]types.Datum, error) {
	var (
		values = make([]types.Datum, len(index.DefinedColumns))
	)
	for i, col := range index.GetDefinedColumns() {
		colMeta := t.Columns[t.colIndex[col]]

		val, err := datumFromTypeValue(colMeta.ColumnType, lv.Values[i])
		if err != nil {
			return nil, err
		}
		values[i] = val
	}
	return values, nil
}

func genUniqueIndex(tableID, indexID int64, values []types.Datum) (kv.Key, error) {
	sc := &stmtctx.StatementContext{TimeZone: time.Local}
	k, err := tablecodec.EncodeIndexUnique(sc, tableID, indexID, values)
	return kv.Key(k), err
}

func (t *tableCommon) scanIndexRange(ctx context.Context, index *model.IndexMeta, krl []*keyRange, ri *resultIter) error {
	return nil
}

func (t *tableCommon) scanRange(ctx context.Context, start, upperBound kv.Key) ([]*KeyValue, error) {
	txn := ctx.Value(sessionctx.TxnIDKey).(kv.Transaction)
	iter, err := txn.Iter(start, upperBound)
	if err != nil {
		logutil.Logger(ctx).Error("scan range failed", zap.Error(err))
		return nil, err
	}
	defer iter.Close()
	kvpairs := make([]*KeyValue, 0)
	for iter.Valid() {
		kv := &KeyValue{
			Key: iter.Key(),
			Val: iter.Value(),
		}
		kvpairs = append(kvpairs, kv)
		iter.Next()
	}
	return kvpairs, nil
}

func (t *tableCommon) prepareFields(ctx context.Context, columns []string) (fields []*columnEntry,
	cfColMap map[string]map[string]int,
	cfmap map[string]*model.ColumnFamilyMeta,
	err error) {

	fields = make([]*columnEntry, 0)
	cfmap = make(map[string]*model.ColumnFamilyMeta)
	cfColMap = make(map[string]map[string]int)
	// pkeys = make([]*columnEntry, 0)

	if len(columns) == 0 {
		for _, cf := range t.CFs {
			cfmap[cf.Name] = cf
			cfColMap[cf.Name] = make(map[string]int)
		}
		return fields, cfColMap, cfmap, nil
	}

	for _, col := range columns {
		cfn, coln, err := resolveColumn(col)
		if err != nil {
			return nil, nil, nil, err
		}
		cf, ok := t.cfIDMap[cfn]
		if !ok {
			return nil, nil, nil, status.Errorf(codes.FailedPrecondition, "no such column family: %v", cfn)
		}
		if _, ok := cfmap[cfn]; !ok {
			cfmap[cfn] = cf
			cfColMap[cfn] = map[string]int{}
		}
		var colEntry *columnEntry

		if !isFlexible(cf) {
			idx, ok := t.colIndex[col]
			if !ok {
				return nil, nil, nil, status.Errorf(codes.FailedPrecondition, "column `%s` not exists in table `%s`", col, t.meta.TableName)
			}

			colEntry = &columnEntry{
				id:   t.Columns[idx].Id,
				name: t.Columns[idx].ColumnMeta.Name,
				t:    t.Columns[idx].ColumnType,
				ft:   &t.Columns[idx].FieldType,
				cfID: cf.Id,
				cf:   cf.Name,
			}

		} else {
			colEntry = &columnEntry{
				name: coln,
				t:    &tspb.Type{Code: tspb.TypeCode_TYPE_CODE_UNSPECIFIED},
				ft:   nil,
				cfID: cf.Id,
				cf:   cf.Name,
			}
		}
		fields = append(fields, colEntry)
	}

	for i, col := range fields {
		if _, ok := cfColMap[col.cf][col.name]; !ok {
			cfColMap[col.cf][col.name] = i
		}
	}
	return fields, cfColMap, cfmap, nil
}

func makeFields(cols []*columnEntry) []*tspb.ColumnMeta {
	fields := make([]*tspb.ColumnMeta, len(cols))
	for i, col := range cols {
		fields[i] = &tspb.ColumnMeta{
			Name:       col.ColumnName(),
			ColumnType: col.t,
		}
	}
	return fields
}

func (t *tableCommon) primaryKeys(values []*tspb.Value) ([]int, []types.Datum, error) {
	if len(values) != t.pkCols {
		return nil, nil, status.Errorf(codes.InvalidArgument, "primary key length mismatch: got %d values, table has %d", len(values), t.pkCols)
	}

	var (
		pks   = []types.Datum{}
		pkIdx = []int{}
	)

	for i, value := range values {
		pkMeta := t.pkeys[i]
		pkDatum, err := datumFromTypeValue(pkMeta.ColumnType, value)
		if err != nil {
			return nil, nil, err
		}
		pks = append(pks, pkDatum)
		pkIdx = append(pkIdx, i)
	}
	return pkIdx, pks, nil
}

func (t *tableCommon) pKeyValuesFromDatum(pkeyDatums []types.Datum) ([]*tspb.Value, error) {
	var pvs = []*tspb.Value{}

	for i, pkDatum := range pkeyDatums {
		pv, err := protoValueFromDatum(pkDatum, t.pkeys[i].ColumnType)
		if err != nil {
			return nil, err
		}
		pvs = append(pvs, pv)
	}
	return pvs, nil
}
