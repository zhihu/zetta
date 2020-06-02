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
	"github.com/zhihu/zetta/tablestore/sessionctx"
)

var (
	fetchRowsCounterOK   = metrics.FetchRowsCounter.WithLabelValues(metrics.LblOK)
	fetchRowsCounterErr  = metrics.FetchRowsCounter.WithLabelValues(metrics.LblError)
	fetchRowsDurationOK  = metrics.FetchRowsDuration.WithLabelValues(metrics.LblOK)
	fetchRowsDurationErr = metrics.FetchRowsDuration.WithLabelValues(metrics.LblError)
)

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

	ri := &resultIter{
		rowChan: make(chan *tspb.SliceCell, 32),
		limit:   rr.Limit,
	}

	// indexMeta := t.searchIndex(index)
	// if indexMeta != nil {
	// 	t.readIndex(ctx, indexMeta, keySet)
	// }

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
			// krl := makeKeyRangeList(keySet.GetRanges())
			// if err := t.readRangeRows(ctx, krl, ri); err != nil {
			// 	logutil.Logger(ctx).Error("fetch rows for ranges err", zap.Error(err))
			// 	return err
			// }
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
								Column: col.Name,
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
							Column: col.Name,
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
		txn       = ctx.Value(sessionctx.TxnIDKey).(kv.Transaction)
		recKeys   = make([]kv.Key, len(pkeysList))
		sc        = &stmtctx.StatementContext{TimeZone: time.Local}
		fieldsMap = map[string]map[string]*tspb.Cell{}
	)

	for i, keyPrefix := range pkeysList {
		rkey := tablecodec.EncodePkCF(keyPrefix, cf.Id)
		fieldsMap[string(keyPrefix)] = map[string]*tspb.Cell{}
		recKeys[i] = rkey
	}

	valsMap, err := txn.BatchGet(recKeys)
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
						fields[col.Name] = &tspb.Cell{
							Family: cf.Name,
							Column: col.Name,
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
					fields[col.Name] = &tspb.Cell{
						Family: cf.Name,
						Column: col.Name,
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

		if len(cols) == 0 {
			for _, cf := range t.CFs {
				rkeyMap := cfFields[cf.Name]
				cellsMap := rkeyMap[string(pkeys)]
				if !isFlexible(cf) {
					for _, col := range t.Columns {
						if col.Family != cf.Name {
							continue
						}
						cell, ok := cellsMap[col.Name]
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
			return nil
		}
	}

	return nil
}

func (t *tableCommon) getRow() error {
	// txn := ctx.Value(sessionctx.TxnIDKey).(kv.Transaction)

	// pKeys, recKeys, err := t.getPrimaryKeyData(ctx, lv)
	// if err != nil {
	// 	return err
	// }

	// valBody, err := txn.Get(recKey)
	// if err != nil {
	// 	logutil.Logger(ctx).Error("get encode rows val error", zap.Error(err))
	// 	return err
	// }

	// row, err := tablecodec.DecodeRowWide(valBody, t.colFtMap, time.Local, nil)
	// if err != nil {
	// 	logutil.Logger(ctx).Error("decode rows column value error", zap.Error(err))
	// 	return err
	// }
	// entry, err := t.genRow(pKeys, row, ri.columns)
	// if err != nil {
	// 	logutil.Logger(ctx).Error("generate row values error", zap.Error(err))
	// 	ri.lastErr = err
	// 	return err
	// }
	// ri.sendData(entry)
	return nil
}

func (t *tableCommon) readRangeRows(ctx context.Context, krl keyRangeList, ri *resultIter) error {
	// pkfts := t.buildPrimaryKeyFieldTypes()
	// readFunc := func(kr *keyRange) {
	// 	startVals, err := t.buildPrimaryKeyValues(kr.start)
	// 	if err != nil {
	// 		logutil.Logger(ctx).Error("build start primary key datums error", zap.Error(err))
	// 		return
	// 	}
	// 	endVals, err := t.buildPrimaryKeyValues(kr.end)
	// 	if err != nil {
	// 		logutil.Logger(ctx).Error("build end primary key datums error", zap.Error(err))
	// 		return
	// 	}
	// 	//cfID := 0
	// 	startKey, err := genRecordKey(t.tableID, startVals, 0)
	// 	if err != nil {
	// 		logutil.Logger(ctx).Error("generate record key error", zap.Error(err))
	// 		return
	// 	}
	// 	//cfID := 0
	// 	endKey, err := genRecordKey(t.tableID, endVals, 0)
	// 	if err != nil {
	// 		logutil.Logger(ctx).Error("generate record key error", zap.Error(err))
	// 		return
	// 	}
	// 	if !kr.startClosed {
	// 		startKey = startKey.Next()
	// 	}
	// 	if kr.endClosed {
	// 		endKey = endKey.Next()
	// 	}
	// 	kvpairs, err := t.scanRange(ctx, startKey, endKey)
	// 	if err != nil {
	// 		logutil.Logger(ctx).Error("scan range error", zap.Error(err))
	// 	}
	// 	for _, kvpair := range kvpairs {
	// 		_, pKeys, _, _, err := tablecodec.DecodeRecordKey(kvpair.Key, pkfts, time.Local)
	// 		if err != nil {
	// 			logutil.Logger(ctx).Error("decode primary key error", zap.Error(err))
	// 			ri.lastErr = err
	// 			return
	// 		}

	// 		row, err := tablecodec.DecodeRowWide(kvpair.Val, t.colFtMap, time.Local, nil)
	// 		if err != nil {
	// 			logutil.Logger(ctx).Error("decode rows column value error", zap.Error(err))
	// 			ri.lastErr = err
	// 			return
	// 		}
	// 		entry, err := t.genRow(pKeys, row, ri.Columns())
	// 		if err != nil {
	// 			logutil.Logger(ctx).Error("generate row values error", zap.Error(err))
	// 			ri.lastErr = err
	// 			return
	// 		}
	// 		ri.sendData(entry)
	// 	}
	// }
	// for _, kr := range krl {
	// 	readFunc(kr)
	// }
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
		ftypes[colMeta.GetId()] = colMeta.FieldType
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
				name: t.Columns[idx].Name,
				t:    t.Columns[idx].ColumnType,
				ft:   t.Columns[idx].FieldType,
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
