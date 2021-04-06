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

	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/logutil"
	tspb "github.com/zhihu/zetta-proto/pkg/tablestore"
	"github.com/zhihu/zetta/pkg/model"
	"github.com/zhihu/zetta/pkg/tablecodec"
	"github.com/zhihu/zetta/tablestore/sessionctx"
	"github.com/zhihu/zetta/tablestore/table"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type sparseScanContext struct {
	startRowPrefix kv.Key
	stopRowPrefix  kv.Key
	rowCells       *tspb.SliceCell
	filterString   []byte
	caching        int32
	batchSize      int32
	sortColumns    bool
	reversed       bool
	cfidMap        map[int64]*model.ColumnFamilyMeta
	qlMap          map[int64]map[string]struct{}
}

func (t *tableCommon) ScanStore(ctx context.Context, scanReq *table.ScanRequest) (*resultIter, error) {
	scanCtx, err := t.prepareSparseScan(ctx, scanReq)
	if err != nil {
		return nil, err
	}

	ri := NewResultIter(0)
	go func() {
		defer ri.Close()
		if err := t.sparseScanRange(ctx, scanCtx, ri); err != nil {
			ri.lastErr = err
			return
		}
	}()
	return ri, nil
}

func (t *tableCommon) sparseScanRange(ctx context.Context, scanCtx *sparseScanContext, ri *resultIter) error {
	var (
		txn             = ctx.Value(sessionctx.TxnIDKey).(kv.Transaction)
		sc              = &stmtctx.StatementContext{TimeZone: time.Local}
		preRowKeyPrefix = scanCtx.startRowPrefix
		rowCells        *tspb.SliceCell
		rkFieldTypes    = make([]*types.FieldType, len(t.pkeys))
	)
	for i, pkeyMeta := range t.pkeys {
		rkFieldTypes[i] = &pkeyMeta.FieldType
	}
	iter, err := txn.Iter(scanCtx.startRowPrefix, scanCtx.stopRowPrefix)
	if err != nil {
		logutil.Logger(ctx).Error("get iterator error", zap.Error(err))
		return err
	}
	defer iter.Close()

	for iter.Valid() {
		if ctxErr := ctx.Err(); ctxErr != nil {
			logutil.Logger(ctx).Error("Client canceled scan", zap.Error(ctxErr))
			return ctxErr
		}
		_, rkeys, cfid, column, err := tablecodec.DecodeRecordKey(iter.Key(), rkFieldTypes, time.Local)
		if err != nil {
			logutil.BgLogger().Error("decode record key error", zap.Error(err))
			return err
		}
		if _, ok := scanCtx.cfidMap[cfid]; !ok {
			// undesired column family
			iter.Next()
			continue
		}
		qlDict := scanCtx.qlMap[cfid]
		qlDictSize := len(qlDict)
		if _, ok := qlDict[string(column)]; !ok && qlDictSize != 0 {
			// undesired column
			iter.Next()
			continue
		}
		rkeyPrefix, err := tablecodec.EncodeRecordPrimaryKey(sc, t.tableID, rkeys)
		if err != nil {
			logutil.BgLogger().Error("encode primary key prefix error", zap.Error(err))
			return err
		}

		datum, err := tablecodec.DecodeRowHigh(sc, iter.Value(), nil, time.Local)
		if err != nil {
			return err
		}
		pv, _, err := flexibleProtoValueFromDatum(datum)
		if err != nil {
			logutil.Logger(ctx).Error("gen proto value error", zap.Error(err))
			return err
		}

		cmpVal := kv.Key(rkeyPrefix).Cmp(preRowKeyPrefix)
		preRowKeyPrefix = rkeyPrefix
		if cmpVal > 0 {
			// switch to a new record row
			if rowCells != nil {
				// send pre record row cells data
				ri.sendData(rowCells)
			}
			rowCells, err = t.genRowCells(rkeys)
			if err != nil {
				return err
			}
		} else if cmpVal < 0 {
			logutil.BgLogger().Fatal("cmpVal < 0 shuld not be happend")
		}
		cell := &tspb.Cell{
			Family: scanCtx.cfidMap[cfid].Name,
			Column: string(column),
			Type:   &tspb.Type{Code: tspb.TypeCode_TYPE_CODE_UNSPECIFIED},
			Value:  pv,
		}
		if rowCells == nil {
			rowCells, err = t.genRowCells(rkeys)
			if err != nil {
				return err
			}
		}
		rowCells.Cells = append(rowCells.Cells, cell)
		iter.Next()
	}
	if rowCells != nil {
		ri.sendData(rowCells)
	}

	return nil

}
func (t *tableCommon) genRowCells(rkeys []types.Datum) (*tspb.SliceCell, error) {
	rkeyValues, err := t.pKeyValuesFromDatum(rkeys)
	if err != nil {
		logutil.Logger(context.Background()).Error("decode primary key values error", zap.Error(err))
		return nil, err
	}
	rowCells := &tspb.SliceCell{
		PrimaryKeys: rkeyValues,
		Cells:       []*tspb.Cell{},
	}
	return rowCells, nil
}

func (t *tableCommon) prepareSparseScan(ctx context.Context, scanReq *table.ScanRequest) (*sparseScanContext, error) {

	var scanCtx = &sparseScanContext{
		startRowPrefix: nil,
		stopRowPrefix:  nil,
		filterString:   scanReq.FilterString,
		caching:        scanReq.Caching,
		batchSize:      scanReq.BatchSize,
		sortColumns:    scanReq.SortColumns,
		reversed:       scanReq.Reversed,
		cfidMap:        make(map[int64]*model.ColumnFamilyMeta),
		qlMap:          make(map[int64]map[string]struct{}),
	}

	for _, cf := range t.cfIDMap {
		if len(scanReq.ColFamilyMap) != 0 {
			if _, ok := scanReq.ColFamilyMap[cf.Name]; !ok {
				continue
			}
			if !isFlexible(cf) {
				return nil, status.Errorf(codes.Canceled, "columns famliy %v mode not flexible", cf.Name)
			}

		}
		if !isFlexible(cf) {
			continue
		}
		scanCtx.cfidMap[cf.Id] = cf
		scanCtx.qlMap[cf.Id] = map[string]struct{}{}
	}

	for ql, cf := range scanReq.QualifierMap {
		scanCtx.qlMap[t.cfIDMap[cf].Id][ql] = struct{}{}
	}
	//encode startRowPrefix & stopRowPrefix
	startRowPrefix, err := t.encodeRowkeyPrefix(scanReq.StartRow)
	if err != nil {
		logutil.BgLogger().Error("encode start row prefix error", zap.Error(err))
		return nil, err
	}
	scanCtx.startRowPrefix = startRowPrefix
	if scanReq.StopRow == nil || len(scanReq.StopRow) == 0 {
		stopKey := kv.Key(tablecodec.EncodeKeyTablePrefix(t.tableID)).PrefixNext()
		scanCtx.stopRowPrefix = stopKey
	} else {
		stopRowPrefix, err := t.encodeRowkeyPrefix(scanReq.StopRow)
		if err != nil {
			logutil.BgLogger().Error("encode stop row prefix error", zap.Error(err))
			return nil, err
		}
		scanCtx.stopRowPrefix = stopRowPrefix
	}
	return scanCtx, nil
}

func (t *tableCommon) encodeRowkeyPrefix(rowkey []byte) (kv.Key, error) {
	var (
		sc = &stmtctx.StatementContext{TimeZone: time.Local}
	)
	if rowkey == nil || len(rowkey) == 0 {
		return tablecodec.EncodeKeyTablePrefix(t.tableID), nil
	}
	datums, err := t.buildRowkeyDatnum(rowkey)
	if err != nil {
		logutil.BgLogger().Error("build rowkey datum error", zap.Error(err))
		return nil, err
	}
	prefix, err := tablecodec.EncodeRecordPrimaryKey(sc, t.tableID, datums)
	if err != nil {
		logutil.BgLogger().Error("encode record rowkey prefix error", zap.Error(err))
		return nil, err
	}
	return prefix, nil
}

func (t *tableCommon) buildRowkeyDatnum(rowkey []byte) ([]types.Datum, error) {
	val := &tspb.Value{
		Kind: &tspb.Value_BytesValue{BytesValue: rowkey},
	}
	vals := []*tspb.Value{val}
	colTypes := make([]*tspb.Type, 0)
	for _, pkey := range t.pkeys {
		colType := pkey.ColumnType
		colTypes = append(colTypes, colType)
	}
	return t.datumsFromListValue(colTypes, vals)
}
