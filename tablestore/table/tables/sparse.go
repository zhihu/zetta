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
	"sync"
	"time"

	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	tspb "github.com/zhihu/zetta-proto/pkg/tablestore"
	"github.com/zhihu/zetta/pkg/metrics"
	"github.com/zhihu/zetta/pkg/model"
	"github.com/zhihu/zetta/pkg/tablecodec"
	"github.com/zhihu/zetta/tablestore/sessionctx"
	"github.com/zhihu/zetta/tablestore/table"
)

var (
	fetchSparseCounterOK   = metrics.FetchSparseCounter.WithLabelValues(metrics.LblOK)
	fetchSparseCounterErr  = metrics.FetchSparseCounter.WithLabelValues(metrics.LblError)
	fetchSparseDurationOK  = metrics.FetchSparseDuration.WithLabelValues(metrics.LblOK)
	fetchSparseDurationErr = metrics.FetchSparseDuration.WithLabelValues(metrics.LblError)

	batchSparseCounterOK  = metrics.BatchSparseCounter.WithLabelValues(metrics.LblOK)
	batchSparseCounterErr = metrics.BatchSparseCounter.WithLabelValues(metrics.LblError)
	batchSparseDurationOK = metrics.BatchSparseDuration.WithLabelValues(metrics.LblOK)

	scanSparseCounterOK  = metrics.ScanSparseCounter.WithLabelValues(metrics.LblOK)
	scanSparseCounterErr = metrics.ScanSparseCounter.WithLabelValues(metrics.LblError)
	scanSparseDurationOK = metrics.ScanSparseDuration.WithLabelValues(metrics.LblOK)
)

type sparseRow struct {
	idx        int
	raw        *tspb.Row
	entireRow  bool
	rowCells   *tspb.SliceCell
	rkeyPrefix kv.Key
}

func (t *tableCommon) ReadSparse(ctx context.Context, req *tspb.SparseReadRequest) (*resultIter, error) {

	if len(req.Family) == 0 {
		return nil, status.Error(codes.FailedPrecondition, "family must be specific")
	}

	if req.Family == DefaultColumnFamily {
		return nil, status.Error(codes.FailedPrecondition, "default column family not suitable for sparse read")
	}

	rkeys, cellMap, srows, cf, err := t.prepareRowKeys(ctx, req)
	if err != nil {
		return nil, err
	}

	ri := &resultIter{
		rowChan: make(chan *tspb.SliceCell, 32),
		limit:   req.Limit,
	}

	go func() {
		defer ri.Close()
		startTS := time.Now()
		err := t.fetchSparse(ctx, rkeys, cellMap, srows, cf, ri)
		durFetchSparse := time.Since(startTS)
		if err != nil {
			fetchSparseCounterErr.Inc()
			fetchSparseDurationErr.Observe(durFetchSparse.Seconds())
			ri.lastErr = err
			logutil.Logger(ctx).Error("fetch sparse value err", zap.Error(err))
			return
		}
		fetchSparseDurationOK.Observe(durFetchSparse.Seconds())
		fetchSparseCounterOK.Inc()
	}()
	return ri, nil
}

func (t *tableCommon) prepareRowKeys(ctx context.Context, sr *tspb.SparseReadRequest) ([]kv.Key, map[string]*sparseRow, []*sparseRow, *model.ColumnFamilyMeta, error) {
	var (
		rowKeys = []kv.Key{}
		cellMap = map[string]*sparseRow{}
		srows   = []*sparseRow{}
	)
	cf, ok := t.cfIDMap[sr.Family]
	if !ok {
		return nil, nil, nil, nil, status.Errorf(codes.NotFound, "column family: %v not found", sr.Family)
	}
	for i, row := range sr.Rows {
		pkeyDatums, err := t.getPrimaryKeyData(ctx, row.Keys)
		if err != nil {
			return nil, nil, nil, nil, status.Error(codes.Aborted, err.Error())
		}
		rkeyPrefix, err := genRecordPrimaryCFPrefix(t.tableID, pkeyDatums, cf.Id)
		if err != nil {
			return nil, nil, nil, nil, status.Error(codes.Aborted, err.Error())
		}

		srow := &sparseRow{
			idx:        i,
			raw:        row,
			rkeyPrefix: rkeyPrefix,
			rowCells: &tspb.SliceCell{
				PrimaryKeys: row.Keys.Values,
				Cells:       []*tspb.Cell{},
			},
		}

		srows = append(srows, srow)
		qLen := len(row.Qualifiers)
		if qLen == 0 {
			srow.entireRow = true
			srow.rowCells.Cells = make([]*tspb.Cell, 0, 200000)
			continue
		}
		srow.rowCells.Cells = make([]*tspb.Cell, 0, qLen)

		for _, col := range row.Qualifiers {
			rkey := tablecodec.EncodePkCFColumn(rkeyPrefix, []byte(col))
			rowKeys = append(rowKeys, rkey)
			cellMap[string(rkey)] = srow
		}
	}
	return rowKeys, cellMap, srows, cf, nil
}

func (t *tableCommon) fetchSparse(ctx context.Context, rowKeys []kv.Key, cellMap map[string]*sparseRow, srows []*sparseRow, cf *model.ColumnFamilyMeta, ri *resultIter) error {
	var (
		wg         sync.WaitGroup
		err1, err2 error
	)
	streamRead := sessionctx.StreamReadFromContext(ctx)
	startTs := time.Now()

	if len(rowKeys) > 0 {
		if !streamRead {
			wg.Add(1)
		}
		go func() {
			if !streamRead {
				defer wg.Done()
			}
			err1 = t.readSparseBatch(ctx, rowKeys, cellMap, cf, ri)
			if err1 != nil {
				batchSparseCounterErr.Inc()
				return
			}
			batchSparseCounterOK.Inc()
			batchSparseDurationOK.Observe(time.Since(startTs).Seconds())
		}()
	}
	err2 = t.scanSparseRow(ctx, srows, cf, ri)
	if err2 != nil {
		scanSparseCounterErr.Inc()
		return err2
	}
	scanSparseCounterOK.Inc()
	scanSparseDurationOK.Observe(time.Since(startTs).Seconds())
	if !streamRead {
		wg.Wait()
	}

	if err1 != nil {
		return err1
	}
	if !streamRead {
		for _, srow := range srows {
			ri.sendData(srow.rowCells)
		}
	}
	return nil
}

func (t *tableCommon) readSparseBatch(ctx context.Context, rowKeys []kv.Key, cellMap map[string]*sparseRow, cf *model.ColumnFamilyMeta, ri *resultIter) error {
	var (
		txn                   = ctx.Value(sessionctx.TxnIDKey).(kv.Transaction)
		sc                    = &stmtctx.StatementContext{TimeZone: time.Local}
		streamRead            = sessionctx.StreamReadFromContext(ctx)
		preIdx                = -1
		preSrow    *sparseRow = nil
	)
	valsMap, err := txn.BatchGet(rowKeys)
	if err != nil {
		logutil.Logger(ctx).Error("batch get row value error", zap.Error(err))
		return err
	}
	for i := 0; i < len(rowKeys); i++ {
		val, ok := valsMap[string(rowKeys[i])]
		if !ok {
			continue
		}

		srow := cellMap[string(rowKeys[i])]
		if srow.idx > preIdx {
			if preSrow != nil && streamRead {
				if err := ri.sendData(preSrow.rowCells); err == table.ErrUserLimitReached {
					return nil
				}
			}
			preIdx = srow.idx
			preSrow = srow

			if srow.rowCells == nil {
				srow.rowCells = &tspb.SliceCell{
					PrimaryKeys: srow.raw.Keys.Values,
					Cells:       []*tspb.Cell{},
				}
			}
		}

		datum, err := tablecodec.DecodeRowHigh(sc, val, nil, time.Local)
		if err != nil {
			return err
		}
		pv, _, err := flexibleProtoValueFromDatum(datum)
		if err != nil {
			logutil.Logger(ctx).Error("gen proto value error", zap.Error(err))
			return err
		}

		column := rowKeys[i][len(srow.rkeyPrefix):]

		cell := &tspb.Cell{
			Family: cf.Name,
			Column: string(column),
			Type:   &tspb.Type{Code: tspb.TypeCode_TYPE_CODE_UNSPECIFIED},
			Value:  pv,
		}
		srow.rowCells.Cells = append(srow.rowCells.Cells, cell)
	}
	return nil
}

func (t *tableCommon) scanSparseRow(ctx context.Context, srows []*sparseRow, cf *model.ColumnFamilyMeta, ri *resultIter) error {
	metrics.MetricCount("scan_sparse_row")
	tm := metrics.MetricStartTiming()
	defer func() {
		metrics.MetricRecordTiming(tm, "scan_sparse_row")
	}()
	var (
		txn        = ctx.Value(sessionctx.TxnIDKey).(kv.Transaction)
		sc         = &stmtctx.StatementContext{TimeZone: time.Local}
		streamRead = sessionctx.StreamReadFromContext(ctx)
	)

	for _, srow := range srows {
		if !srow.entireRow {
			continue
		}
		iter, err := txn.Iter(srow.rkeyPrefix, srow.rkeyPrefix.PrefixNext())
		if err != nil {
			logutil.Logger(ctx).Error("get iterator error", zap.Error(err))
			return err
		}
		for iter.Valid() {
			datum, err := tablecodec.DecodeRowHigh(sc, iter.Value(), nil, time.Local)
			if err != nil {
				return err
			}
			pv, _, err := flexibleProtoValueFromDatum(datum)
			if err != nil {
				logutil.Logger(ctx).Error("gen proto value error", zap.Error(err))
				return err
			}
			column := iter.Key()[len(srow.rkeyPrefix):]

			cell := &tspb.Cell{
				Family: cf.Name,
				Column: string(column),
				Type:   &tspb.Type{Code: tspb.TypeCode_TYPE_CODE_UNSPECIFIED},
				Value:  pv,
			}
			if srow.rowCells == nil {
				srow.rowCells = &tspb.SliceCell{
					PrimaryKeys: srow.raw.Keys.Values,
					Cells:       []*tspb.Cell{},
				}
			}
			srow.rowCells.Cells = append(srow.rowCells.Cells, cell)
			iter.Next()
		}
		if streamRead {
			if err := ri.sendData(srow.rowCells); err != nil {
				return err
			}
		}
	}
	// metrics.MetricRecordTiming(tm, "scan_sparse_row")
	return nil
}

func hasSuffix(rkey kv.Key, column []byte) bool {
	return bytes.HasSuffix(rkey, column)
}
