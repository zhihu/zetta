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

const streamingBatchSize = 1000

type sparseRow struct {
	idx        int
	raw        *tspb.Row
	entireRow  bool
	rowCells   *tspb.SliceCell
	rkeyPrefix kv.Key
}

type sparseContext struct {
	cf      *model.ColumnFamilyMeta
	rowKeys []kv.Key
	cellMap map[string]*sparseRow
	srows   []*sparseRow
}

func buildSparseContext(cf *model.ColumnFamilyMeta) *sparseContext {
	return &sparseContext{
		cf:      cf,
		rowKeys: []kv.Key{},
		cellMap: map[string]*sparseRow{},
		srows:   []*sparseRow{},
	}
}

func (t *tableCommon) ReadSparse(ctx context.Context, req *tspb.SparseReadRequest) (*resultIter, error) {

	if req.Family == DefaultColumnFamily {
		return nil, status.Error(codes.FailedPrecondition, "default column family not suitable for sparse read")
	}

	sparseCtxs, _, err := t.prepareRowKeys(ctx, req)
	if err != nil {
		return nil, err
	}

	ri := NewResultIter(req.Limit)

	go func() {
		defer ri.Close()
		for _, sparseCtx := range sparseCtxs {
			startTS := time.Now()
			err := t.fetchSparse(ctx, sparseCtx, ri)
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
		}
	}()
	return ri, nil
}

func (t *tableCommon) prepareRowKeys(ctx context.Context, sr *tspb.SparseReadRequest) ([]*sparseContext, []*model.ColumnFamilyMeta, error) {
	var (
		sparseCtxs = []*sparseContext{}
		cfList     = []*model.ColumnFamilyMeta{}
	)
	if sr.Family == "" {
		for _, cf := range t.cfIDMap {
			if isFlexible(cf) {
				cfList = append(cfList, cf)
				sparseCtx := buildSparseContext(cf)
				sparseCtxs = append(sparseCtxs, sparseCtx)
			}
		}
	} else {
		cf, ok := t.cfIDMap[sr.Family]
		if !ok {
			return nil, nil, status.Errorf(codes.NotFound, "column family: %v not found", sr.Family)
		}
		if !isFlexible(cf) {
			return nil, nil, status.Errorf(codes.Canceled, "columns famliy %v mode not flexible", sr.Family)
		}

		cfList = append(cfList, cf)
		sparseCtx := buildSparseContext(cf)
		sparseCtxs = append(sparseCtxs, sparseCtx)
	}

	for scid, cf := range cfList {
		for i, row := range sr.Rows {
			pkeyDatums, err := t.getPrimaryKeyData(ctx, row.Keys)
			if err != nil {
				return nil, nil, status.Error(codes.Aborted, err.Error())
			}
			rkeyPrefix, err := genRecordPrimaryCFPrefix(t.tableID, pkeyDatums, cf.Id)
			if err != nil {
				return nil, nil, status.Error(codes.Aborted, err.Error())
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

			sparseCtxs[scid].srows = append(sparseCtxs[scid].srows, srow)
			if len(row.Qualifiers) == 0 {
				srow.entireRow = true
				continue
			}
			for _, col := range row.Qualifiers {
				rkey := tablecodec.EncodePkCFColumn(rkeyPrefix, []byte(col))
				sparseCtxs[scid].rowKeys = append(sparseCtxs[scid].rowKeys, rkey)
				sparseCtxs[scid].cellMap[string(rkey)] = srow
			}
		}
	}
	return sparseCtxs, cfList, nil
}

func (t *tableCommon) fetchSparse(ctx context.Context, sparseCtx *sparseContext, ri *resultIter) error {

	var (
		wg         sync.WaitGroup
		err1, err2 error

		rowKeys = sparseCtx.rowKeys
		cellMap = sparseCtx.cellMap
		srows   = sparseCtx.srows
		cf      = sparseCtx.cf
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
	valsMap, err := txn.BatchGet(ctx, rowKeys)
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
				if err := ri.sendData(preSrow.rowCells); err == table.ErrResultSetUserLimitReached {
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

type kvPair struct {
	key   []byte
	value []byte
	sRow  *sparseRow
	err   error
}

func (t *tableCommon) scanSparseRow(ctx context.Context, srows []*sparseRow, cf *model.ColumnFamilyMeta, ri *resultIter) error {
	if len(srows) == 0 {
		return nil
	}

	var (
		txn              = ctx.Value(sessionctx.TxnIDKey).(kv.Transaction)
		sc               = &stmtctx.StatementContext{TimeZone: time.Local}
		streamRead       = sessionctx.StreamReadFromContext(ctx)
		streamBatchIndex = 0
	)

	var cells []*tspb.Cell
	kvPairChan := make(chan *kvPair, 10000)
	defer close(kvPairChan)

	go func() {
		for _, srow := range srows {
			if !srow.entireRow {
				continue
			}
			iter, err := txn.Iter(srow.rkeyPrefix, srow.rkeyPrefix.PrefixNext())
			if err != nil {
				logutil.Logger(ctx).Error("get iterator error", zap.Error(err))
				pair := &kvPair{
					err: err,
				}
				kvPairChan <- pair
				return
			}
			defer iter.Close()
			for iter.Valid() {
				if ctxErr := ctx.Err(); ctxErr != nil {
					logutil.Logger(ctx).Error("Client canceled scan", zap.Error(ctxErr))
					pair := &kvPair{
						err: ctxErr,
					}
					kvPairChan <- pair
					return
				}
				pair := &kvPair{
					key:   iter.Key(),
					value: iter.Value(),
					sRow:  srow,
				}
				kvPairChan <- pair
				iter.Next()
			}
		}
		kvPairChan <- nil
	}()

	srow := srows[0]

	for pair := range kvPairChan {
		if pair != nil && pair.err != nil {
			return pair.err
		}

		if pair == nil || pair.sRow != srow {
			if streamBatchIndex > 0 {
				rowCells := &tspb.SliceCell{
					PrimaryKeys: srow.rowCells.PrimaryKeys,
					Cells:       cells[:streamBatchIndex],
				}
				if err := ri.sendData(rowCells); err != nil {
					return err
				}
				streamBatchIndex = 0
			}

			if pair == nil {
				return nil
			}

			srow = pair.sRow
		}

		datum, err := tablecodec.DecodeRowHigh(sc, pair.value, nil, time.Local)
		if err != nil {
			return err
		}
		pv, _, err := flexibleProtoValueFromDatum(datum)
		if err != nil {
			logutil.Logger(ctx).Error("gen proto value error", zap.Error(err))
			return err
		}
		column := pair.key[len(srow.rkeyPrefix):]

		cell := &tspb.Cell{
			Family: cf.Name,
			Column: string(column),
			Type:   &tspb.Type{Code: tspb.TypeCode_TYPE_CODE_UNSPECIFIED},
			Value:  pv,
		}
		if streamRead {
			if streamBatchIndex == 0 {
				cells = make([]*tspb.Cell, streamingBatchSize)
			}
			cells[streamBatchIndex] = cell
			streamBatchIndex++
			if streamBatchIndex >= streamingBatchSize {
				streamBatchIndex = 0
				rowCells := &tspb.SliceCell{
					PrimaryKeys: srow.rowCells.PrimaryKeys,
					Cells:       cells,
				}
				if err := ri.sendData(rowCells); err != nil {
					return err
				}
			}
		} else {
			srow.rowCells.Cells = append(srow.rowCells.Cells, cell)
		}
	}

	return nil
}

func hasSuffix(rkey kv.Key, column []byte) bool {
	return bytes.HasSuffix(rkey, column)
}
