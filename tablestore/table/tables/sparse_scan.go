package tables

import (
	"context"
	"fmt"
	"time"

	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/logutil"
	tspb "github.com/zhihu/zetta-proto/pkg/tablestore"
	"github.com/zhihu/zetta/pkg/model"
	"github.com/zhihu/zetta/pkg/tablecodec"
	"github.com/zhihu/zetta/tablestore/sessionctx"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (t *tableCommon) SparseScan(ctx context.Context, req *tspb.SparseScanRequest) (*resultIter, error) {
	ri := NewResultIter(req.Limit)
	scanCtx, err := t.parseSparseScan(ctx, req)
	if err != nil {
		return nil, err
	}
	go func() {
		defer func() {
			ri.Close()
		}()
		keySet := req.KeySet
		if keySet.GetAll() {

		}

		if len(keySet.GetKeys()) > 0 {

		}

		if len(keySet.GetRanges()) > 0 {
			krl := makeKeyRangeList(keySet.GetRanges())
			if err := t.scanKeyRanges(ctx, scanCtx, krl, ri); err != nil {
				logutil.Logger(ctx).Error("fetch rows for ranges err", zap.Error(err))
				return
			}
		}

	}()

	return ri, nil
}

func (t *tableCommon) scanKeyRanges(ctx context.Context, scanCtx *sparseScanContext, krl keyRangeList, ri *resultIter) error {

	for _, kr := range krl {
		if err := t.scanSingleRange(ctx, scanCtx, kr, ri); err != nil {
			return err
		}
	}
	return nil
}

func (t *tableCommon) scanSingleRange(ctx context.Context, scanCtx *sparseScanContext, kr *keyRange, ri *resultIter) error {
	if err := t.prepareBound(ctx, scanCtx, kr); err != nil {
		return err
	}

	if err := t.sparseScanRange2(ctx, scanCtx, ri); err != nil {
		return err
	}
	return nil
}

func (t *tableCommon) prepareBound(ctx context.Context, scanCtx *sparseScanContext, kr *keyRange) error {
	var (
		sc = &stmtctx.StatementContext{TimeZone: time.Local}
	)

	startVals, err := t.buildPrimaryKeyValues(kr.start)
	if err != nil {
		logutil.Logger(ctx).Error("build start primary key datums error", zap.Error(err))
		return err
	}
	endVals, err := t.buildPrimaryKeyValues(kr.end)
	if err != nil {
		logutil.Logger(ctx).Error("build end primary key datums error", zap.Error(err))
		return err
	}

	startKey, err := tablecodec.EncodeRecordPrimaryKey(sc, t.tableID, startVals)
	if err != nil {
		logutil.Logger(ctx).Error("encode start record key prefix error", zap.Error(err))
		return err
	}

	endKey, err := tablecodec.EncodeRecordPrimaryKey(sc, t.tableID, endVals)
	if err != nil {
		logutil.Logger(ctx).Error("encode end record key prefix error", zap.Error(err))
		return err
	}

	if !kr.startClosed {
		startKey = kv.Key(startKey).Next()
	}
	if kr.endClosed {
		endKey = kv.Key(endKey).PrefixNext()
	}
	scanCtx.startRowPrefix = startKey
	scanCtx.stopRowPrefix = endKey

	return nil
}

func (t *tableCommon) parseSparseScan(ctx context.Context, req *tspb.SparseScanRequest) (*sparseScanContext, error) {
	var scanCtx = &sparseScanContext{
		startRowPrefix: nil,
		stopRowPrefix:  nil,
		cfidMap:        make(map[int64]*model.ColumnFamilyMeta),
		qlMap:          make(map[int64]map[string]struct{}),
	}
	for _, cf := range t.cfIDMap {
		if req.Family != cf.Name {
			continue
		}
		if !isFlexible(cf) {
			return nil, status.Errorf(codes.Canceled, "columns famliy %v mode not flexible", cf.Name)
		}
		scanCtx.cfidMap[cf.Id] = cf
		scanCtx.qlMap[cf.Id] = map[string]struct{}{}
	}
	for _, ql := range req.Qualifiers {
		scanCtx.qlMap[t.cfIDMap[req.Family].Id][ql] = struct{}{}
	}
	return scanCtx, nil
}

func (t *tableCommon) sparseScanRange2(ctx context.Context, scanCtx *sparseScanContext, ri *resultIter) error {
	var (
		txn             = ctx.Value(sessionctx.TxnIDKey).(kv.Transaction)
		sc              = &stmtctx.StatementContext{TimeZone: time.Local}
		preRowKeyPrefix = scanCtx.startRowPrefix
		rowCells        *tspb.SliceCell
		rkFieldTypes    = make([]*types.FieldType, len(t.pkeys))
		lastPkeyVals    []*tspb.Value

		streamRead       = sessionctx.StreamReadFromContext(ctx)
		streamBatchIndex = 0
		cells            []*tspb.Cell
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
			lastPkeyVals = rowCells.PrimaryKeys
		} else if cmpVal < 0 {
			logutil.BgLogger().Fatal("cmpVal < 0 shuld not be happend")
			return fmt.Errorf("unexpect error for cmpVal < 0")
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
			lastPkeyVals = rowCells.PrimaryKeys
		}
		if streamRead {
			if streamBatchIndex == 0 {
				rowCells.Cells = nil
				cells = make([]*tspb.Cell, streamingBatchSize)
			}
			cells[streamBatchIndex] = cell
			streamBatchIndex++
			if streamBatchIndex >= streamingBatchSize {
				streamBatchIndex = 0
				rowCells.Cells = cells
				if err := ri.sendData(rowCells); err != nil {
					return err
				}
				rowCells = &tspb.SliceCell{
					PrimaryKeys: lastPkeyVals,
					Cells:       cells,
				}
			}
		} else {
			rowCells.Cells = append(rowCells.Cells, cell)
		}
		iter.Next()
	}
	if streamBatchIndex > 0 {
		rowCells = &tspb.SliceCell{
			PrimaryKeys: lastPkeyVals,
			Cells:       cells[:streamBatchIndex],
		}
		if err := ri.sendData(rowCells); err != nil {
			return err
		}
		streamBatchIndex = 0

	} else if rowCells != nil {
		ri.sendData(rowCells)
	}

	return nil

}
