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

package session

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/util/logutil"

	tspb "github.com/zhihu/zetta-proto/pkg/tablestore"
	"github.com/zhihu/zetta/tablestore/domain"
	"github.com/zhihu/zetta/tablestore/rpc"
	sctx "github.com/zhihu/zetta/tablestore/sessionctx"
	"github.com/zhihu/zetta/tablestore/table"

	"github.com/zhihu/zetta/tablestore/table/tables"
)

func (s *session) HandleRead(ctx context.Context, req rpc.ReadRequest, txn kv.Transaction) (RecordSet, error) {
	var (
		ri  RecordSet
		err error
	)
	if err := s.FetchTxn(txn.(*TxnState), true); err != nil {
		return nil, err
	}
	ztable, err := s.prepareZettaTable(ctx, req.GetTable())
	if err != nil {
		return nil, err
	}

	ctx = sctx.SetTxnCtx(ctx, txn)
	switch req.(type) {
	case *tspb.ReadRequest:
		ri, err = ztable.ReadStore(ctx, req.(*tspb.ReadRequest))
	case *tspb.SparseReadRequest:
		ri, err = ztable.ReadSparse(ctx, req.(*tspb.SparseReadRequest))
	case *tspb.SparseScanRequest:
		ri, err = ztable.SparseScan(ctx, req.(*tspb.SparseScanRequest))
	case *table.ScanRequest:
		ri, err = ztable.ScanStore(ctx, req.(*table.ScanRequest))
	default:
		return nil, status.Error(codes.FailedPrecondition, "not expect read request")
	}

	if err != nil {
		logutil.Logger(ctx).Error("read store err ", zap.Error(err))
		return nil, err
	}
	return ri, nil
}

func (s *session) HandleMutate(ctx context.Context, req rpc.MutationRequest, txn kv.Transaction) (rpc.Response, error) {
	var (
		tx  = txn.(*TxnState)
		err error
	)
	if tx.TxnOpt.ReadOnly {
		return nil, status.Errorf(codes.Aborted, "session:%s tx:%s read only", s.name, tx.ID)
	}
	if err := s.FetchTxn(txn.(*TxnState), true); err != nil {
		return nil, err
	}
	nctx := sctx.SetTxnCtx(ctx, tx)
	if err = s.dispatchMutation(nctx, req, tx); err != nil {
		return &rpc.CommitTimeResp{CommitTs: time.Time{}}, err
	}
	ct, err := s.doMutationCommit(nctx, req, tx)
	if err == nil {
		return &rpc.CommitTimeResp{CommitTs: ct}, nil
	}

	if err != nil {
		if !kv.IsTxnRetryableError(err) {
			logutil.Logger(ctx).Error("txn commit error", zap.Error(err))
			return nil, status.Errorf(codes.Aborted, "transaction commit error %v", err)
		}
		return s.retry(ctx, req, 5)
	}

	return &rpc.CommitTimeResp{CommitTs: ct}, nil

}

func (s *session) retry(ctx context.Context, req rpc.MutationRequest, maxCnt int) (rpc.Response, error) {
	var retryCnt uint

	for {
		tx, err := s.buildRetryTxn(ctx)
		if err != nil {
			logutil.Logger(ctx).Error("build retry txn error", zap.Error(err))
		}
		nctx := sctx.SetTxnCtx(ctx, tx)
		if err = s.dispatchMutation(nctx, req, tx); err != nil {
			return &rpc.CommitTimeResp{CommitTs: time.Time{}}, err
		}

		ct, err := s.doMutationCommit(nctx, req, tx)
		if err == nil {
			return &rpc.CommitTimeResp{CommitTs: ct}, err
		}
		if !kv.IsTxnRetryableError(err) {
			logutil.Logger(ctx).Error("txn commit error", zap.Error(err))
			return nil, status.Errorf(codes.Aborted, "transaction commit error %v", err)
		}

		retryCnt++
		if maxCnt != -1 && int(retryCnt) >= maxCnt {
			return &rpc.CommitTimeResp{CommitTs: time.Time{}}, err
		}
		kv.BackOff(retryCnt)
	}
}

func (s *session) buildRetryTxn(ctx context.Context) (*TxnState, error) {
	opt := TxnStateOption{Single: false, ReadOnly: false, isRawKV: false}
	tx, err := s.createTxn(ctx, opt)
	if err != nil {
		return nil, err
	}
	s.PrepareTSFuture(ctx, tx.(*TxnState))
	s.PrepareTxnCtx(ctx, tx.(*TxnState))
	if err := s.FetchTxn(tx.(*TxnState), true); err != nil {
		return nil, err
	}
	return tx.(*TxnState), nil
}

func (s *session) dispatchMutation(ctx context.Context, req rpc.MutationRequest, tx *TxnState) error {
	var (
		err    error
		ztable *tables.Table
	)
	for _, m := range req.GetMutations() {
		switch op := m.Operation.(type) {
		default:
			return fmt.Errorf("unsupported mutation operation type %T", op)
		case *tspb.Mutation_Insert:
			insert := op.Insert
			ztable, err = s.prepareZettaTable(ctx, insert.Table)
			if err != nil {
				break
			}
			if err = ztable.Insert(ctx, insert.KeySet, insert.Family, insert.Columns, insert.Values); err != nil {
				logutil.Logger(ctx).Error("mutation insert error", zap.Error(err))
				break
			}
		case *tspb.Mutation_Delete_:
			del := op.Delete
			ztable, err = s.prepareZettaTable(ctx, del.Table)
			if err != nil {
				break
			}
			if err := ztable.Delete(ctx, del.GetKeySet(), del.Family, del.Columns); err != nil {
				logutil.Logger(ctx).Error("mutation delete error", zap.Error(err))
				break
			}
		case *tspb.Mutation_InsertOrUpdate:
			iou := op.InsertOrUpdate
			ztable, err = s.prepareZettaTable(ctx, iou.Table)
			if err != nil {
				break
			}
			if err = ztable.InsertOrUpdate(ctx, iou.KeySet, iou.Family, iou.Columns, iou.Values); err != nil {
				logutil.Logger(ctx).Error("mutation insert-or-update error", zap.Error(err))
				break
			}

		case *tspb.Mutation_Update:
			up := op.Update
			ztable, err = s.prepareZettaTable(ctx, up.Table)
			if err != nil {
				break
			}
			if err = ztable.Update(ctx, up.KeySet, up.Family, up.Columns, up.Values); err != nil {
				logutil.Logger(ctx).Error("mutation update error", zap.Error(err))
				return err
			}
		case *tspb.Mutation_Replace:
			replace := op.Replace

			ztable, err = s.prepareZettaTable(ctx, replace.Table)
			if err != nil {
				break
			}
			if err = ztable.Replace(ctx, replace.KeySet, replace.Family, replace.Columns, replace.Values); err != nil {
				logutil.Logger(ctx).Error("mutation replace error", zap.Error(err))
				break
			}
		}
	}
	if err != nil {
		if er := tx.Rollback(); er != nil {
			logutil.BgLogger().Error("rollback error", zap.Error(er))
		}
		s.dropTxn(tx.ID)
		return err
	}
	return nil
}

func (s *session) prefetchTable(ctx context.Context, tableDict map[string]*tables.Table, table string, txn kv.Transaction) (*tables.Table, error) {
	var err error
	ztable, ok := tableDict[table]
	if ok {
		return ztable, nil
	}
	ztable, err = s.prepareZettaTable(ctx, table)
	if err != nil {
		return nil, err
	}
	tableDict[table] = ztable

	return ztable, nil
}

func (s *session) buildZettaTable(ctx context.Context, table string, txn kv.Transaction) (*tables.Table, error) {
	domain := domain.GetOnlyDomain()
	tableMeta, err := domain.InfoSchema().GetTableMetaByName(s.database, table)
	if err != nil {
		logutil.Logger(ctx).Error("tableMeta fetch error", zap.Error(err))
		return nil, err
	}
	tt := tables.TableFromMeta(tableMeta, txn)
	ztable := tt.(*tables.Table)
	return ztable, nil
}

func (s *session) prepareZettaTable(ctx context.Context, table string) (*tables.Table, error) {
	domain := domain.GetOnlyDomain()
	tt, err := domain.InfoSchema().GetTableByName(s.database, table)
	if err != nil {
		logutil.Logger(ctx).Error("prepare zetta table info error", zap.Error(err))
		return nil, err
	}
	// tt.SetTxn(txn)
	ztable, ok := tt.(*tables.Table)
	if !ok {
		err = fmt.Errorf("invalid table type (%T): %#v", tt, tt)
		logutil.Logger(ctx).Error(err.Error())
		return nil, err
	}
	return ztable, nil
}

func (s *session) doMutationCommit(ctx context.Context, req rpc.MutationRequest, txn kv.Transaction) (time.Time, error) {

	tx := txn.(*TxnState)
	if mr, ok := req.(*tspb.MutationRequest); ok {
		if mr.Transaction.GetId() != nil && len(mr.Transaction.GetId()) > 0 {
			return time.Time{}, nil
		}
	}
	defer tx.cleanup()
	if ctx.Err() != nil {
		logutil.BgLogger().Error("context error", zap.Error(ctx.Err()))
		return time.Time{}, nil
	}
	if !txn.Valid() {
		logutil.BgLogger().Error("txn invalid")
		return time.Time{}, nil
	}
	if _, err := tx.Flush(); err != nil {
		return time.Time{}, err
	}

	err := tx.Commit(ctx)
	if err != nil {
		flag := kv.IsTxnRetryableError(err)
		logutil.BgLogger().Error(fmt.Sprintf("tx commit error %+v", tx), zap.Error(err), zap.Bool("retryable", flag))
		if tx.Valid() {
			if er := tx.Rollback(); er != nil {
				logutil.BgLogger().Error("rollback error", zap.Error(er))
			}
		}
		return time.Time{}, err
	}
	s.dropTxn(tx.ID)
	return time.Now(), nil
}

func (s *session) readTx(ctx context.Context, tsel *tspb.TransactionSelector, rtopt *RetrieveTxnOpt) (tx *TxnState, err error) {

	singleUse := func(readOnly bool) (*TxnState, error) {
		opt := TxnStateOption{Single: true, ReadOnly: readOnly, isRawKV: rtopt.IsRawKV}
		tx, err := s.createTxn(ctx, opt)
		txs := tx.(*TxnState)
		if txs.TxnOpt.isRawKV {
			s.PrepareRawkvTxn(ctx, txs)
		} else {
			s.PrepareTSFuture(ctx, txs)
		}
		return txs, err
	}

	singleUseReadOnly := func() (*TxnState, error) {
		return singleUse(true)
	}

	beginReadWrite := func(readOnly bool) (*TxnState, error) {
		opt := TxnStateOption{Single: false, ReadOnly: readOnly, isRawKV: rtopt.IsRawKV}
		tx, err := s.createTxn(ctx, opt)
		if err != nil {
			return nil, err
		}
		s.insertTxn(tx.(*TxnState))
		return tx.(*TxnState), nil
	}

	beginReadOnly := func() (*TxnState, error) {
		return beginReadWrite(true)
	}

	switch sel := tsel.Selector.(type) {
	default:
		return nil, fmt.Errorf("TransactionSelector type %T not supported", sel)
	case *tspb.TransactionSelector_SingleUse:
		switch mode := sel.SingleUse.Mode.(type) {
		case *tspb.TransactionOptions_ReadOnly_:
			return singleUseReadOnly()
		case *tspb.TransactionOptions_ReadWrite_:
			return singleUse(false)
		default:
			return nil, fmt.Errorf("single use transaction in mode %T not supported", mode)
		}
	case *tspb.TransactionSelector_Begin:
		switch mode := sel.Begin.Mode.(type) {
		case *tspb.TransactionOptions_ReadOnly_:
			return beginReadOnly()
		case *tspb.TransactionOptions_ReadWrite_:
			return beginReadWrite(false)
		default:
			return nil, fmt.Errorf("single use transaction in mode %T not supported", mode)
		}
	case *tspb.TransactionSelector_Id:
		txnid := sel.Id
		pop := rtopt.Committable
		tx, err := s.searchTxn(string(txnid), pop)
		if err != nil {
			return nil, err
		}

		if rtopt != nil {
			return nil, status.Error(codes.Aborted, "retrieve transaction option should be specific")
		}
		if !rtopt.IsRawKV {
			s.PrepareTSFuture(ctx, tx)
			s.PrepareTxnCtx(ctx, tx)
		} else {
			s.PrepareRawkvTxn(ctx, tx)
		}
		return tx, nil
	}
}

func (s *session) searchTxn(tid string, pop bool) (*TxnState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	txn, ok := s.txns[tid]
	if ok {
		if pop {
			delete(s.txns, tid)
		}
		return txn, nil
	}
	return nil, status.Errorf(codes.NotFound, "no such transaction %v in session %v", tid, s.name)
}

func (s *session) RawkvAccess(ctx context.Context, table string) (bool, error) {
	ztable, err := s.prepareZettaTable(ctx, table)
	if err != nil {
		return false, err
	}
	accessMode, ok := ztable.Meta().Attributes["AccessMode"]
	if !ok {
		return false, nil
	}
	if accessMode == "rawkv" {
		return true, nil
	}
	return false, nil
}
