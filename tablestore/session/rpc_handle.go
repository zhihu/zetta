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

	"github.com/zhihu/zetta/tablestore/table/tables"
)

func (s *session) HandleRead(ctx context.Context, req rpc.ReadRequest, txn kv.Transaction) (RecordSet, error) {
	var (
		ri  RecordSet
		err error
	)

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
	tx := txn.(*TxnState)
	if tx.ReadOnly {
		return nil, status.Errorf(codes.Aborted, "session:%s tx:%s read only", s.name, tx.ID)
	}
	ctx = sctx.SetTxnCtx(ctx, tx)
	for _, m := range req.GetMutations() {
		switch op := m.Operation.(type) {
		default:
			return nil, fmt.Errorf("unsupported mutation operation type %T", op)
		case *tspb.Mutation_Insert:
			insert := op.Insert
			ztable, err := s.prepareZettaTable(ctx, insert.Table)
			if err != nil {
				return nil, err
			}
			if err := ztable.Insert(ctx, insert.KeySet, insert.Family, insert.Columns, insert.Values); err != nil {
				logutil.Logger(ctx).Error("mutation insert error", zap.Error(err))
				return nil, err
			}
		case *tspb.Mutation_Delete_:
			del := op.Delete
			ztable, err := s.prepareZettaTable(ctx, del.Table)

			if err != nil {
				return nil, err
			}
			if err := ztable.Delete(ctx, del.GetKeySet(), del.Family, del.Columns); err != nil {
				logutil.Logger(ctx).Error("mutation delete error", zap.Error(err))
				return nil, err
			}
		case *tspb.Mutation_InsertOrUpdate:
			iou := op.InsertOrUpdate
			ztable, err := s.prepareZettaTable(ctx, iou.Table)
			if err != nil {
				return nil, err
			}

			if err := ztable.InsertOrUpdate(ctx, iou.KeySet, iou.Family, iou.Columns, iou.Values); err != nil {
				logutil.Logger(ctx).Error("mutation insert-or-update error", zap.Error(err))
				return nil, err
			}

		case *tspb.Mutation_Update:
			up := op.Update
			ztable, err := s.prepareZettaTable(ctx, up.Table)
			if err != nil {
				return nil, err
			}
			if err := ztable.Update(ctx, up.KeySet, up.Family, up.Columns, up.Values); err != nil {
				logutil.Logger(ctx).Error("mutation update error", zap.Error(err))
				return nil, err
			}
		case *tspb.Mutation_Replace:
			replace := op.Replace

			ztable, err := s.prepareZettaTable(ctx, replace.Table)
			if err != nil {
				return nil, err
			}
			if err := ztable.Replace(ctx, replace.KeySet, replace.Family, replace.Columns, replace.Values); err != nil {
				logutil.Logger(ctx).Error("mutation replace error", zap.Error(err))
				return nil, err
			}
		}
	}

	ct, err := s.doMutationCommit(ctx, req, tx)
	if err != nil {
		logutil.Logger(ctx).Error("txn commit error", zap.Error(err))
		return nil, status.Errorf(codes.Aborted, "transaction commit error %v", err)
	}

	return &rpc.CommitTimeResp{CommitTs: ct}, nil

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
	ztable := tt.(*tables.Table)
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
	err := kv.WalkMemBuffer(tx.buf, func(k kv.Key, v []byte) error {
		if len(v) == 0 {
			return tx.Transaction.Delete(k)
		}
		return tx.Transaction.Set(k, v)
	})

	if err != nil {
		tx.doNotCommit = err
	}
	err = tx.Commit(ctx)
	if err != nil {
		logutil.Logger(ctx).Error("commit error", zap.Error(err))
		if er := tx.Rollback(); er != nil {
			logutil.Logger(ctx).Error("rollback error", zap.Error(er))
		}
		return time.Time{}, err
	}
	return time.Now(), nil
}

func (s *session) readTx(ctx context.Context, tsel *tspb.TransactionSelector, committable bool) (tx *TxnState, err error) {

	singleUse := func(readOnly bool) (*TxnState, error) {
		tx, err := s.CreateTxn(ctx, true, readOnly)
		return tx.(*TxnState), err
	}

	singleUseReadOnly := func() (*TxnState, error) {
		return singleUse(true)
	}

	beginReadWrite := func(readOnly bool) (*TxnState, error) {
		tx, err := s.CreateTxn(ctx, false, readOnly)
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
		pop := committable
		tx, err := s.searchTxn(string(txnid), pop)
		if err != nil {
			return nil, err
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
