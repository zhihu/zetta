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

// Copyright 2018 PingCAP, Inc.

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
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/zhihu/zetta/tablestore/config"
	"go.uber.org/zap"
)

var (
	_ kv.Transaction = (*TxnState)(nil)
)

// TxnStateOption Zetta TxnState Option
type TxnStateOption struct {
	ReadOnly bool
	Single   bool
	isRawKV  bool
}

// TxnState wraps kv.Transaction to provide a new kv.Transaction.
// 1. It holds all statement related modification in the buffer before flush to the txn,
// so if execute statement meets error, the txn won't be made dirty.
// 2. It's a lazy transaction, that means it's a txnFuture before StartTS() is really need.
type TxnState struct {
	// States of a TxnState should be one of the followings:
	// Invalid: kv.Transaction == nil && txnFuture == nil
	// Pending: kv.Transaction == nil && txnFuture != nil
	// Valid:	kv.Transaction != nil && txnFuture == nil
	ID string

	kv.Transaction
	txnFuture *txnFuture

	stmtBuf kv.MemBuffer

	TxnOpt      TxnStateOption
	doNotCommit error
	mu          sync.Mutex
	inProgress  atomic.Value

	session *session
}

func (st *TxnState) init() {
}

func (st *TxnState) initStmtBuf() {
	if st.stmtBuf == nil {
		st.stmtBuf = st.Transaction.NewStagingBuffer()
	}
}

func (st *TxnState) stmtBufLen() int {
	if st.stmtBuf == nil {
		return 0
	}
	return st.stmtBuf.Len()
}

func (st *TxnState) stmtBufSize() int {
	if st.stmtBuf == nil {
		return 0
	}
	return st.stmtBuf.Size()
}

func (st *TxnState) stmtBufGet(ctx context.Context, k kv.Key) ([]byte, error) {
	if st.stmtBuf == nil {
		return nil, kv.ErrNotExist
	}
	return st.stmtBuf.Get(ctx, k)
}

// Size implements the MemBuffer interface.
func (st *TxnState) Size() int {
	size := st.stmtBufSize()
	if st.Transaction != nil {
		size += st.Transaction.Size()
	}
	return size
}

// Valid implements the kv.Transaction interface.
func (st *TxnState) Valid() bool {
	return st.Transaction != nil && st.Transaction.Valid()
}

func (st *TxnState) pending() bool {
	return st.Transaction == nil && st.txnFuture != nil
}

func (st *TxnState) validOrPending() bool {
	return st.txnFuture != nil || st.Valid()
}

func (st *TxnState) String() string {
	if st.Transaction != nil {
		return st.Transaction.String()
	}
	if st.txnFuture != nil {
		return "txnFuture"
	}
	return "invalid transaction"
}

// GoString implements the "%#v" format for fmt.Printf.
func (st *TxnState) GoString() string {
	var s strings.Builder
	s.WriteString("Txn{")
	if st.pending() {
		s.WriteString("state=pending")
	} else if st.Valid() {
		s.WriteString("state=valid")
		fmt.Fprintf(&s, ", txnStartTS=%d", st.Transaction.StartTS())
		// if len(st.dirtyTableOP) > 0 {
		// 	fmt.Fprintf(&s, ", len(dirtyTable)=%d, %#v", len(st.dirtyTableOP), st.dirtyTableOP)
		// }
		// if len(st.mutations) > 0 {
		// 	fmt.Fprintf(&s, ", len(mutations)=%d, %#v", len(st.mutations), st.mutations)
		// }
		if st.stmtBuf.Len() != 0 {
			fmt.Fprintf(&s, ", buf.length: %d, buf.size: %d", st.stmtBufLen(), st.stmtBufSize())
		}
	} else {
		s.WriteString("state=invalid")
	}

	s.WriteString("}")
	return s.String()
}

func (st *TxnState) changeInvalidToValid(txn kv.Transaction) {
	st.Transaction = txn
	st.txnFuture = nil
}

func (st *TxnState) changeInvalidToPending(future *txnFuture) {
	st.Transaction = nil
	st.txnFuture = future
}

func (st *TxnState) changePendingToValid() error {
	if st.txnFuture == nil {
		return errors.New("transaction future is not set")
	}
	future := st.txnFuture
	st.txnFuture = nil
	txn, err := future.wait()
	if err != nil {
		st.Transaction = nil
		return err
	}
	st.Transaction = txn
	return nil
}

func (st *TxnState) changeToInvalid() {
	st.Transaction = nil
	st.txnFuture = nil
}

// Commit overrides the Transaction interface.
func (st *TxnState) Commit(ctx context.Context) error {
	defer st.reset()
	if st.doNotCommit != nil {
		if err1 := st.Transaction.Rollback(); err1 != nil {
			logutil.Logger(context.Background()).Error("rollback error", zap.Error(err1))
		}
		return errors.Trace(st.doNotCommit)
	}
	if err := st.Transaction.Commit(ctx); err != nil {
		logutil.BgLogger().Error("commit error", zap.Error(err))
		return err
	}
	return nil

}

// Flush flushes all staging kvs into parent buffer.
func (st *TxnState) Flush() (int, error) {
	if st.stmtBuf == nil {
		return 0, nil
	}
	return st.stmtBuf.Flush()
}

// Rollback overrides the Transaction interface.
func (st *TxnState) Rollback() error {
	defer st.reset()
	logutil.BgLogger().Info(fmt.Sprintf("transaction %v, st: %+v", st.Transaction, st))
	return st.Transaction.Rollback()
}

func (st *TxnState) reset() {
	st.doNotCommit = nil
	st.cleanup()
	st.changeToInvalid()
}

// Get overrides the Transaction interface.
func (st *TxnState) Get(ctx context.Context, k kv.Key) ([]byte, error) {
	val, err := st.stmtBufGet(ctx, k)
	if kv.IsErrNotFound(err) {
		val, err = st.Transaction.Get(ctx, k)
		if kv.IsErrNotFound(err) {
			return nil, err
		}
	}
	if err != nil {
		return nil, err
	}
	if len(val) == 0 {
		return nil, kv.ErrNotExist
	}
	return val, nil
}

// GetMemBuffer overrides the Transaction interface.
func (st *TxnState) GetMemBuffer() kv.MemBuffer {
	if st.stmtBuf == nil || st.stmtBuf.Size() == 0 {
		return st.Transaction.GetMemBuffer()
	}
	return kv.NewBufferStoreFrom(st.Transaction.GetMemBuffer(), st.stmtBuf)
}

// BatchGet overrides the Transaction interface.
func (st *TxnState) BatchGet(ctx context.Context, keys []kv.Key) (map[string][]byte, error) {
	bufferValues := make([][]byte, len(keys))
	shrinkKeys := make([]kv.Key, 0, len(keys))
	for i, key := range keys {
		val, err := st.stmtBufGet(ctx, key)
		if kv.IsErrNotFound(err) {
			shrinkKeys = append(shrinkKeys, key)
			continue
		}
		if err != nil {
			return nil, err
		}
		if len(val) != 0 {
			bufferValues[i] = val
		}
	}
	storageValues, err := st.Transaction.BatchGet(ctx, shrinkKeys)
	if err != nil {
		return nil, err
	}
	for i, key := range keys {
		if bufferValues[i] == nil {
			continue
		}
		storageValues[string(key)] = bufferValues[i]
	}
	return storageValues, nil
}

// Set overrides the Transaction interface.
func (st *TxnState) Set(k kv.Key, v []byte) error {
	st.initStmtBuf()
	return st.stmtBuf.Set(k, v)
}

// Delete overrides the Transaction interface.
func (st *TxnState) Delete(k kv.Key) error {
	st.initStmtBuf()
	return st.stmtBuf.Delete(k)
}

// Iter overrides the Transaction interface.
func (st *TxnState) Iter(k kv.Key, upperBound kv.Key) (kv.Iterator, error) {
	retrieverIt, err := st.Transaction.Iter(k, upperBound)
	if err != nil {
		return nil, err
	}
	if st.stmtBuf == nil {
		return retrieverIt, nil
	}
	bufferIt, err := st.stmtBuf.Iter(k, upperBound)
	if err != nil {
		return nil, err
	}

	return kv.NewUnionIter(bufferIt, retrieverIt, false)
}

// IterReverse overrides the Transaction interface.
func (st *TxnState) IterReverse(k kv.Key) (kv.Iterator, error) {
	retrieverIt, err := st.Transaction.IterReverse(k)
	if err != nil {
		return nil, err
	}
	if st.stmtBuf == nil {
		return retrieverIt, nil
	}
	bufferIt, err := st.stmtBuf.IterReverse(k)
	if err != nil {
		return nil, err
	}
	return kv.NewUnionIter(bufferIt, retrieverIt, true)
}

func (st *TxnState) cleanup() {
	if st.stmtBuf != nil {
		st.stmtBuf.Discard()
		st.stmtBuf = nil
	}
}

// KeysNeedToLock returns the keys need to be locked.
func (st *TxnState) KeysNeedToLock() ([]kv.Key, error) {
	keys := make([]kv.Key, 0, st.stmtBuf.Len())
	if err := kv.WalkMemBuffer(st.stmtBuf, func(k kv.Key, v []byte) error {
		if !keyNeedToLock(k, v) {
			return nil
		}
		// If the key is already locked, it will be deduplicated in LockKeys method later.
		// The statement MemBuffer will be reused, so we must copy the key here.
		keys = append(keys, append([]byte{}, k...))
		return nil
	}); err != nil {
		return nil, err
	}
	return keys, nil
}

func (st *TxnState) checkInProgress() error {
	if st.inProgress.Load().(*transactionInProgressKey) != nil {
		return fmt.Errorf("zetta does not support nested transactions ")
	}
	return nil
}

func (st *TxnState) setInProgress() {
	st.inProgress.Store(&transactionInProgressKey{})
}

func keyNeedToLock(k, v []byte) bool {
	isTableKey := bytes.HasPrefix(k, tablecodec.TablePrefix())
	if !isTableKey {
		// meta key always need to lock.
		return true
	}
	isDelete := len(v) == 0
	if isDelete {
		// only need to delete row key.
		return k[10] == 'r'
	}
	isNonUniqueIndex := len(v) == 1 && v[0] == '0'
	// Put row key and unique index need to lock.
	return !isNonUniqueIndex
}

// txnFuture is a promise, which promises to return a txn in future.
type txnFuture struct {
	future oracle.Future
	store  kv.Storage

	mockFail bool
}

func (tf *txnFuture) wait() (kv.Transaction, error) {
	startTS, err := tf.future.Wait()
	if err == nil {
		logutil.BgLogger().Debug("wait startTS", zap.Uint64("startTs", startTS))
		return tf.store.BeginWithStartTS(startTS)
	} else if config.GetGlobalConfig().Store == "mocktikv" {
		return nil, err
	}
	logutil.BgLogger().Warn("wait tso failed", zap.Error(err))
	// It would retry get timestamp.
	return tf.store.Begin()
}

func (s *session) getTxnFuture(ctx context.Context) *txnFuture {
	// if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
	// 	span1 := span.Tracer().StartSpan("session.getTxnFuture", opentracing.ChildOf(span.Context()))
	// 	defer span1.Finish()
	// }

	oracleStore := s.store.GetOracle()
	var tsFuture oracle.Future
	if s.sessionVars.LowResolutionTSO {
		tsFuture = oracleStore.GetLowResolutionTimestampAsync(ctx)
	} else {
		tsFuture = oracleStore.GetTimestampAsync(ctx)
	}
	ret := &txnFuture{future: tsFuture, store: s.store}
	return ret
}

func NewTxnState(sess *session) (*TxnState, error) {
	txn := &TxnState{
		ID:      genRandomTransaction(sess.name),
		session: sess,
	}
	txn.init()
	return txn, nil
}

func genRandomTransaction(session string) string {
	var b [6]byte
	rand.Read(b[:])
	return fmt.Sprintf("tx-%s-%x", session, b)
}

type transactionInProgressKey struct{}

func checkNestedTxn(ctx context.Context) error {
	if ctx.Value(transactionInProgressKey{}) != nil {
		return fmt.Errorf("zetta does not support nested transactions ")
	}
	return nil
}
