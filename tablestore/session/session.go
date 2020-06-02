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
	"crypto/rand"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"

	tspb "github.com/zhihu/zetta-proto/pkg/tablestore"
	"github.com/zhihu/zetta/pkg/metrics"
	"github.com/zhihu/zetta/tablestore/domain"
	"github.com/zhihu/zetta/tablestore/rpc"
	"github.com/zhihu/zetta/tablestore/sessionctx"
	"github.com/zhihu/zetta/tablestore/sessionctx/variable"
)

var (
	sessionCount                     = metrics.SessionCounter.WithLabelValues(metrics.LblGeneral)
	transactionDurationGeneralCommit = metrics.TransactionDuration.WithLabelValues(metrics.LblGeneral, metrics.LblCommit)
	transactionDurationGeneralAbort  = metrics.TransactionDuration.WithLabelValues(metrics.LblGeneral, metrics.LblAbort)

	transactionCounterGeneralOK  = metrics.TransactionCounter.WithLabelValues(metrics.LblGeneral, metrics.LblOK)
	transactionCounterGeneralErr = metrics.TransactionCounter.WithLabelValues(metrics.LblGeneral, metrics.LblError)

	transactionCounterGeneralCommitRollback = metrics.TransactionCounter.WithLabelValues(metrics.LblGeneral, metrics.LblComRol)
	transactionRollbackCounterGeneral       = metrics.TransactionCounter.WithLabelValues(metrics.LblGeneral, metrics.LblRollback)
)

type Session interface {
	sessionctx.Context

	Close()

	SetLabels(map[string]string)
	Labels() map[string]string

	HandleRead(context.Context, rpc.ReadRequest, kv.Transaction) (RecordSet, error)
	HandleMutate(context.Context, rpc.MutationRequest, kv.Transaction) (rpc.Response, error)

	RetrieveTxn(context.Context, *tspb.TransactionSelector, bool) (*TxnState, error)

	CommitTxn(context.Context, kv.Transaction) error
	RollbackTxn(context.Context, kv.Transaction) error

	SetDB(db string)
	GetName() string
	ToProto() *tspb.Session

	SetLastActive(time.Time)
	LastActive() time.Time
	Active() bool
}

var (
	_         Session = (*session)(nil)
	SessCount int32   = 0
)

type session struct {
	name     string
	creation time.Time

	txn         TxnState
	sessionVars *variable.SessionVars
	cancel      func()
	mu          struct {
		sync.RWMutex
		values map[fmt.Stringer]interface{}
	}

	lastUse time.Time
	labels  map[string]string

	database string

	store kv.Storage
	// shared coprocessor client per session
	client kv.Client

	txns map[string]*TxnState

	lastActive time.Time
	active     bool
}

func CreateSession(store kv.Storage) (Session, error) {
	se, err := createSession(store)
	if err != nil {
		return nil, err
	}

	return se, nil
}

func createSession(store kv.Storage) (*session, error) {
	s := &session{
		name:        genRandomSession(),
		store:       store,
		sessionVars: variable.NewSessionVars(),
		txns:        make(map[string]*TxnState),
		creation:    time.Now(),
		client:      store.GetClient(),
		lastActive:  time.Now(),
	}
	s.mu.values = make(map[fmt.Stringer]interface{})
	sessionCount.Add(1)
	atomic.AddInt32(&SessCount, 1)
	return s, nil
}

func (s *session) GetName() string {
	return s.name
}

func (s *session) ToProto() *tspb.Session {
	s.mu.Lock()
	defer s.mu.Unlock()
	spb := &tspb.Session{
		Name:                   s.name,
		CreateTime:             rpc.TimestampProto(s.creation),
		ApproximateLastUseTime: rpc.TimestampProto(s.lastUse),
	}
	return spb
}

func (s *session) GetStore() kv.Storage {
	return s.store
}

func (s *session) SetDB(db string) {
	s.database = db
}

func (s *session) CreateTxn(ctx context.Context, single bool, readOnly bool) (kv.Transaction, error) {
	txn, err := NewTxnState(s)
	if err != nil {
		logutil.Logger(ctx).Error("create txnState error", zap.Error(err), zap.String("session", s.name))
		return nil, err
	}
	txn.Single = single
	txn.ReadOnly = readOnly
	return txn, nil
}

func (s *session) CommitTxn(ctx context.Context, txn kv.Transaction) error {
	return txn.Commit(ctx)
}

func (s *session) RollbackTxn(ctx context.Context, txn kv.Transaction) error {
	return txn.Rollback()
}

func (s *session) removeTxn(ctx context.Context, txn *TxnState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.txns, txn.ID)
	return nil
}

func (s *session) Close() {
	sessionCount.Dec()
	atomic.AddInt32(&SessCount, -1)
}

func (s *session) SetValue(key fmt.Stringer, value interface{}) {
	s.mu.Lock()
	s.mu.values[key] = value
	s.mu.Unlock()
}

func (s *session) Value(key fmt.Stringer) interface{} {
	s.mu.RLock()
	value := s.mu.values[key]
	s.mu.RUnlock()
	return value
}

func (s *session) ClearValue(key fmt.Stringer) {
	s.mu.Lock()
	delete(s.mu.values, key)
	s.mu.Unlock()
}

func (s *session) Labels() map[string]string {
	return s.labels
}

func (s *session) SetLabels(labels map[string]string) {
	s.labels = labels
}

func (s *session) GetClient() kv.Client {
	return s.store.GetClient()
}

func (s *session) GetSessionVars() *variable.SessionVars {
	return s.sessionVars
}

func (s *session) RetrieveTxn(ctx context.Context, sel *tspb.TransactionSelector, commitable bool) (*TxnState, error) {
	txn, err := s.readTx(ctx, sel, commitable)
	return txn, err
}

func (s *session) SetLastActive(t time.Time) {
	s.lastActive = t
}

func (s *session) LastActive() time.Time {
	return s.lastActive
}

func (s *session) Active() bool {
	return s.active
}

func (s *session) InitTxnWithStartTS(startTS uint64) error {
	if s.txn.Valid() {
		return nil
	}
	// no need to get txn from txnFutureCh since txn should init with startTs
	txn, err := s.store.BeginWithStartTS(startTS)
	if err != nil {
		return err
	}
	s.txn.changeInvalidToValid(txn)
	s.txn.SetCap(s.getMembufCap())
	return nil
}

func (s *session) NewTxn(ctx context.Context) error {
	if s.txn.Valid() {
		txnID := s.txn.StartTS()
		err := s.CommitTxn(ctx, &s.txn)
		if err != nil {
			return err
		}
		vars := s.GetSessionVars()
		logutil.Logger(ctx).Info("NewTxn() inside a transaction auto commit",
			zap.Int64("schemaVersion", vars.TxnCtx.SchemaVersion),
			zap.Uint64("txnStartTS", txnID))
	}

	txn, err := s.store.Begin()
	if err != nil {
		return err
	}
	txn.SetCap(s.getMembufCap())
	txn.SetVars(s.sessionVars.KVVars)
	s.txn.changeInvalidToValid(txn)
	is := domain.GetOnlyDomain().InfoSchema()
	s.sessionVars.TxnCtx = &variable.TransactionContext{
		InfoSchema:    is,
		SchemaVersion: is.SchemaMetaVersion(),
		CreateTime:    time.Now(),
		StartTS:       txn.StartTS(),
	}
	return nil
}

func (s *session) newTxn(ctx context.Context) (*TxnState, error) {
	txn := &TxnState{
		buf: kv.NewMemDbBuffer(kv.DefaultTxnMembufCap),
	}
	kvtxn, err := s.store.Begin()
	if err != nil {
		return nil, err
	}
	txn.Transaction = kvtxn
	txn.SetCap(s.getMembufCap())
	txn.SetVars(s.sessionVars.KVVars)
	s.txn.changeInvalidToValid(txn)
	is := domain.GetOnlyDomain().InfoSchema()
	s.sessionVars.TxnCtx = &variable.TransactionContext{
		InfoSchema:    is,
		SchemaVersion: is.SchemaMetaVersion(),
		CreateTime:    time.Now(),
		StartTS:       txn.StartTS(),
	}
	return txn, nil
}

func (s *session) Txn(active bool) (kv.Transaction, error) {
	if s.txn.pending() && active {
		// Transaction is lazy initialized.
		// PrepareTxnCtx is called to get a tso future, makes s.txn a pending txn,
		// If Txn() is called later, wait for the future to get a valid txn.
		txnCap := s.getMembufCap()
		if err := s.txn.changePendingToValid(txnCap); err != nil {
			logutil.Logger(context.Background()).Error("active transaction fail",
				zap.Error(err))
			s.txn.cleanup()
			s.sessionVars.TxnCtx.StartTS = 0
			return &s.txn, err
		}
		s.sessionVars.TxnCtx.StartTS = s.txn.StartTS()
		if s.sessionVars.TxnCtx.IsPessimistic {
			s.txn.SetOption(kv.Pessimistic, true)
		}
		// if !s.sessionVars.IsAutocommit() {
		// 	s.sessionVars.SetStatusFlag(mysql.ServerStatusInTrans, true)
		// }
	}
	return &s.txn, nil
}

func (s *session) insertTxn(txn *TxnState) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.txns[txn.ID] = txn
}

func (s *session) dropTxn(txnId string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.txns, txnId)
}

func (s *session) getMembufCap() int {
	if s.sessionVars.LightningMode {
		return kv.ImportingTxnMembufCap
	}

	return kv.DefaultTxnMembufCap
}

func genRandomSession() string {
	var b [8]byte
	rand.Read(b[:])
	return fmt.Sprintf("%x", b)
}
