package server

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	parser_model "github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/zhihu/zetta/pkg/metrics"
	"github.com/zhihu/zetta/tablestore/config"
	"github.com/zhihu/zetta/tablestore/domain"
	"github.com/zhihu/zetta/tablestore/mysql/executor"
	"github.com/zhihu/zetta/tablestore/mysql/sctx"
	"github.com/zhihu/zetta/tablestore/mysql/sqlexec"
	"go.uber.org/zap"

	"github.com/pingcap/tidb/owner"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/kvcache"
	"github.com/pingcap/tidb/util/memory"
	"github.com/pingcap/tipb/go-binlog"
)

var (
	commitCounterGerneralOK        = metrics.CommitCounter.WithLabelValues(metrics.LblGeneral, metrics.LblOK)
	executeMutateDurationGeneralOK = metrics.ExecuteMutateDuration.WithLabelValues(metrics.LblGeneral, metrics.LblOK)
)

type Session interface {
	sctx.Context
	Status() uint16 // Flag of current status, such as autocommit.
	ExecuteStmt(context.Context, ast.StmtNode) (sqlexec.RecordSet, error)
	Execute(context.Context, string) (sqlexec.RecordSet, error)
	Parse(ctx context.Context, sql string) ([]ast.StmtNode, error)
	String() string // String is used to debug.
	CommitTxn(context.Context) error
	RollbackTxn(context.Context)
	SetConnectionID(uint64)
	SetCollation(coID int) error
	Close()
	// FieldList returns fields list of a table.
	FieldList(tableName string) (fields []*ast.ResultField, err error)
	AffectedRows() uint64

	GetHistorySQL() string
}

type baseSession struct{}

func (b *baseSession) Status() uint16 {
	return 0
}

func (b *baseSession) AffectedRows() uint64 {
	return 0
}

func (b *baseSession) ExecuteStmt(ctx context.Context, node ast.StmtNode) (sqlexec.RecordSet, error) {
	return nil, errors.New("ExecuteStmt unimplemented")
}

func (b *baseSession) Execute(ctx context.Context, node ast.StmtNode) (sqlexec.RecordSet, error) {
	return nil, errors.New("Execute unimplemented")
}

func (b *baseSession) GetStore() kv.Storage {
	return nil
}

func (b *baseSession) Parse(ctx context.Context, sql string) ([]ast.StmtNode, error) {
	return nil, errors.New("Parse unimplemented")
}

func (b *baseSession) String() string {
	return "baseSession"
}

func (b *baseSession) CommitTxn(ctx context.Context) error {
	return errors.New("CommitTxn unimplemented")
}

func (b *baseSession) CommitTxnWrapper(ctx context.Context) error {
	return errors.New("CommitTxn unimplemented")
}

func (b *baseSession) RollbackTxn(ctx context.Context) {}

func (b *baseSession) SetConnectionID(id uint64) {}

func (b *baseSession) SetCollation(coID int) error {
	return errors.New("SetCollation unimplemented")
}

func (b *baseSession) Close() {}

func (b *baseSession) FieldList(tableName string) (fields []*ast.ResultField, err error) {
	return nil, errors.New("FieldList unimplemented")
}

func (b *baseSession) NewTxn(ctx context.Context) error {
	return errors.New("NewTxn unimplemented")
}

func (b *baseSession) Txn(active bool) (kv.Transaction, error) {
	return nil, errors.New("Txn unimplemented")
}

func (b *baseSession) PrepareTSFuture(ctx context.Context) {}

// GetClient gets a kv.Client.
func (b *baseSession) GetClient() kv.Client { return nil }

// SetValue saves a value associated with this context for key.
func (b *baseSession) SetValue(key fmt.Stringer, value interface{}) {}

// Value returns the value associated with this context for key.
func (b *baseSession) Value(key fmt.Stringer) interface{} { return nil }

// ClearValue clears the value associated with this context for key.
func (b *baseSession) ClearValue(key fmt.Stringer) {}

func (b *baseSession) GetSessionVars() *variable.SessionVars { return nil }

func (b *baseSession) GetSessionManager() util.SessionManager { return nil }

// RefreshTxnCtx commits old transaction without retry,
// and creates a new transaction.
// now just for load data and batch insert.
func (b *baseSession) RefreshTxnCtx(context.Context) error { return nil }

// InitTxnWithStartTS initializes a transaction with startTS.
// It should be called right before we builds an executor.
func (b *baseSession) InitTxnWithStartTS(startTS uint64) error { return nil }

// PreparedPlanCache returns the cache of the physical plan
func (b *baseSession) PreparedPlanCache() *kvcache.SimpleLRUCache { return nil }

// StoreQueryFeedback stores the query feedback.
func (b *baseSession) StoreQueryFeedback(feedback interface{}) {}

// HasDirtyContent checks whether there's dirty update on the given table.
func (b *baseSession) HasDirtyContent(tid int64) bool { return false }

// StmtCommit flush all changes by the statement to the underlying transaction.
func (b *baseSession) StmtCommit(tracker *memory.Tracker) error { return nil }

// StmtRollback provides statement level rollback.
func (b *baseSession) StmtRollback() {}

// StmtGetMutation gets the binlog mutation for current statement.
func (b *baseSession) StmtGetMutation(int64) *binlog.TableMutation { return nil }

// StmtAddDirtyTableOP adds the dirty table operation for current statement.
func (b *baseSession) StmtAddDirtyTableOP(op int, physicalID int64, handle int64) {}

// DDLOwnerChecker returns owner.DDLOwnerChecker.
func (b *baseSession) DDLOwnerChecker() owner.DDLOwnerChecker { return nil }

// AddTableLock adds table lock to the session lock map.
func (b *baseSession) AddTableLock([]parser_model.TableLockTpInfo) {}

// ReleaseTableLocks releases table locks in the session lock map.
func (b *baseSession) ReleaseTableLocks(locks []parser_model.TableLockTpInfo) {}

// ReleaseTableLockByTableID releases table locks in the session lock map by table ID.
func (b *baseSession) ReleaseTableLockByTableIDs(tableIDs []int64) {}

// CheckTableLocked checks the table lock.
func (b *baseSession) CheckTableLocked(tblID int64) (bool, parser_model.TableLockType) {
	return false, parser_model.TableLockNone
}

// GetAllTableLocks gets all table locks table id and db id hold by the session.
func (b *baseSession) GetAllTableLocks() []parser_model.TableLockTpInfo { return nil }

// ReleaseAllTableLocks releases all table locks hold by the session.
func (b *baseSession) ReleaseAllTableLocks() {}

// HasLockedTables uses to check whether this session locked any tables.
func (b *baseSession) HasLockedTables() bool { return false }

type session struct {
	baseSession
	txn         TxnState
	sessionVars *variable.SessionVars
	store       kv.Storage
	parser      *parser.Parser

	sql string
	//Use for retry.
	history sqlexec.RecordSet
}

func CreateSession(store kv.Storage) (Session, error) {
	s := &session{
		store:       store,
		parser:      parser.New(),
		sessionVars: variable.NewSessionVars(),
	}
	s.sessionVars.GlobalVarsAccessor = s
	err := variable.SetSessionSystemVar(s.sessionVars, variable.MaxAllowedPacket, types.NewStringDatum("2143741824"))
	if err != nil {
		return nil, errors.Trace(err)
	}
	return s, nil
}

func (s *session) GetAllSysVars() (map[string]string, error) {
	m := make(map[string]string)
	for _, v := range variable.SysVars {
		m[v.Name] = v.Value
	}
	return m, nil
}

func (s *session) GetGlobalSysVar(name string) (string, error) {
	for _, v := range variable.SysVars {
		if v.Scope&variable.ScopeGlobal != 0 && v.Name == name {
			return v.Value, nil
		}
	}
	return "", errors.New("no value found")
}

func (s *session) SetGlobalSysVar(name string, value string) error {
	return nil
}

func (s *session) GetStore() kv.Storage {
	return s.store
}

func (s *session) SetCollation(coID int) error {
	return nil
}

func (s *session) Execute(ctx context.Context, sql string) (sqlexec.RecordSet, error) {
	stmts, err := s.Parse(ctx, sql)
	if err != nil {
		return nil, err
	}
	return s.ExecuteStmt(ctx, stmts[0])
}

func (s *session) Parse(ctx context.Context, sql string) ([]ast.StmtNode, error) {
	currentDB := s.sessionVars.CurrentDB
	if strings.HasPrefix(sql, "select") || strings.HasPrefix(sql, "SELECT") {
		if strings.Contains(sql, "(SELECT SCHEMA())") || strings.Contains(sql, "(select schema())") || strings.Contains(sql, "t.*") {
			sql = strings.ToLower(sql)
			sql = strings.Replace(sql, "(select schema())", "'"+currentDB+"'", -1)
			sql = strings.Replace(sql, "t.*", "*", -1)
		}
	}
	stmtNodes, _, err := s.parser.Parse(sql, "", "")
	return stmtNodes, err
}

func (s *session) finishStmt(ctx context.Context, meetsError error) error {
	var err error
	err = autoCommitAfterStmt(ctx, s, meetsError)
	if err != nil {
		return err
	}
	if s.txn.pending() {
		s.txn.changeToInvalid()
	}
	return nil
}

func autoCommitAfterStmt(ctx context.Context, se *session, meetsErr error) error {
	sessVars := se.sessionVars
	if meetsErr != nil {
		if !sessVars.InTxn() {
			logutil.BgLogger().Info("rollbackTxn for ddl/autocommit failed")
			se.RollbackTxn(ctx)
		}
		return meetsErr
	}

	if !sessVars.InTxn() {
		if err := se.CommitTxn(ctx); err != nil {
			return err
		}
		return nil
	}
	return nil
}

func (s *session) GetHistorySQL() string {
	return s.sql
}

func (s *session) AffectedRows() uint64 {
	return s.sessionVars.StmtCtx.AffectedRows()
}

func (s *session) ExecuteStmt(ctx context.Context, node ast.StmtNode) (sqlexec.RecordSet, error) {
	start := time.Now()
	defer func() {
		if !ast.IsReadOnly(node) {
			executeMutateDurationGeneralOK.Observe(time.Since(start).Seconds())
			commitCounterGerneralOK.Inc()
		}
	}()

	compiler := &executor.Compiler{
		Ctx: s,
	}
	//Default set autocommit = true
	s.sessionVars.SetStatusFlag(mysql.ServerStatusAutocommit, true)
	s.PrepareTSFuture(ctx)
	executorBuilder, err := compiler.Compile(ctx, node)
	if err != nil {
		return nil, err
	}
	rset, err := executorBuilder.Build(ctx)
	s.history = rset

	var rs sqlexec.RecordSet
	_, rs, err = s.handleNoDelay(ctx, rset)

	if !ast.IsReadOnly(node) {
		if s.txn.Valid() {
			if err != nil {
				s.StmtRollback()
			} else {
				err = s.StmtCommit(nil)
			}
		}
	}

	if rs != nil {
		return rs, err
	}
	err = s.finishStmt(ctx, err)

	return rs, err
}

func (s *session) StmtCommit(tracker *memory.Tracker) error {
	defer func() {
		s.txn.cleanup()
	}()
	if _, err := s.txn.Flush(); err != nil {
		return err
	}
	return nil
}

func (s *session) StmtRollback() {
	//Still in memory.
	s.txn.cleanup()
}

func (s *session) Close() {
	ctx := context.Background()
	s.RollbackTxn(ctx)
}

func (s *session) Status() uint16 {
	return s.sessionVars.Status
}

func (s *session) RollbackTxn(ctx context.Context) {
	if s.txn.Valid() {
		terror.Log(s.txn.Rollback())
	}
	s.txn.changeToInvalid()
	s.sessionVars.TxnCtx.Cleanup()
	s.sessionVars.SetStatusFlag(mysql.ServerStatusInTrans, false)
	s.sql = ""
}

func (s *session) handleNoDelay(ctx context.Context, rs sqlexec.RecordSet) (bool, sqlexec.RecordSet, error) {
	if len(rs.Fields()) == 0 {
		rs, err := s.handleNoDelayExecutor(ctx, rs)
		return true, rs, err
	}
	return false, rs, nil
}

func (s *session) handleNoDelayExecutor(ctx context.Context, rs sqlexec.RecordSet) (sqlexec.RecordSet, error) {
	chk := rs.NewChunk()
	err := rs.Next(ctx, chk)
	return nil, err
}

func (s *session) GetSessionVars() *variable.SessionVars {
	return s.sessionVars
}

// FieldList returns fields list of a table.
func (s *session) FieldList(tableName string) ([]*ast.ResultField, error) {
	is := domain.GetOnlyDomain().InfoSchema()
	dbName := parser_model.NewCIStr(s.GetSessionVars().CurrentDB)
	tName := parser_model.NewCIStr(tableName)
	table, err := is.GetTableByName(dbName.L, tName.L)
	if err != nil {
		return nil, err
	}

	cols := table.Cols()
	fields := make([]*ast.ResultField, 0, len(cols))
	for _, col := range cols {
		rf := &ast.ResultField{
			ColumnAsName: parser_model.NewCIStr(col.ColumnMeta.ColumnMeta.Name),
			TableAsName:  tName,
			DBName:       dbName,
			Table:        table.Meta().ToTableInfo(),
			Column:       col.ToColumnInfo(),
		}
		fields = append(fields, rf)
	}
	return fields, nil
}

// PrepareTSFuture uses to try to get ts future.
func (s *session) PrepareTSFuture(ctx context.Context) {
	sessVars := s.sessionVars
	if s.txn.Transaction != nil && !sessVars.InTxn() {
		s.txn.Rollback()
	}

	if !s.txn.validOrPending() {
		txnFuture := s.getTxnFuture(ctx)
		s.txn.changeInvalidToPending(txnFuture)
		return
	} else if !sessVars.InTxn() {
		txnFuture := s.getTxnFuture(ctx)
		s.txn.changeInvalidToPending(txnFuture)
		return
	}
}

func (s *session) getTxnFuture(ctx context.Context) *txnFuture {
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

func (s *session) CommitTxnWrapper(ctx context.Context) error {
	return s.CommitTxn(ctx)
}

func (s *session) CommitTxn(ctx context.Context) error {
	s.sql = ""
	return s.doCommit(ctx)
}

func (s *session) isTxnRetryableError(err error) bool {
	return kv.IsTxnRetryableError(err)
}

func (s *session) doCommit(ctx context.Context) error {
	if !s.txn.Valid() {
		return nil
	}
	defer s.txn.changeToInvalid()

	// TODO: Set option for 2 phase commit to validate schema lease.
	err := s.txn.Commit(ctx)
	if err != nil {
		if !s.isTxnRetryableError(err) {
			return err
		}
		// -1 means retry forever.
		return s.retry(ctx, -1)
	}
	return nil
}

func (s *session) retry(ctx context.Context, maxCnt int) error {
	var retryCnt uint
	for {
		s.PrepareTSFuture(ctx)
		_, _, err := s.handleNoDelay(ctx, s.history)
		if err != nil {
			return err
		}
		err = s.StmtCommit(nil)
		if err != nil {
			return err
		}
		err = s.txn.Commit(ctx)
		if err == nil {
			return nil
		}
		if !s.isTxnRetryableError(err) {
			return err
		}
		retryCnt++
		if maxCnt != -1 && int(retryCnt) >= maxCnt {
			return err
		}
		kv.BackOff(retryCnt)
	}
}

func (s *session) GetTxn(active bool, raw bool) (kv.Transaction, error) {
	if raw {
		if s.txn.Transaction == nil {
			rawTxn, err := s.store.BeginWithStartTS(0)
			if err != nil {
				return nil, err
			}
			s.txn = TxnState{
				Transaction: rawTxn,
			}
		}

		return &s.txn, nil
	}

	if !active {
		return &s.txn, nil
	}
	if !s.txn.validOrPending() {
		return &s.txn, errors.AddStack(kv.ErrInvalidTxn)
	}
	if s.txn.pending() {
		// Transaction is lazy initialized.
		// PrepareTxnCtx is called to get a tso future, makes s.txn a pending txn,
		// If Txn() is called later, wait for the future to get a valid txn.
		if err := s.txn.changePendingToValid(); err != nil {
			logutil.BgLogger().Error("active transaction fail",
				zap.Error(err))
			s.txn.cleanup()
			s.sessionVars.TxnCtx.StartTS = 0
			return &s.txn, err
		}
		s.sessionVars.TxnCtx.StartTS = s.txn.StartTS()
		if s.sessionVars.TxnCtx.IsPessimistic {
			s.txn.SetOption(kv.Pessimistic, true)
		}
		if !s.sessionVars.IsAutocommit() {
			s.sessionVars.SetStatusFlag(mysql.ServerStatusInTrans, true)
		}
	}
	return &s.txn, nil
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
	kv.Transaction
	txnFuture *txnFuture

	stmtBuf kv.MemBuffer

	// If doNotCommit is not nil, Commit() will not commit the transaction.
	// doNotCommit flag may be set when StmtCommit fail.
	doNotCommit error
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

// NewStagingBuffer returns a new child write buffer.
func (st *TxnState) NewStagingBuffer() kv.MemBuffer {
	st.initStmtBuf()
	return st.stmtBuf.NewStagingBuffer()
}

// Flush flushes all staging kvs into parent buffer.
func (st *TxnState) Flush() (int, error) {
	if st.stmtBuf == nil {
		return 0, nil
	}
	return st.stmtBuf.Flush()
}

// Discard discards all staging kvs.
func (st *TxnState) Discard() {
	if st.stmtBuf == nil {
		return
	}
	st.stmtBuf.Discard()
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
		if st.stmtBufLen() != 0 {
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
	st.stmtBuf = nil
	st.Transaction = nil
	st.txnFuture = nil
}

// Commit overrides the Transaction interface.
func (st *TxnState) Commit(ctx context.Context) error {
	defer st.reset()
	if st.stmtBufLen() != 0 {
		logutil.BgLogger().Error("the code should never run here",
			zap.String("TxnState", st.GoString()),
			zap.Stack("something must be wrong"))
		return errors.Trace(kv.ErrInvalidTxn)
	}
	if st.doNotCommit != nil {
		if err1 := st.Transaction.Rollback(); err1 != nil {
			logutil.BgLogger().Error("rollback error", zap.Error(err1))
		}
		return errors.Trace(st.doNotCommit)
	}

	return st.Transaction.Commit(ctx)
}

// Rollback overrides the Transaction interface.
func (st *TxnState) Rollback() error {
	defer st.reset()
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

// txnFuture is a promise, which promises to return a txn in future.
type txnFuture struct {
	future oracle.Future
	store  kv.Storage
}

func (tf *txnFuture) wait() (kv.Transaction, error) {
	startTS, err := tf.future.Wait()
	if err == nil {
		return tf.store.BeginWithStartTS(startTS)
	} else if config.GetGlobalConfig().Store == "unistore" {
		return nil, err
	}

	logutil.BgLogger().Warn("wait tso failed", zap.Error(err))
	// It would retry get timestamp.
	return tf.store.Begin()
}
