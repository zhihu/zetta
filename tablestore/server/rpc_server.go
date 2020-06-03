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

package server

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	emptypb "github.com/gogo/protobuf/types"

	"github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_opentracing "github.com/grpc-ecosystem/go-grpc-middleware/tracing/opentracing"
	"github.com/grpc-ecosystem/go-grpc-prometheus"

	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/mock"

	tspb "github.com/zhihu/zetta-proto/pkg/tablestore"
	"github.com/zhihu/zetta/pkg/metrics"
	"github.com/zhihu/zetta/pkg/model"
	"github.com/zhihu/zetta/tablestore/domain"
	"github.com/zhihu/zetta/tablestore/rpc"
	"github.com/zhihu/zetta/tablestore/session"
)

var (
	grpcMetrics = grpc_prometheus.NewServerMetrics()

	createSessionGerneralOK = metrics.CreateSessionCounter.WithLabelValues(metrics.LblGeneral, metrics.LblOK)
	deleteSessionGerneralOK = metrics.DeleteSessionCounter.WithLabelValues(metrics.LblGeneral, metrics.LblOK)

	readCounterGerneralOK        = metrics.ReadCounter.WithLabelValues(metrics.LblGeneral, metrics.LblOK)
	readCounterGerneralErr       = metrics.ReadCounter.WithLabelValues(metrics.LblGeneral, metrics.LblError)
	streamReadCounterGerneralOK  = metrics.StreamReadCounter.WithLabelValues(metrics.LblGeneral, metrics.LblOK)
	streamReadCounterGerneralErr = metrics.StreamReadCounter.WithLabelValues(metrics.LblGeneral, metrics.LblError)
	sparseReadCounterGerneralOK  = metrics.SparseReadCounter.WithLabelValues(metrics.LblGeneral, metrics.LblOK)
	sparseReadCounterGerneralErr = metrics.SparseReadCounter.WithLabelValues(metrics.LblGeneral, metrics.LblError)

	commitCounterGerneralOK  = metrics.CommitCounter.WithLabelValues(metrics.LblGeneral, metrics.LblOK)
	commitCounterGerneralErr = metrics.CommitCounter.WithLabelValues(metrics.LblGeneral, metrics.LblError)

	mutateCounterGerneralOK  = metrics.MutateCounter.WithLabelValues(metrics.LblGeneral, metrics.LblOK)
	mutateCounterGerneralErr = metrics.MutateCounter.WithLabelValues(metrics.LblGeneral, metrics.LblError)

	executeMutateDurationGeneralOK  = metrics.ExecuteMutateDuration.WithLabelValues(metrics.LblGeneral, metrics.LblOK)
	executeMutateDurationGeneralErr = metrics.ExecuteMutateDuration.WithLabelValues(metrics.LblGeneral, metrics.LblError)
	executeReadDurationGeneralOK    = metrics.ExecuteReadDuration.WithLabelValues(metrics.LblGeneral, metrics.LblOK)
	executeReadDurationGeneralErr   = metrics.ExecuteReadDuration.WithLabelValues(metrics.LblGeneral, metrics.LblError)
)

// RPCServer implements tablestore gRPC server
type RPCServer struct {
	s   *Server
	rpc *grpc.Server
	mu  sync.Mutex

	tspb.TablestoreAdminServer
	tspb.TablestoreServer
}

// NewRPCServer xxx
func NewRPCServer(s *Server) *RPCServer {
	rs := &RPCServer{s: s}

	unaryInterceptor := grpc_middleware.ChainUnaryServer(
		grpc_prometheus.UnaryServerInterceptor,
		grpc_opentracing.UnaryServerInterceptor(),
	)
	streamInterceptor := grpc_middleware.ChainStreamServer(
		grpc_prometheus.StreamServerInterceptor,
		grpc_opentracing.StreamServerInterceptor(),
	)

	rs.rpc = grpc.NewServer(
		grpc.StreamInterceptor(streamInterceptor),
		grpc.UnaryInterceptor(unaryInterceptor),
	)
	tspb.RegisterTablestoreAdminServer(rs.rpc, rs)
	tspb.RegisterTablestoreServer(rs.rpc, rs)
	return rs
}

func (rs *RPCServer) Run() {
	go rs.rpc.Serve(rs.s.listener)
}

func (rs *RPCServer) Close() {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	rs.rpc.GracefulStop()
}

// CreateSession Creates a new session.
func (rs *RPCServer) CreateSession(ctx context.Context, req *tspb.CreateSessionRequest) (*tspb.Session, error) {
	if req.Database == "" {
		return nil, status.Errorf(codes.InvalidArgument, "database null")
	}
	is := domain.GetOnlyDomain().InfoSchema()
	if _, ok := is.GetDatabaseMetaByName(req.Database); !ok {
		return nil, status.Errorf(codes.NotFound, "no such database %s", req.Database)
	}

	queryID := atomic.AddUint32(&baseQueryID, 1)
	rs.s.rwlock.Lock()
	defer rs.s.rwlock.Unlock()
	if len(rs.s.queryCtxs)+1 > rs.s.cfg.MaxSessions {
		return nil, status.Errorf(codes.ResourceExhausted, "session count exceed max-sesions %v", rs.s.cfg.MaxSessions)
	}
	qctx, err := rs.s.driver.OpenCtx(uint64(queryID), req.Database)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "open query ctx error %v", err)
	}

	if req.Session != nil {
		qctx.GetSession().SetLabels(req.Session.Labels)
	}

	rs.s.queryCtxs[qctx.GetSession().GetName()] = qctx

	createSessionGerneralOK.Add(1)
	return qctx.GetSession().ToProto(), nil
}

// BatchCreateSessions creates multiple new sessions.
func (rs *RPCServer) BatchCreateSessions(ctx context.Context, req *tspb.BatchCreateSessionsRequest) (*tspb.BatchCreateSessionsResponse, error) {
	if req.Database == "" {
		return nil, status.Errorf(codes.InvalidArgument, "database null")
	}
	is := domain.GetOnlyDomain().InfoSchema()
	if _, ok := is.GetDatabaseMetaByName(req.Database); !ok {
		return nil, status.Errorf(codes.NotFound, "no such database %s", req.Database)
	}

	if req.SessionTemplate == nil {
		return nil, status.Errorf(codes.InvalidArgument, "session template is required")
	}
	var sessions []*tspb.Session

	rs.s.rwlock.Lock()
	defer rs.s.rwlock.Unlock()
	if len(rs.s.queryCtxs)+int(req.GetSessionCount()) > rs.s.cfg.MaxSessions {
		return nil, status.Errorf(codes.ResourceExhausted, "session count exceed max-sesions %v", rs.s.cfg.MaxSessions)
	}

	for i := int32(0); i < req.GetSessionCount(); i++ {
		queryID := atomic.AddUint32(&baseQueryID, 1)
		qctx, err := rs.s.driver.OpenCtx(uint64(queryID), req.Database)
		if err != nil {
			return nil, status.Errorf(codes.Aborted, "create session error %v", err)
		}
		qctx.GetSession().SetLabels(req.SessionTemplate.Labels)
		rs.s.queryCtxs[qctx.GetSession().GetName()] = qctx
		sessions = append(sessions, qctx.GetSession().ToProto())
		createSessionGerneralOK.Add(1)
	}
	return &tspb.BatchCreateSessionsResponse{Session: sessions}, nil
}

// GetSession gets a session. Returns NOT_FOUND if the session does not exist.
// This is mainly useful for determining whether a session is still alive.
func (rs *RPCServer) GetSession(ctx context.Context, req *tspb.GetSessionRequest) (*tspb.Session, error) {
	rs.s.rwlock.RLock()
	defer rs.s.rwlock.RUnlock()
	queryCtx, ok := rs.s.queryCtxs[req.Name]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "session %s not found", req.Name)
	}
	session := queryCtx.GetSession()
	session.SetLastActive(time.Now())
	return session.ToProto(), nil
}

// ListSessions lists all sessions in a given database.
func (rs *RPCServer) ListSessions(ctx context.Context, req *tspb.ListSessionsRequest) (*tspb.ListSessionsResponse, error) {
	rs.s.rwlock.RLock()
	defer rs.s.rwlock.RUnlock()
	sessions := []*tspb.Session{}
	for _, qctx := range rs.s.queryCtxs {
		sess := qctx.GetSession().ToProto()
		sessions = append(sessions, sess)
	}
	rsp := &tspb.ListSessionsResponse{
		Sessions: sessions,
	}
	return rsp, nil
}

// DeleteSession ends a session, releasing server resources associated with it. This will
// asynchronously trigger cancellation of any operations that are running with this session.
func (rs *RPCServer) DeleteSession(ctx context.Context, req *tspb.DeleteSessionRequest) (*emptypb.Empty, error) {
	rs.s.rwlock.Lock()
	defer rs.s.rwlock.Unlock()
	queryCtx, ok := rs.s.queryCtxs[req.Name]
	if !ok {
		return &emptypb.Empty{}, nil
	}
	if err := queryCtx.Close(); err != nil {
		logutil.Logger(ctx).Error("session close error", zap.Error(err), zap.String("session", req.Name))
		return nil, status.Errorf(codes.Aborted, fmt.Sprintf("session %s close error, try later", req.Name))
	}
	delete(rs.s.queryCtxs, req.Name)
	deleteSessionGerneralOK.Add(1)
	return &emptypb.Empty{}, nil
}

// Read reads rows from the database using key lookups and scans, as a
// simple key/value style alternative to
func (rs *RPCServer) Read(ctx context.Context, req *tspb.ReadRequest) (*tspb.ResultSet, error) {
	queryCtx, err := rs.getQueryCtxBySessionID(req.Session)
	if err != nil {
		readCounterGerneralErr.Inc()
		return nil, err
	}
	if len(req.ResumeToken) > 0 {
		// This should only happen if we send resume_token ourselves.
		return nil, fmt.Errorf("read resumption not supported")
	}
	if len(req.PartitionToken) > 0 {
		return nil, fmt.Errorf("partition restrictions not supported")
	}

	if req.GetTransaction() == nil {
		readCounterGerneralErr.Inc()
		return nil, status.Error(codes.FailedPrecondition, "no transaction selector in grpc request")
	}
	sess := queryCtx.GetSession()
	sess.SetLastActive(time.Now())
	txn, err := sess.RetrieveTxn(ctx, req.GetTransaction(), false)
	if err != nil {
		readCounterGerneralErr.Inc()
		logutil.Logger(ctx).Error("fetch transaction error", zap.Error(err))
		return nil, err
	}
	startTS := time.Now()
	ri, err := queryCtx.GetSession().HandleRead(ctx, req, txn)
	if err != nil {
		readCounterGerneralErr.Inc()
		logutil.Logger(ctx).Error("read error", zap.Error(err))
		return nil, err
	}
	resultSet, err := rs.readRows(ctx, ri)
	durRead := time.Since(startTS)
	if err != nil {
		executeReadDurationGeneralErr.Observe(durRead.Seconds())
		sparseReadCounterGerneralErr.Inc()
		return nil, err
	}
	executeReadDurationGeneralOK.Observe(durRead.Seconds())
	readCounterGerneralOK.Inc()
	return resultSet, nil

}

func (rs *RPCServer) readRows(ctx context.Context, ri session.RecordSet) (*tspb.ResultSet, error) {
	resultSet := &tspb.ResultSet{
		Metadata: buildResultSetMetaData(ri.Columns()),
		Rows:     []*tspb.ListValue{},
	}
	for {
		row, err := ri.Next(ctx)
		if err != nil {
			break
		}
		// resultSet.Rows = append(resultSet.Rows, row.(*tspb.ListValue))
		resultSet.SliceRows = append(resultSet.SliceRows, row.(*tspb.SliceCell))
	}
	if ri.LastErr() != nil {
		return nil, ri.LastErr()
	}
	return resultSet, nil
}

// SparseRead reads rows from the database using key lookups and scans, as a
// simple key/value style alternative to
func (rs *RPCServer) SparseRead(ctx context.Context, req *tspb.SparseReadRequest) (*tspb.ResultSet, error) {
	queryCtx, err := rs.getQueryCtxBySessionID(req.Session)
	if err != nil {
		sparseReadCounterGerneralErr.Inc()
		return nil, err
	}
	if len(req.ResumeToken) > 0 {
		// This should only happen if we send resume_token ourselves.
		return nil, fmt.Errorf("read resumption not supported")
	}
	if len(req.PartitionToken) > 0 {
		return nil, fmt.Errorf("partition restrictions not supported")
	}

	if req.GetTransaction() == nil {
		sparseReadCounterGerneralErr.Inc()
		return nil, status.Error(codes.FailedPrecondition, "no transaction selector in grpc request")
	}
	sess := queryCtx.GetSession()
	sess.SetLastActive(time.Now())
	txn, err := sess.RetrieveTxn(ctx, req.GetTransaction(), false)
	if err != nil {
		sparseReadCounterGerneralErr.Inc()
		logutil.Logger(ctx).Error("fetch transaction error", zap.Error(err))
		return nil, err
	}
	startTS := time.Now()
	ri, err := queryCtx.GetSession().HandleRead(ctx, req, txn)
	if err != nil {
		sparseReadCounterGerneralErr.Inc()
		logutil.Logger(ctx).Error("read error", zap.Error(err))
		return nil, err
	}
	resultSet, err := rs.readRows(ctx, ri)
	durRead := time.Since(startTS)
	if err != nil {
		executeReadDurationGeneralErr.Observe(durRead.Seconds())
		sparseReadCounterGerneralErr.Inc()
		return resultSet, err
	}
	executeReadDurationGeneralOK.Observe(durRead.Seconds())
	sparseReadCounterGerneralOK.Inc()
	return resultSet, nil
}

// Mutate mutate request
func (rs *RPCServer) Mutate(ctx context.Context, req *tspb.MutationRequest) (*tspb.MutationResponse, error) {

	queryCtx, err := rs.getQueryCtxBySessionID(req.Session)
	if err != nil {
		mutateCounterGerneralErr.Inc()
		return nil, err
	}
	sess := queryCtx.GetSession()
	sess.SetLastActive(time.Now())
	if req.GetTransaction() == nil {
		return nil, status.Errorf(codes.FailedPrecondition, "transaction selector should be specific")
	}
	txn, err := sess.RetrieveTxn(ctx, req.GetTransaction(), false)
	if err != nil {
		mutateCounterGerneralErr.Inc()
		return nil, status.Errorf(codes.Aborted, "retrieve session transaction failed %v", err)
	}
	startTS := time.Now()
	r, err := sess.HandleMutate(ctx, req, txn)
	durMutate := time.Since(startTS)
	if err != nil {
		executeMutateDurationGeneralErr.Observe(durMutate.Seconds())
		mutateCounterGerneralErr.Inc()
		return nil, err
	}

	executeMutateDurationGeneralOK.Observe(durMutate.Seconds())
	mutateCounterGerneralOK.Inc()
	return rpc.BuildMutationResponse(r), nil
}

// StreamingRead xxx
func (rs *RPCServer) StreamingRead(req *tspb.ReadRequest, stream tspb.Tablestore_StreamingReadServer) error {
	queryCtx, err := rs.getQueryCtxBySessionID(req.Session)
	if err != nil {
		streamReadCounterGerneralErr.Inc()
		return err
	}
	session := queryCtx.GetSession()
	session.SetLastActive(time.Now())
	txn, err := session.RetrieveTxn(stream.Context(), req.GetTransaction(), false)
	if err != nil {
		streamReadCounterGerneralErr.Inc()
		logutil.Logger(stream.Context()).Error("fetch transaction error", zap.Error(err))
		return err
	}
	ri, err := queryCtx.GetSession().HandleRead(stream.Context(), req, txn)
	if err != nil {
		streamReadCounterGerneralErr.Inc()
		logutil.Logger(stream.Context()).Error("mutate error", zap.Error(err))
		return status.Error(codes.Aborted, err.Error())
	}
	return rs.readStream(stream.Context(), ri, stream.Send)
}

func (rs *RPCServer) readStream(ctx context.Context, ri session.RecordSet, send func(*tspb.PartialResultSet) error) error {
	var (
		row interface{}
		err error
	)
	startTS := time.Now()
	rsm := buildResultSetMetaData(ri.Columns())
	for {
		row, err = ri.Next(ctx)
		if err != nil {
			break
		}
		prs := &tspb.PartialResultSet{
			Metadata: rsm,

			RowCells: row.(*tspb.SliceCell),
		}
		if err = send(prs); err != nil {
			break
		}
		// ResultSetMetadata is only set for the first PartialResultSet.
		rsm = nil
	}
	durRead := time.Since(startTS)

	if ri.LastErr() != nil || err != nil {
		streamReadCounterGerneralErr.Inc()
		executeReadDurationGeneralErr.Observe(durRead.Seconds())
		if ri.LastErr() != nil {
			return ri.LastErr()
		}
		return err
	}
	executeReadDurationGeneralOK.Observe(durRead.Seconds())
	streamReadCounterGerneralOK.Inc()
	return nil
}

// BeginTransaction begins a new transaction. This step can often be skipped:
func (rs *RPCServer) BeginTransaction(ctx context.Context, req *tspb.BeginTransactionRequest) (*tspb.Transaction, error) {
	queryCtx, err := rs.getQueryCtxBySessionID(req.GetSession())
	if err != nil {
		return nil, err
	}
	session := queryCtx.GetSession()
	session.SetLastActive(time.Now())
	txnSel := &tspb.TransactionSelector{
		Selector: &tspb.TransactionSelector_Begin{
			Begin: req.GetOptions(),
		},
	}
	txn, err := session.RetrieveTxn(ctx, txnSel, false)
	if err != nil {
		logutil.Logger(ctx).Error("session retrieve transaction error", zap.Error(err), zap.String("session", req.Session))
		return nil, status.Errorf(codes.Internal, "begin transaction error %v", err)
	}
	transaction := &tspb.Transaction{
		Id: []byte(txn.ID),
	}
	return transaction, nil
}

// Commit commits a transaction. The request includes the mutations to be
// applied to rows in the database.
//
// `Commit` might return an `ABORTED` error. This can occur at any time;
// commonly, the cause is conflicts with concurrent
// transactions. However, it can also happen for a variety of other
// reasons. If `Commit` returns `ABORTED`, the caller should re-attempt
// the transaction from the beginning, re-using the same session.
func (rs *RPCServer) Commit(ctx context.Context, req *tspb.CommitRequest) (*tspb.CommitResponse, error) {
	queryCtx, err := rs.getQueryCtxBySessionID(req.Session)
	if err != nil {
		commitCounterGerneralErr.Inc()
		return nil, err
	}
	sess := queryCtx.GetSession()
	sess.SetLastActive(time.Now())
	txnSel := &tspb.TransactionSelector{}
	switch req.Transaction.(type) {
	case *tspb.CommitRequest_SingleUseTransaction:
		txnSel.Selector = &tspb.TransactionSelector_SingleUse{
			SingleUse: req.GetSingleUseTransaction(),
		}
	case *tspb.CommitRequest_TransactionId:
		txnSel.Selector = &tspb.TransactionSelector_Id{
			Id: req.GetTransactionId(),
		}
	default:
		return nil, status.Errorf(codes.Aborted, "unsupported transaction type %T", req.Transaction)
	}
	txn, err := sess.RetrieveTxn(ctx, txnSel, true)
	if err != nil {
		commitCounterGerneralErr.Inc()
		return nil, status.Errorf(codes.Aborted, "retrieve session transaction failed %v", err)
	}
	startTS := time.Now()
	r, err := sess.HandleMutate(ctx, req, txn)
	durMutate := time.Since(startTS)
	if err != nil {
		executeMutateDurationGeneralErr.Observe(durMutate.Seconds())
		commitCounterGerneralErr.Inc()
		return nil, err
	}
	executeMutateDurationGeneralOK.Observe(durMutate.Seconds())
	commitCounterGerneralOK.Inc()
	return rpc.BuildCommitResponse(r), nil
}

// Rollback xxx
func (rs *RPCServer) Rollback(ctx context.Context, req *tspb.RollbackRequest) (*emptypb.Empty, error) {
	queryCtx, err := rs.getQueryCtxBySessionID(req.Session)
	if err != nil {
		return nil, err
	}
	sess := queryCtx.GetSession()
	sess.SetLastActive(time.Now())
	txnSel := &tspb.TransactionSelector{
		Selector: &tspb.TransactionSelector_Id{Id: req.TransactionId},
	}

	txn, err := sess.RetrieveTxn(ctx, txnSel, true)
	if err != nil {
		return nil, status.Errorf(codes.Aborted, "retrieve session transaction failed %v", err)
	}
	if err := sess.RollbackTxn(ctx, txn); err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

func (rs *RPCServer) getQueryCtxBySessionID(id string) (QueryCtx, error) {
	rs.s.rwlock.RLock()
	defer rs.s.rwlock.RUnlock()
	queryCtx, ok := rs.s.queryCtxs[id]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "session %s not found", id)
	}
	return queryCtx, nil
}

// CreateDatabase xxx
func (rs *RPCServer) CreateDatabase(ctx context.Context, req *tspb.CreateDatabaseRequest) (*tspb.CreateDatabaseResponse, error) {
	do := domain.GetOnlyDomain()
	dbMeta := model.NewDatabaseMetaFromPbReq(req)
	sctx := mock.NewContext() // Use mock to provide a default session context.
	err := do.DDL().CreateSchema(sctx, dbMeta)
	if err != nil {
		return nil, status.Errorf(codes.Internal, err.Error())
	}
	return &tspb.CreateDatabaseResponse{}, nil
}

// DeleteDatabase xxx
func (rs *RPCServer) DeleteDatabase(ctx context.Context, req *tspb.DeleteDatabaseRequest) (*tspb.DeleteDatabaseResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method DeleteDatabase not implemented")
}

func (rs *RPCServer) ListDatabase(ctx context.Context, req *tspb.ListDatabaseRequest) (*tspb.ListDatabaseResponse, error) {
	dbMetas := make([]*tspb.DatabaseMeta, 0)
	do := domain.GetOnlyDomain()
	dbs := do.InfoSchema().ListDatabases()
	for _, d := range dbs {
		dbMetas = append(dbMetas, &d.DatabaseMeta)
	}
	return &tspb.ListDatabaseResponse{
		Databases: dbMetas,
	}, nil
}

// CreateTable xxx
func (rs *RPCServer) CreateTable(ctx context.Context, req *tspb.CreateTableRequest) (*tspb.CreateTableResponse, error) {
	do := domain.GetOnlyDomain()
	tbMeta := model.NewTableMetaFromPbReq(req)
	fmt.Println(tbMeta)
	sctx := mock.NewContext() // Use mock to provide a default session context.
	err := do.DDL().CreateTable(sctx, tbMeta)
	if err != nil {
		return nil, status.Errorf(codes.Internal, err.Error())
	}
	return &tspb.CreateTableResponse{}, nil
}

func (rs *RPCServer) AddColumn(ctx context.Context, req *tspb.AddColumnRequest) (*tspb.AddColumnResponse, error) {
	do := domain.GetOnlyDomain()
	sctx := mock.NewContext()
	err := do.DDL().AddColumn(sctx, req)
	if err != nil {
		return nil, status.Errorf(codes.Internal, err.Error())
	}
	return &tspb.AddColumnResponse{}, nil
}

func (rs *RPCServer) ListTables(ctx context.Context,
	req *tspb.ListTableByDatabaseRequest) (*tspb.ListTableByDatabaseResponse, error) {
	do := domain.GetOnlyDomain()
	database := req.Database
	tbls := do.InfoSchema().ListTablesByDatabase(database)
	res := &tspb.ListTableByDatabaseResponse{}
	for _, t := range tbls {
		pbTable := t.TableMeta
		columns := make([]*tspb.ColumnMeta, 0)
		columnFamilies := make([]*tspb.ColumnFamilyMeta, 0)
		for _, c := range t.Columns {
			columns = append(columns, &c.ColumnMeta)
		}
		for _, cf := range t.ColumnFamilies {
			columnFamilies = append(columnFamilies, &cf.ColumnFamilyMeta)
		}
		pbTable.Columns = columns
		pbTable.ColumnFamilies = columnFamilies
		res.Tables = append(res.Tables, &pbTable)
	}
	return res, nil
}

// DropTable xxx
func (rs *RPCServer) DropTable(ctx context.Context, req *tspb.DropTableRequest) (*tspb.DropTableResponse, error) {
	do := domain.GetOnlyDomain()
	sctx := mock.NewContext()
	err := do.DDL().DropTable(sctx, req.Database, req.Table)
	if err != nil {
		return nil, status.Errorf(codes.Internal, err.Error())
	}
	return &tspb.DropTableResponse{}, nil
}

// AlterTable xxx
func (rs *RPCServer) AlterTable(ctx context.Context, req *tspb.AlterTableRequest) (*tspb.AlterTableResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method AlterTable not implemented")
}

// CreateIndex xxx
func (rs *RPCServer) CreateIndex(ctx context.Context, req *tspb.CreateIndexRequest) (*tspb.CreateIndexResponse, error) {
	do := domain.GetOnlyDomain()
	indexMeta := model.NewIndexMetaFromPbReq(req)
	sctx := mock.NewContext()
	err := do.DDL().CreateIndex(sctx, req.Database, req.Table, indexMeta)
	if err != nil {
		return nil, status.Errorf(codes.Internal, err.Error())
	}
	return &tspb.CreateIndexResponse{}, nil
}

// DropIndex xxx
func (rs *RPCServer) DropIndex(ctx context.Context, req *tspb.DropIndexRequest) (*tspb.DropIndexResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method DropIndex not implemented")
}

func zettaResultFromResult(x interface{}) (*tspb.ResultSet, error) {
	r := &tspb.ResultSet{
		Metadata: &tspb.ResultSetMetadata{},
		Rows:     []*tspb.ListValue{},
	}
	return r, nil
}

func buildResultSetMetaData(columns []*tspb.ColumnMeta) *tspb.ResultSetMetadata {
	rowType := &tspb.StructType{
		Fields: make([]*tspb.StructType_Field, len(columns)),
	}
	for i, col := range columns {
		rowType.Fields[i] = &tspb.StructType_Field{Name: col.Name, Type: col.ColumnType}
	}
	return &tspb.ResultSetMetadata{RowType: rowType}
}
