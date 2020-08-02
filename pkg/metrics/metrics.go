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

package metrics

import (
	"github.com/pingcap/tidb/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	// PanicCounter measures the count of panics.
	PanicCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "server",
			Name:      "panic_total",
			Help:      "Counter of panic.",
		}, []string{LblType})
)

// metrics labels.
const (
	LabelSession   = "session"
	LabelDomain    = "domain"
	LabelDDLOwner  = "ddl-owner"
	LabelDDL       = "ddl"
	LabelDDLSyncer = "ddl-syncer"
	LabelGCWorker  = "gcworker"
	LabelAnalyze   = "analyze"

	LabelBatchRecvLoop = "batch-recv-loop"
	LabelBatchSendLoop = "batch-send-loop"

	opSucc   = "ok"
	opFailed = "err"

	LableScope   = "scope"
	ScopeGlobal  = "global"
	ScopeSession = "session"
)

// RetLabel returns "ok" when err == nil and "err" when err != nil.
// This could be useful when you need to observe the operation result.
func RetLabel(err error) string {
	if err == nil {
		return opSucc
	}
	return opFailed
}

// RegisterMetrics registers the metrics which are ONLY used in TiDB server.
func RegisterMetrics() {

	prometheus.MustRegister(metrics.TiKVBackoffCounter)
	prometheus.MustRegister(metrics.TiKVBackoffHistogram)
	prometheus.MustRegister(metrics.TiKVCoprocessorHistogram)
	prometheus.MustRegister(metrics.TiKVLoadSafepointCounter)
	prometheus.MustRegister(metrics.TiKVLockResolverCounter)
	prometheus.MustRegister(metrics.TiKVRawkvCmdHistogram)
	prometheus.MustRegister(metrics.TiKVRawkvSizeHistogram)
	prometheus.MustRegister(metrics.TiKVRegionCacheCounter)
	prometheus.MustRegister(metrics.TiKVRegionErrorCounter)
	prometheus.MustRegister(metrics.TiKVSecondaryLockCleanupFailureCounter)
	prometheus.MustRegister(metrics.TiKVSendReqHistogram)
	prometheus.MustRegister(metrics.TiKVSnapshotCounter)
	prometheus.MustRegister(metrics.TiKVTxnCmdCounter)
	prometheus.MustRegister(metrics.TiKVTxnCmdHistogram)
	prometheus.MustRegister(metrics.TiKVTxnCounter)
	prometheus.MustRegister(metrics.TiKVTxnRegionsNumHistogram)
	prometheus.MustRegister(metrics.TiKVTxnWriteKVCountHistogram)
	prometheus.MustRegister(metrics.TiKVTxnWriteSizeHistogram)
	prometheus.MustRegister(metrics.TiKVLocalLatchWaitTimeHistogram)
	prometheus.MustRegister(metrics.TiKVPendingBatchRequests)
	prometheus.MustRegister(metrics.TiKVBatchWaitDuration)
	prometheus.MustRegister(metrics.TiKVBatchClientUnavailable)
	prometheus.MustRegister(metrics.TiKVRangeTaskStats)
	prometheus.MustRegister(metrics.TiKVRangeTaskPushDuration)
	prometheus.MustRegister(metrics.TiKVTxnHeartBeatHistogram)

	prometheus.MustRegister(TSFutureWaitDuration)

	// grpc metrics
	prometheus.MustRegister(CreateSessionCounter)
	prometheus.MustRegister(DeleteSessionCounter)
	prometheus.MustRegister(ReadCounter)
	prometheus.MustRegister(SparseReadCounter)
	prometheus.MustRegister(StreamReadCounter)
	prometheus.MustRegister(CommitCounter)
	prometheus.MustRegister(MutateCounter)
	prometheus.MustRegister(ExecuteMutateDuration)
	prometheus.MustRegister(ExecuteReadDuration)

	// session metrics
	prometheus.MustRegister(SessionRetry)
	prometheus.MustRegister(SessionCounter)
	prometheus.MustRegister(SessionRetryErrorCounter)
	prometheus.MustRegister(TransactionCounter)
	prometheus.MustRegister(TransactionDuration)
	prometheus.MustRegister(SchemaLeaseErrorCounter)

	//tables metrics
	prometheus.MustRegister(FetchRowsCounter)
	prometheus.MustRegister(FetchSparseCounter)
	prometheus.MustRegister(FetchRowsDuration)
	prometheus.MustRegister(FetchSparseDuration)

	prometheus.MustRegister(ScanSparseCounter)
	prometheus.MustRegister(ScanSparseDuration)

	prometheus.MustRegister(BatchSparseCounter)
	prometheus.MustRegister(BatchSparseDuration)
}

// Label constants.
const (
	LblUnretryable = "unretryable"
	LblReachMax    = "reach_max"
	LblOK          = "ok"
	LblError       = "error"
	LblCommit      = "commit"
	LblAbort       = "abort"
	LblRollback    = "rollback"
	LblComRol      = "com_rol"
	LblType        = "type"
	LblDb          = "db"
	LblResult      = "result"
	LblSQLType     = "sql_type"
	LblGeneral     = "general"
	LblInternal    = "internal"
	LblStore       = "store"
	LblAddress     = "address"

	LblGRPCType = "grpc_type"
)
