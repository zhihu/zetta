package metrics

import "github.com/prometheus/client_golang/prometheus"

var (
	QueryTotalCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "zetta",
			Subsystem: "grpc",
			Name:      "query_total",
			Help:      "Counter of queries.",
		}, []string{LblType, LblResult})

	CreateSessionCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "zetta",
			Subsystem: "grpc",
			Name:      "create_session_total",
			Help:      "Counter of create session api.",
		}, []string{LblGRPCType, LblType})

	DeleteSessionCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "zetta",
			Subsystem: "grpc",
			Name:      "delete_session_total",
			Help:      "Counter of delete session api.",
		}, []string{LblGRPCType, LblType})

	ExecuteErrorCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "zetta",
			Subsystem: "grpc",
			Name:      "execute_error_total",
			Help:      "Counter of execute errors.",
		}, []string{LblGRPCType, LblType})

	ReadCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "zetta",
			Subsystem: "grpc",
			Name:      "read_op_total",
			Help:      "Counter of read api.",
		}, []string{LblGRPCType, LblType})

	SparseReadCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "zetta",
			Subsystem: "grpc",
			Name:      "sparse_read_op_total",
			Help:      "Counter of sparse-read api.",
		}, []string{LblGRPCType, LblType})

	StreamReadCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "zetta",
			Subsystem: "grpc",
			Name:      "stream_read_op_total",
			Help:      "Counter of stream read api.",
		}, []string{LblGRPCType, LblType})

	MutateCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "zetta",
			Subsystem: "grpc",
			Name:      "mutate_op_total",
			Help:      "Counter of mutate api.",
		}, []string{LblGRPCType, LblType})

	CommitCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "zetta",
			Subsystem: "grpc",
			Name:      "commit_op_total",
			Help:      "Counter of commit api errors.",
		}, []string{LblGRPCType, LblType})

	ExecuteReadDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "zetta",
			Subsystem: "grpc",
			Name:      "execute_read_duration_seconds",
			Help:      "Bucketed histogram of processing time (s) in running read executor.",
			Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 22), // 100us ~ 419s
		}, []string{LblGRPCType, LblType})

	ExecuteMutateDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "zetta",
			Subsystem: "grpc",
			Name:      "execute_mutate_duration_seconds",
			Help:      "Bucketed histogram of processing time (s) in running mutate executor.",
			Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 22), // 100us ~ 419s
		}, []string{LblGRPCType, LblType})
)
