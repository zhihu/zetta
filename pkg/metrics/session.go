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

import "github.com/prometheus/client_golang/prometheus"

var (
	SessionRetry = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "zetta",
			Subsystem: "session",
			Name:      "retry_num",
			Help:      "Bucketed histogram of session retry count.",
			Buckets:   prometheus.LinearBuckets(0, 1, 20), // 0 ~ 20
		})
	SessionRetryErrorCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "zetta",
			Subsystem: "session",
			Name:      "retry_error_total",
			Help:      "Counter of session retry error.",
		}, []string{LblSQLType, LblType})

	SessionCounter = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "zetta",
			Subsystem: "session",
			Name:      "session_num",
			Help:      "num of sessions ",
		}, []string{LblType})

	TransactionCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "zetta",
			Subsystem: "session",
			Name:      "transaction_total",
			Help:      "Counter of transactions.",
		}, []string{LblSQLType, LblType})

	TransactionDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "zetta",
			Subsystem: "session",
			Name:      "transaction_duration_seconds",
			Help:      "Bucketed histogram of a transaction execution duration, including retry.",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 20), // 1ms ~ 1049s
		}, []string{LblSQLType, LblType})

	SchemaLeaseErrorCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "zetta",
			Subsystem: "session",
			Name:      "schema_lease_error_total",
			Help:      "Counter of schema lease error",
		}, []string{LblType})
)
