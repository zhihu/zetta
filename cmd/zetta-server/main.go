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

package main

import (
	"flag"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"github.com/ngaut/pools"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/parser/terror"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/push"
	"go.uber.org/zap"

	"github.com/pingcap/tidb/kv"

	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/signal"

	//"github.com/zhihu/zetta/pkg/store/mockstore"
	//"github.com/zhihu/zetta/pkg/store/tikv"

	kvstore "github.com/pingcap/tidb/store"

	opentracing "github.com/opentracing/opentracing-go"
	"github.com/zhihu/zetta/pkg/metrics"
	ts "github.com/zhihu/zetta/tablestore"
	"github.com/zhihu/zetta/tablestore/config"
	"github.com/zhihu/zetta/tablestore/ddl/gcworker"
	"github.com/zhihu/zetta/tablestore/domain"
	mysvr "github.com/zhihu/zetta/tablestore/mysql/server"
	"github.com/zhihu/zetta/tablestore/server"
	"github.com/zhihu/zetta/tablestore/zstore"
)

const (
	nmVersion          = "V"
	nmPort             = "P"
	nmLogLevel         = "L"
	nmLogFile          = "log-file"
	nmStore            = "store"
	nmStorePath        = "path"
	nmHost             = "host"
	nmAdvertiseAddress = "advertise-address"
	nmProtocol         = "protocol"

	nmReportStatus    = "report-status"
	nmStatusHost      = "status-host"
	nmStatusPort      = "status"
	nmMetricsAddr     = "metrics-addr"
	nmMetricsInterval = "metrics-interval"
	nmListenPort      = "port"
	nmListenSQLPort   = "sqlport"

	nmThrift          = "enable-thrift"
	nmThriftTransport = "thrift-transport"
	nmThriftProtocol  = "thrift-protocol"
	nmThriftPort      = "thrift-port"
)

var (
	version = flagBoolean(nmVersion, false, "print version information and exit")
	// Base
	storePath        = flag.String(nmStorePath, "/tmp/tidb", "tidb storage path")
	store            = flag.String(nmStore, "mocktikv", "registered store name, [tikv, mocktikv]")
	advertiseAddress = flag.String(nmAdvertiseAddress, "", "tidb server advertise IP")
	port             = flag.Uint(nmListenPort, 4000, "rpc server listening port")
	sqlPort          = flag.Uint(nmListenSQLPort, 4001, "sql server listening port")
	// Log
	logLevel = flag.String(nmLogLevel, "info", "log level: info, debug, warn, error, fatal")
	logFile  = flag.String(nmLogFile, "", "log file path")

	// Status
	reportStatus    = flagBoolean(nmReportStatus, true, "If enable status report HTTP service.")
	statusHost      = flag.String(nmStatusHost, "0.0.0.0", "tidb server status host")
	protocol        = flag.String(nmProtocol, "*", "server protocol")
	statusPort      = flag.String(nmStatusPort, "10090", "tidb server status port")
	metricsAddr     = flag.String(nmMetricsAddr, "", "prometheus pushgateway address, leaves it empty will disable prometheus push.")
	metricsInterval = flag.Uint(nmMetricsInterval, 15, "prometheus client push interval in second, set \"0\" to disable prometheus push.")

	thriftEnable = flagBoolean(nmThrift, true, "if enable hbase thrift server")
	tTransport   = flag.String(nmThriftTransport, "buffered", "HBase thrift server transport")
	tProtocol    = flag.String(nmThriftProtocol, "binary", "HBase thrift server protocol")
	thriftPort   = flag.Uint(nmThriftPort, 9090, "HBase thrift server listen port")
)
var (
	cfg      *config.Config
	storage  kv.Storage
	dom      *domain.Domain
	svr      *server.Server
	mysqlsvr *mysvr.Server
	graceful bool
)

func main() {
	flag.Parse()
	if *version {
		versionInfo := fmt.Sprintf("%v.%v.%v", ts.GitBranch, ts.GitHash, ts.BuildTS)
		fmt.Println(versionInfo)
		os.Exit(0)
	}
	registerStores()
	registerMetrics()
	loadConfig()
	setupLog()
	setupTracing()
	createStoreAndDomain()
	createServer()
	signal.SetupSignalHandler(serverShutdown)
	runServer()
	cleanup()
	syncLog()
}

func registerMetrics() {
	metrics.RegisterMetrics()
}

func registerStores() {
	err := kvstore.Register("tikv", zstore.Driver{})
	terror.MustNil(err)
	tikv.NewGCHandlerFunc = gcworker.NewGCWorker
	err = kvstore.Register("mocktikv", mockstore.MockDriver{})
	terror.MustNil(err)
}

func loadConfig() {
	cfg = config.GetGlobalConfig()
	overrideConfig()
}

func overrideConfig() {
	config.OverideEnv(cfg)
	actualFlags := make(map[string]bool)
	flag.Visit(func(f *flag.Flag) {
		actualFlags[f.Name] = true
	})
	if actualFlags[nmStore] {
		cfg.Store = *store
	}
	if actualFlags[nmStorePath] {
		cfg.Path = *storePath
	}
	if actualFlags[nmListenPort] {
		cfg.Port = *port
	}
	if actualFlags[nmListenSQLPort] {
		cfg.SQLPort = *sqlPort
	}
	if actualFlags[nmAdvertiseAddress] {
		cfg.AdvertiseAddress = *advertiseAddress
	}
	if len(cfg.AdvertiseAddress) == 0 {
		cfg.AdvertiseAddress = cfg.Host
	}

	if actualFlags[nmThrift] {
		cfg.HBase.ThriftEnable = *thriftEnable
	}
	if actualFlags[nmThriftPort] {
		cfg.HBase.ThriftPort = int32(*thriftPort)
	}
	if actualFlags[nmThriftProtocol] {
		cfg.HBase.ThriftProtocol = *tProtocol
	}
	if actualFlags[nmThriftTransport] {
		cfg.HBase.ThriftProtocol = *tTransport
	}
}

func setupLog() {
	fmt.Printf("%+v\n", cfg)
	err := logutil.InitZapLogger(cfg.Log.ToLogConfig())
	terror.MustNil(err)
	err = logutil.InitLogger(cfg.Log.ToLogConfig())
	terror.MustNil(err)
}

func setupTracing() {
	tracingCfg := cfg.OpenTracing.ToTracingConfig()
	tracer, _, err := tracingCfg.New("Zetta")
	if err != nil {
		log.Fatal("setup jaeger tracer failed", zap.String("error message", err.Error()))
	}
	opentracing.SetGlobalTracer(tracer)
}

func setupMetrics() {
	pushMetric(cfg.Status.MetricsAddr, time.Duration(cfg.Status.MetricsInterval)*time.Second)
}

func createServer() {
	var err error
	if *protocol == "grpc" || *protocol == "*" {
		driver := server.NewZettaDriver(storage)
		svr, err = server.NewServer(cfg, driver)
		terror.MustNil(err)
	}
	if *protocol == "mysql" || *protocol == "*" {
		zdriver := mysvr.NewZDBDriver(storage)
		mysqlsvr, err = mysvr.NewServer(cfg, zdriver)
		terror.MustNil(err)
	}
}

// TODO: Run grpc and mysql protocol at the same time.
func runServer() {
	var err error
	switch *protocol {
	case "grpc":
		err = svr.Run()
		terror.MustNil(err)
	case "mysql":
		err = mysqlsvr.Run()
		terror.MustNil(err)
	case "*":
		go func() {
			err = svr.Run()
			terror.MustNil(err)
		}()
		err = mysqlsvr.Run()
		terror.MustNil(err)
	default:
		err = svr.Run()
		terror.MustNil(err)
	}
}

func closeDomainAndStorage() {
	atomic.StoreUint32(&tikv.ShuttingDown, 1)
	dom.Close()
	err := storage.Close()
	terror.Log(errors.Trace(err))
}

func createStoreAndDomain() {
	fullPath := fmt.Sprintf("%s://%s", cfg.Store, cfg.Path)
	var err error
	storage, err = kvstore.New(fullPath)
	terror.MustNil(err)
	createSessionFunc := func() (pools.Resource, error) {
		return mysvr.CreateSession(storage)
	}
	dom, err = domain.Bootstrap(storage, createSessionFunc)
	terror.MustNil(err)
}

func serverShutdown(isgraceful bool) {
	if isgraceful {
		graceful = true
	}
	mysqlsvr.Close()
	svr.Close()
}

func cleanup() {
	closeDomainAndStorage()
}

func syncLog() {
	if err := log.Sync(); err != nil {
		fmt.Fprintln(os.Stderr, "sync log err:", err)
		os.Exit(1)
	}
}

// Prometheus push.
const zeroDuration = time.Duration(0)

// pushMetric pushes metrics in background.
func pushMetric(addr string, interval time.Duration) {
	if interval == zeroDuration || len(addr) == 0 {
		log.Info("disable Prometheus push client")
		return
	}
	log.Info("start prometheus push client", zap.String("server addr", addr), zap.String("interval", interval.String()))
	go prometheusPushClient(addr, interval)
}

// prometheusPushClient pushes metrics to Prometheus Pushgateway.
func prometheusPushClient(addr string, interval time.Duration) {
	// TODO: Zetta do not have uniq name, so we use host+port to compose a name.
	job := "zetta"
	pusher := push.New(addr, job)
	pusher = pusher.Gatherer(prometheus.DefaultGatherer)
	pusher = pusher.Grouping("instance", instanceName())
	for {
		err := pusher.Push()
		if err != nil {
			log.Error("could not push metrics to prometheus pushgateway", zap.String("err", err.Error()))
		}
		time.Sleep(interval)
	}
}

func flagBoolean(name string, defaultVal bool, usage string) *bool {
	if !defaultVal {
		// Fix #4125, golang do not print default false value in usage, so we append it.
		usage = fmt.Sprintf("%s (default false)", usage)
		return flag.Bool(name, defaultVal, usage)
	}
	return flag.Bool(name, defaultVal, usage)
}

func instanceName() string {
	hostname, err := os.Hostname()
	if err != nil {
		return "unknown"
	}
	return fmt.Sprintf("%s_%d", hostname, cfg.Port)
}
