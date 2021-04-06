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

package domain

import (
	"context"
	"math"
	"os"
	"sync"
	"time"

	"github.com/ngaut/pools"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/logutil"

	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"

	"github.com/zhihu/zetta/pkg/meta"
	"github.com/zhihu/zetta/pkg/metrics"
	"github.com/zhihu/zetta/tablestore/config"
	"github.com/zhihu/zetta/tablestore/ddl"
	"github.com/zhihu/zetta/tablestore/domain/infosync"
	"github.com/zhihu/zetta/tablestore/infoschema"
)

const (
	// NewSessionDefaultRetryCnt is the default retry times when create new session.
	NewSessionDefaultRetryCnt = 3
	// NewSessionRetryUnlimited is the unlimited retry times when create new session.
	NewSessionRetryUnlimited = math.MaxInt64
	keyOpDefaultTimeout      = 5 * time.Second
)

var (
	domain          *Domain
	defaultddlLease = 10 * time.Second
)

// Domain represents a storage space. Different domains can use the same database name.
// Multiple domains can be used in parallel without synchronization.
type Domain struct {
	store           kv.Storage
	ddl             ddl.DDL
	isHandler       *infoschema.Handler
	SchemaValidator SchemaValidator
	sysSessionPool  *sessionPool
	info            *infosync.InfoSyncer
	etcdClient      *clientv3.Client
	exit            chan struct{}
	mu              sync.Mutex
	wg              sync.WaitGroup

	sessFactory pools.Factory
}

const resourceIdleTimeout = 3 * time.Minute // resources in the ResourcePool will be recycled after idleTimeout

func NewDomain(store kv.Storage, sessionFactory pools.Factory) *Domain {
	capacity := 200
	return &Domain{
		store:          store,
		sysSessionPool: newSessionPool(capacity, sessionFactory),
		isHandler:      infoschema.NewHandler(),
		exit:           make(chan struct{}),
		sessFactory:    sessionFactory,
	}
}

// Init: ddlLease is used for ddl initialization.
func (do *Domain) Init(ddlLease time.Duration) error {
	if ebd, ok := do.store.(tikv.EtcdBackend); ok {
		if addrs := ebd.EtcdAddrs(); addrs != nil {
			cfg := config.GetGlobalConfig()
			cli, err := clientv3.New(clientv3.Config{
				Endpoints:        addrs,
				AutoSyncInterval: 30 * time.Second,
				DialTimeout:      5 * time.Second,
				DialOptions: []grpc.DialOption{
					grpc.WithBackoffMaxDelay(time.Second * 3),
					grpc.WithKeepaliveParams(keepalive.ClientParameters{
						Time:                time.Duration(cfg.TiKVClient.GrpcKeepAliveTime) * time.Second,
						Timeout:             time.Duration(cfg.TiKVClient.GrpcKeepAliveTimeout) * time.Second,
						PermitWithoutStream: true,
					}),
				},
				TLS: ebd.TLSConfig(),
			})
			if err != nil {
				return errors.Trace(err)
			}
			do.etcdClient = cli
		}
		err := ebd.StartGCWorker()
		if err != nil {
			return errors.Trace(err)
		}
	}
	do.SchemaValidator = NewSchemaValidator(ddlLease)
	ctx := context.Background()
	callback := &ddlCallback{do: do}
	sessPool := pools.NewResourcePool(do.sessFactory, 5, 5, resourceIdleTimeout)
	do.ddl = ddl.NewDDL(ctx, do.etcdClient, do.store, do.isHandler, callback, ddlLease, sessPool)

	err := do.ddl.SchemaSyncer().Init(ctx)
	if err != nil {
		return err
	}

	err = do.ReloadInfoSchema()
	if err != nil {
		return err
	}
	do.info, err = infosync.GlobalInfoSyncerInit(ctx, do.ddl.GetID(), do.etcdClient)
	if err != nil {
		return err
	}

	do.wg.Add(1)
	go do.loadSchemaInLoop(ddlLease)

	do.wg.Add(1)
	go do.infoSyncerKeeper()

	do.wg.Add(1)
	go do.topologySyncerKeeper()

	return nil
}

func (do *Domain) loadSchemaInLoop(lease time.Duration) {
	defer do.wg.Done()
	// Lease renewal can run at any frequency.
	// Use lease/2 here as recommend by paper.
	ticker := time.NewTicker(lease / 2)
	defer ticker.Stop()
	syncer := do.ddl.SchemaSyncer()
	for {
		select {
		case <-ticker.C:
			err := do.ReloadInfoSchema()
			if err != nil {
				logutil.Logger(context.Background()).Error("reload schema in loop failed", zap.Error(err))
			}
		case _, ok := <-syncer.GlobalVersionCh():
			err := do.ReloadInfoSchema()
			if err != nil {
				logutil.BgLogger().Error("reload schema in loop failed", zap.Error(err))
			}
			if !ok {
				logutil.BgLogger().Warn("reload schema in loop, schema syncer need rewatch")
				// Make sure the rewatch doesn't affect load schema, so we watch the global schema version asynchronously.
				syncer.WatchGlobalSchemaVer(context.Background())
			}
		case <-do.exit:
			return
		}
	}
}

func (do *Domain) infoSyncerKeeper() {
	defer do.wg.Done()
	defer recoverInDomain("infoSyncerKeeper", false)
	ticker := time.NewTicker(infosync.ReportInterval)
	defer ticker.Stop()
	ctx := context.Background()
	for {
		select {
		case <-ticker.C:
			do.info.ReportMinStartTS(do.Store())
		case <-do.info.Done():
			logutil.Logger(ctx).Info("server info syncer need to restart")
			if err := do.info.Restart(context.Background()); err != nil {
				logutil.Logger(ctx).Error("server restart failed", zap.Error(err))
			}
			logutil.Logger(ctx).Info("server info syncer restarted")
		case <-do.exit:
			return
		}
	}
}

func (do *Domain) topologySyncerKeeper() {
	defer do.wg.Done()
	defer recoverInDomain("topologySyncerKeeper", false)
	ticker := time.NewTicker(infosync.TopologyTimeToRefresh)
	defer ticker.Stop()

	ctx := context.Background()
	for {
		select {
		case <-ticker.C:
			err := do.info.StoreTopologyInfo(context.Background())
			if err != nil {
				logutil.Logger(ctx).Error("refresh topology in loop failed", zap.Error(err))
			}
		case <-do.info.TopologyDone():
			logutil.Logger(ctx).Info("server topology syncer need to restart")
			if err := do.info.RestartTopology(context.Background()); err != nil {
				logutil.Logger(ctx).Error("server restart failed", zap.Error(err))
			}
			logutil.Logger(ctx).Info("server topology syncer restarted")
		case <-do.exit:
			return
		}
	}
}

func recoverInDomain(funcName string, quit bool) {
	r := recover()
	if r == nil {
		return
	}
	buf := util.GetStack()
	logutil.Logger(context.Background()).Error("recover in domain failed", zap.String("funcName", funcName),
		zap.Any("error", r), zap.String("buffer", string(buf)))
	metrics.PanicCounter.WithLabelValues(metrics.LabelDomain).Inc()
	if quit {
		// Wait for metrics to be pushed.
		time.Sleep(time.Second * 15)
		os.Exit(1)
	}
}

func (do *Domain) ReloadInfoSchema() error {
	currentVersion, err := do.store.CurrentVersion()
	if err != nil {
		return err
	}
	snapshot, err := do.store.GetSnapshot(kv.NewVersion(currentVersion.Ver))
	if err != nil {
		return err
	}
	m := meta.NewSnapshotMeta(snapshot)
	neededSchemaVersion, err := m.GetSchemaVersion()
	if err != nil {
		return nil
	}

	schemaVersion := int64(0)
	currentInfoSchema := do.isHandler.Get()
	if currentInfoSchema != nil {
		schemaVersion = currentInfoSchema.SchemaMetaVersion()
	} else {
		err = do.isHandler.InitCurrentInfoSchema(m)
		if err != nil {
			return err
		}
	}

	defer func() {
		// There are two possibilities for not updating the self schema version to etcd.
		// 1. Failed to loading schema information.
		// 2. When users use history read feature, the neededSchemaVersion isn't the latest schema version.
		if err != nil || neededSchemaVersion < do.InfoSchema().SchemaMetaVersion() {
			logutil.BgLogger().Info("do not update self schema version to etcd",
				zap.Int64("usedSchemaVersion", schemaVersion),
				zap.Int64("neededSchemaVersion", neededSchemaVersion), zap.Error(err))
			return
		}

		err = do.ddl.SchemaSyncer().UpdateSelfVersion(context.Background(), neededSchemaVersion)
		if err != nil {
			logutil.BgLogger().Info("update self version failed",
				zap.Int64("usedSchemaVersion", schemaVersion),
				zap.Int64("neededSchemaVersion", neededSchemaVersion), zap.Error(err))
		}
	}()

	if neededSchemaVersion == 0 || neededSchemaVersion != schemaVersion {
		return do.reloadInfoSchema(m)
	}
	return nil
}

func (do *Domain) reloadInfoSchema(m *meta.Meta) error {
	return do.isHandler.InitCurrentInfoSchema(m)
}

func (do *Domain) InfoSchema() infoschema.InfoSchema {
	return do.isHandler.Get()
}

func (do *Domain) DDL() ddl.DDL {
	return do.ddl
}

func (do *Domain) Store() kv.Storage {
	return do.store
}

func (do *Domain) isClose() bool {
	select {
	case <-do.exit:
		logutil.Logger(context.Background()).Info("domain is closed")
		return true
	default:
	}
	return false
}

func (do *Domain) Close() {
	startTime := time.Now()
	if do.ddl != nil {
		terror.Log(do.ddl.Stop())
	}

	if do.info != nil {
		do.info.RemoveServerInfo()
		do.info.RemoveMinStartTS()
	}
	close(do.exit)
	if do.etcdClient != nil {
		terror.Log(errors.Trace(do.etcdClient.Close()))
	}
	do.wg.Wait()
	logutil.Logger(context.Background()).Info("domain closed", zap.Duration("take time", time.Since(startTime)))
}

func Bootstrap(store kv.Storage, sessionFactory pools.Factory) (*Domain, error) {
	domain = NewDomain(store, sessionFactory)
	err := domain.Init(defaultddlLease)
	return domain, err
}

func GetDomain4Test() *Domain {
	return domain
}

func GetOnlyDomain() *Domain {
	return domain
}

type ddlCallback struct {
	ddl.BaseCallback
	do *Domain
}

func (c *ddlCallback) OnChanged(err error) error {
	return nil
}

// Domain error codes.
const (
	codeInfoSchemaExpired terror.ErrCode = 1
	codeInfoSchemaChanged terror.ErrCode = 2
)

var (
	// ErrInfoSchemaExpired returns the error that information schema is out of date.
	ErrInfoSchemaExpired = terror.ClassDomain.New(codeInfoSchemaExpired, "Information schema is out of date.")
	// ErrInfoSchemaChanged returns the error that information schema is changed.
	ErrInfoSchemaChanged = terror.ClassDomain.New(codeInfoSchemaChanged,
		"Information schema is changed. "+kv.TxnRetryableMark)
)

func init() {
	// Map error codes to mysql error codes.
	terror.ErrClassToMySQLCodes[terror.ClassDomain] = make(map[terror.ErrCode]struct{})
}
