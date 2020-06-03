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
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/zhihu/zetta/pkg/meta"
	"github.com/zhihu/zetta/tablestore/config"
	"github.com/zhihu/zetta/tablestore/ddl"
	"github.com/zhihu/zetta/tablestore/infoschema"
	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

var domain *Domain
var defaultddlLease = 10 * time.Second

// Domain represents a storage space. Different domains can use the same database name.
// Multiple domains can be used in parallel without synchronization.
type Domain struct {
	store           kv.Storage
	ddl             ddl.DDL
	isHandler       *infoschema.Handler
	SchemaValidator SchemaValidator
	etcdClient      *clientv3.Client
	exit            chan struct{}
	mu              sync.Mutex
	wg              sync.WaitGroup
}

func NewDomain(store kv.Storage) *Domain {
	return &Domain{
		store:     store,
		isHandler: infoschema.NewHandler(),
		exit:      make(chan struct{}),
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
	}
	do.SchemaValidator = NewSchemaValidator(ddlLease)
	ctx := context.Background()
	callback := &ddlCallback{do: do}
	do.ddl = ddl.NewDDL(ctx, do.etcdClient, do.store, do.isHandler, callback, ddlLease)
	err := do.ReloadInfoSchema()
	if err != nil {
		return err
	}
	do.wg.Add(1)
	go do.loadSchemaInLoop(ddlLease)
	return nil
}

func (do *Domain) loadSchemaInLoop(lease time.Duration) {
	defer do.wg.Done()
	// Lease renewal can run at any frequency.
	// Use lease/2 here as recommend by paper.
	ticker := time.NewTicker(lease / 2)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			err := do.ReloadInfoSchema()
			if err != nil {
				logutil.Logger(context.Background()).Error("reload schema in loop failed", zap.Error(err))
			}
		case <-do.exit:
			return
		}
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

func (do *Domain) Close() {
	startTime := time.Now()
	if do.ddl != nil {
		terror.Log(do.ddl.Stop())
	}
	close(do.exit)
	if do.etcdClient != nil {
		terror.Log(errors.Trace(do.etcdClient.Close()))
	}
	do.wg.Wait()
	logutil.Logger(context.Background()).Info("domain closed", zap.Duration("take time", time.Since(startTime)))
}

func Bootstrap(store kv.Storage) (*Domain, error) {
	domain = NewDomain(store)
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
	terror.ErrClassToMySQLCodes[terror.ClassDomain] = make(map[terror.ErrCode]uint16)
}
