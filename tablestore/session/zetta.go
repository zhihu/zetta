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
	"sync"
	"sync/atomic"
	"time"

	"github.com/ngaut/pools"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/zhihu/zetta/pkg/meta"
	"github.com/zhihu/zetta/tablestore/domain"
	"go.uber.org/zap"
)

type domainMap struct {
	domains map[string]*domain.Domain
	mu      sync.Mutex
}

func (dm *domainMap) Get(store kv.Storage) (d *domain.Domain, err error) {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	// If this is the only domain instance, and the caller doesn't provide store.
	if len(dm.domains) == 1 && store == nil {
		for _, r := range dm.domains {
			return r, nil
		}
	}

	key := store.UUID()
	d = dm.domains[key]
	if d != nil {
		return
	}

	// ddlLease := schemaLease
	// statisticLease := statsLease
	// err = util.RunWithRetry(util.DefaultMaxRetries, util.RetryInterval, func() (retry bool, err1 error) {
	// 	logutil.Logger(context.Background()).Info("new domain",
	// 		zap.String("store", store.UUID()),
	// 		zap.Stringer("ddl lease", ddlLease),
	// 		zap.Stringer("stats lease", statisticLease))
	// 	factory := createSessionFunc(store)
	// 	sysFactory := createSessionWithDomainFunc(store)
	// 	d = domain.NewDomain(store, ddlLease, statisticLease, factory)
	// 	err1 = d.Init(ddlLease, sysFactory)
	// 	if err1 != nil {
	// 		// If we don't clean it, there are some dirty data when retrying the function of Init.
	// 		d.Close()
	// 		logutil.Logger(context.Background()).Error("[ddl] init domain failed",
	// 			zap.Error(err1))
	// 	}
	// 	return true, err1
	// })
	// if err != nil {
	// 	return nil, err
	// }
	//d = domain.NewDomain(store)
	d.Init(10 * time.Second)
	dm.domains[key] = d

	return
}

func (dm *domainMap) Delete(store kv.Storage) {
	dm.mu.Lock()
	delete(dm.domains, store.UUID())
	dm.mu.Unlock()
}

var (
	domap = &domainMap{
		domains: map[string]*domain.Domain{},
	}
	// store.UUID()-> IfBootstrapped
	storeBootstrapped     = make(map[string]bool)
	storeBootstrappedLock sync.Mutex

	// schemaLease is the time for re-updating remote schema.
	// In online DDL, we must wait 2 * SchemaLease time to guarantee
	// all servers get the neweset schema.
	// Default schema lease time is 1 second, you can change it with a proper time,
	// but you must know that too little may cause badly performance degradation.
	// For production, you should set a big schema lease, like 300s+.
	schemaLease = int64(1 * time.Second)

	// statsLease is the time for reload stats table.
	statsLease = int64(3 * time.Second)
)

func setStoreBootstrapped(storeUUID string) {
	storeBootstrappedLock.Lock()
	defer storeBootstrappedLock.Unlock()
	storeBootstrapped[storeUUID] = true
}

// unsetStoreBootstrapped delete store uuid from stored bootstrapped map.
// currently this function only used for test.
func unsetStoreBootstrapped(storeUUID string) {
	storeBootstrappedLock.Lock()
	defer storeBootstrappedLock.Unlock()
	delete(storeBootstrapped, storeUUID)
}

// SetSchemaLease changes the default schema lease time for DDL.
// This function is very dangerous, don't use it if you really know what you do.
// SetSchemaLease only affects not local storage after bootstrapped.
func SetSchemaLease(lease time.Duration) {
	atomic.StoreInt64(&schemaLease, int64(lease))
}

// SetStatsLease changes the default stats lease time for loading stats info.
func SetStatsLease(lease time.Duration) {
	atomic.StoreInt64(&statsLease, int64(lease))
}

// DisableStats4Test disables the stats for tests.
func DisableStats4Test() {
	SetStatsLease(-1)
}

const (
	notBootstrapped         = 0
	currentBootstrapVersion = 35
)

func getStoreBootstrapVersion(store kv.Storage) int64 {
	storeBootstrappedLock.Lock()
	defer storeBootstrappedLock.Unlock()
	// check in memory
	_, ok := storeBootstrapped[store.UUID()]
	if ok {
		return currentBootstrapVersion
	}

	var ver int64
	// check in kv store
	err := kv.RunInNewTxn(store, false, func(txn kv.Transaction) error {
		var err error
		t := meta.NewMeta(txn)
		ver, err = t.GetBootstrapVersion()
		return err
	})

	if err != nil {
		logutil.Logger(context.Background()).Fatal("check bootstrapped failed",
			zap.Error(err))
	}

	if ver > notBootstrapped {
		// here mean memory is not ok, but other server has already finished it
		storeBootstrapped[store.UUID()] = true
	}

	return ver
}

func finishBootstrap(store kv.Storage) {
	storeBootstrappedLock.Lock()
	storeBootstrapped[store.UUID()] = true
	storeBootstrappedLock.Unlock()

	err := kv.RunInNewTxn(store, true, func(txn kv.Transaction) error {
		t := meta.NewMeta(txn)
		err := t.FinishBootstrap(currentBootstrapVersion)
		return err
	})
	if err != nil {
		logutil.Logger(context.Background()).Fatal("finish bootstrap failed",
			zap.Error(err))
	}
}

// GetDomain gets the associated domain for store.
func GetDomain(store kv.Storage) (*domain.Domain, error) {
	return domap.Get(store)
}

// BootstrapSession runs the first time when the TiDB server start.
/*
func BootstrapSession(store kv.Storage) (*domain.Domain, error) {
	// cfg := config.GetGlobalConfig()
	dm := domain.NewDomain(store, CreateSessionFunc)
	// dm.Init()
	return dm, nil
}
*/

func CreateSessionFunc(store kv.Storage) pools.Factory {
	return func() (pools.Resource, error) {
		se, err := createSession(store)
		if err != nil {
			return nil, err
		}
		return se, nil
	}
}
