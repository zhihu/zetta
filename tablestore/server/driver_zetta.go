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

	"github.com/pingcap/tidb/kv"
	"github.com/zhihu/zetta/tablestore/session"
)

// ZettaDriver implements IDriver.
type ZettaDriver struct {
	Store kv.Storage
}

// NewZettaDriver creates a new ZettaDriver
func NewZettaDriver(store kv.Storage) *ZettaDriver {
	driver := &ZettaDriver{
		Store: store,
	}
	return driver
}

// ZettaContext implements QueryCtx
type ZettaContext struct {
	session   session.Session
	currentDB string
	// query ?
}

func (zc *ZettaContext) CurrentDB() string {
	return zc.currentDB
}

func (zc *ZettaContext) Close() error {
	zc.session.Close()
	return nil
}

func (zc *ZettaContext) Execute(ctx context.Context, query interface{}) ([]ResultSet, error) {
	return nil, nil
}

func (zc *ZettaContext) GetSession() session.Session {
	return zc.session
}

func (zc *ZettaContext) SetValue(key fmt.Stringer, value interface{}) {

}

// OpenCtx xxx
func (zd *ZettaDriver) OpenCtx(connId uint64, dbname string) (QueryCtx, error) {
	se, err := session.CreateSession(zd.Store)
	if err != nil {
		return nil, err
	}
	// se.SetTLSState(tlsState)
	// err = se.SetCollation(int(collation))
	// if err != nil {
	// 	return nil, err
	// }
	// se.SetClientCapability(capability)
	// se.SetConnectionID(connID)

	se.SetDB(dbname)
	zc := &ZettaContext{
		session:   se,
		currentDB: dbname,
		// stmts:     make(map[int]*TiDBStatement),
	}
	return zc, nil
}
