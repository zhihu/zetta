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

package hthrift

import (
	"errors"
	"fmt"
	"sync"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/ngaut/pools"

	"github.com/pingcap/tidb/util/logutil"
	"github.com/zhihu/zetta/tablestore/config"
	"github.com/zhihu/zetta/tablestore/server/hthrift/hbase"
	"go.uber.org/zap"
)

type TServer struct {
	addr     string
	cfg      *config.Config
	ss       *thrift.TSimpleServer
	sessPool *sessionPool
}

func NewTServer(cfg *config.Config, factory pools.Factory) (*TServer, error) {
	var (
		protocolFactory  thrift.TProtocolFactory
		transportFactory thrift.TTransportFactory
		capacity         = 100
	)

	switch cfg.HBase.ThriftProtocol {
	case "compact":
		protocolFactory = thrift.NewTCompactProtocolFactory()
	case "simplejson":
		protocolFactory = thrift.NewTSimpleJSONProtocolFactory()
	case "json":
		protocolFactory = thrift.NewTJSONProtocolFactory()
	case "binary", "":
		protocolFactory = thrift.NewTBinaryProtocolFactoryDefault()
	default:
		protocolFactory = thrift.NewTBinaryProtocolFactoryDefault()
	}

	switch cfg.HBase.ThriftTransport {
	case "buffered", "":
		transportFactory = thrift.NewTBufferedTransportFactory(8192)
	case "framed":
		transportFactory = thrift.NewTTransportFactory()
		transportFactory = thrift.NewTFramedTransportFactory(transportFactory)
	default:
		transportFactory = thrift.NewTBufferedTransportFactory(8192)
	}

	listenAddr := fmt.Sprintf("%v:%d", cfg.HBase.ThriftHost, cfg.HBase.ThriftPort)
	transport, err := thrift.NewTServerSocket(listenAddr)
	if err != nil {
		logutil.BgLogger().Error("create thrift-server socket error", zap.Error(err))
		return nil, err
	}

	tserver := &TServer{
		addr:     listenAddr,
		cfg:      cfg,
		sessPool: newSessionPool(capacity, factory),
	}
	handler := NewHBaseHandler(tserver)
	processor := hbase.NewHbaseProcessor(handler)
	simpleServer := thrift.NewTSimpleServer4(processor, transport, transportFactory, protocolFactory)
	tserver.ss = simpleServer
	return tserver, nil
}

func (ts *TServer) Run() {
	go func() {
		logutil.BgLogger().Info("hbase thrift server start", zap.String("addr", ts.addr))
		if err := ts.ss.Serve(); err != nil {
			logutil.BgLogger().Error("thrift server serve error", zap.Error(err))
			return
		}
	}()
}

func (ts *TServer) Close() {
	ts.ss.Stop()
	ts.sessPool.Close()
}

type sessionPool struct {
	resources chan pools.Resource
	factory   pools.Factory
	mu        struct {
		sync.RWMutex
		closed bool
	}
}

func newSessionPool(cap int, factory pools.Factory) *sessionPool {
	return &sessionPool{
		resources: make(chan pools.Resource, cap),
		factory:   factory,
	}
}

func (p *sessionPool) Get() (resource pools.Resource, err error) {
	var ok bool
	select {
	case resource, ok = <-p.resources:
		if !ok {
			err = errors.New("session pool closed")
		}
	default:
		resource, err = p.factory()
	}
	return
}

func (p *sessionPool) Put(resource pools.Resource) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.mu.closed {
		resource.Close()
		return
	}

	select {
	case p.resources <- resource:
	default:
		resource.Close()
	}
}
func (p *sessionPool) Close() {
	p.mu.Lock()
	if p.mu.closed {
		p.mu.Unlock()
		return
	}
	p.mu.closed = true
	close(p.resources)
	p.mu.Unlock()

	for r := range p.resources {
		r.Close()
	}
}
