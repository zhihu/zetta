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
	"math/rand"

	"fmt"
	"net"
	"net/http"
	"os"
	"os/user"
	"sync"
	"time"

	_ "net/http/pprof"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/terror"

	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"

	"github.com/zhihu/zetta/tablestore/config"
	"github.com/zhihu/zetta/tablestore/server/hthrift"
	"github.com/zhihu/zetta/tablestore/session"
)

var (
	baseConnID  uint32
	baseQueryID uint32
	serverPID   int
	osUser      string
	osVersion   string
)

func init() {
	serverPID = os.Getpid()
	currentUser, err := user.Current()
	if err != nil {
		osUser = ""
	} else {
		osUser = currentUser.Name
	}

	// osVersion, err = linux.OSVersion()
	// if err != nil {
	// 	osVersion = ""
	// }
}

// Server is the GRPC Server
type Server struct {
	cfg    *config.Config
	driver IDriver

	listener     net.Listener
	socket       net.Listener
	rwlock       sync.RWMutex
	rpcServer    *RPCServer
	tServer      *hthrift.TServer
	statusServer *http.Server

	queryCtxs map[string]QueryCtx

	quitCh chan struct{}
}

// NewServer return a new Server
func NewServer(cfg *config.Config, driver IDriver) (*Server, error) {
	s := &Server{
		cfg:       cfg,
		driver:    driver,
		queryCtxs: make(map[string]QueryCtx),
		quitCh:    make(chan struct{}, 1),
	}
	var err error
	if s.cfg.Host != "" && s.cfg.Port != 0 {
		addr := fmt.Sprintf("%s:%d", s.cfg.Host, s.cfg.Port)
		if s.listener, err = net.Listen("tcp", addr); err == nil {
			logutil.Logger(context.Background()).Info("server is running TCP listener", zap.String("addr", addr))
		}
	} else {
		err = errors.New("Server not configured to listen on either -socket or -host and -port")
	}
	if err != nil {
		return nil, errors.Trace(err)
	}
	s.rpcServer = NewRPCServer(s)

	if s.cfg.HBase.ThriftEnable {
		zDriver := driver.(*ZettaDriver)
		tServer, err := hthrift.NewTServer(s.cfg, session.CreateSessionFunc(zDriver.Store))
		if err != nil {
			return nil, errors.Trace(err)
		}
		s.tServer = tServer
	}

	// Init rand seed for randomBuf()
	rand.Seed(time.Now().UTC().UnixNano())
	return s, nil
}

// Run runs the server.
func (s *Server) Run() error {
	if s.cfg.Status.ReportStatus {
		s.startStatusHTTP()
	}
	if s.tServer != nil {
		s.tServer.Run()
	}
	s.rpcServer.Run()
	ticker := time.NewTicker(60 * time.Second)
	stop := false
	for !stop {
		select {
		case <-ticker.C:
			logutil.Logger(context.Background()).Info("session count", zap.Int32("sc", session.SessCount))
			s.Chore()
		case <-s.quitCh:
			stop = true
			break
		}
	}
	return nil
}

func (s *Server) Close() {
	s.rwlock.Lock()
	defer s.rwlock.Unlock()
	close(s.quitCh)
	if s.rpcServer != nil {
		s.rpcServer.Close()
	}
	if s.statusServer != nil {
		err := s.statusServer.Close()
		terror.Log(errors.Trace(err))
		s.statusServer = nil
	}
	if s.tServer != nil {
		s.tServer.Close()
		s.tServer = nil
	}

}

func (s *Server) Chore() {
	s.rwlock.Lock()
	defer s.rwlock.Unlock()
	curTime := time.Now()
	for sessName, qctx := range s.queryCtxs {
		session := qctx.GetSession()
		if curTime.Sub(session.LastActive()) > 13*time.Minute {
			delete(s.queryCtxs, sessName)
			logutil.Logger(context.Background()).Info("collect session",
				zap.String("session", session.GetName()), zap.Time("lastActive", session.LastActive()))
			go session.Close()
		}

	}
}

func (s *Server) Config() *config.Config {
	return s.cfg
}
