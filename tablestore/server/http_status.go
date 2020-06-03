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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/pprof"
	"net/url"
	"strings"

	"github.com/gorilla/mux"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/terror"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/tiancaiamao/appdash/traceapp"

	"github.com/pingcap/tidb/util/logutil"
	static "sourcegraph.com/sourcegraph/appdash-data"

	"github.com/zhihu/zetta/tablestore/config"

	"go.uber.org/zap"
)

const defaultStatusPort = 10080

func (s *Server) startStatusHTTP() {
	go s.startHTTPServer()
}

func serveError(w http.ResponseWriter, status int, txt string) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("X-Go-Pprof", "1")
	w.Header().Del("Content-Disposition")
	w.WriteHeader(status)
	_, err := fmt.Fprintln(w, txt)
	terror.Log(err)
}

func (s *Server) startHTTPServer() {
	router := mux.NewRouter()
	router.HandleFunc("/status", s.handleStatus).Name("Status")
	router.Handle("/metrics", promhttp.Handler()).Name("Metrics")

	addr := fmt.Sprintf("%s:%d", s.cfg.Status.StatusHost, s.cfg.Status.StatusPort)
	if s.cfg.Status.StatusPort == 0 {
		addr = fmt.Sprintf("%s:%d", s.cfg.Status.StatusHost, defaultStatusPort)
	}

	// HTTP path for web UI.
	if host, port, err := net.SplitHostPort(addr); err == nil {
		if host == "" {
			host = "localhost"
		}
		baseURL := &url.URL{
			Scheme: "http",
			Host:   fmt.Sprintf("%s:%s", host, port),
		}
		router.HandleFunc("/web/trace", traceapp.HandleTiDB).Name("Trace Viewer")
		sr := router.PathPrefix("/web/trace/").Subrouter()
		if _, err := traceapp.New(traceapp.NewRouter(sr), baseURL); err != nil {
			logutil.Logger(context.Background()).Error("new failed", zap.Error(err))
		}
		router.PathPrefix("/static/").Handler(http.StripPrefix("/static", http.FileServer(static.Data)))
	}

	serverMux := http.NewServeMux()
	serverMux.Handle("/", router)

	serverMux.HandleFunc("/debug/pprof/", pprof.Index)
	serverMux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	serverMux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	serverMux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	serverMux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	var (
		httpRouterPage bytes.Buffer
		pathTemplate   string
		err            error
	)
	httpRouterPage.WriteString("<html><head><title>Zetta Status and Metrics Report</title></head><body><h1>Zetta Status and Metrics Report</h1><table>")
	err = router.Walk(func(route *mux.Route, router *mux.Router, ancestors []*mux.Route) error {
		pathTemplate, err = route.GetPathTemplate()
		if err != nil {
			logutil.Logger(context.Background()).Error("get HTTP router path failed", zap.Error(err))
		}
		name := route.GetName()
		// If the name attribute is not set, GetName returns "".
		// "traceapp.xxx" are introduced by the traceapp package and are also ignored.
		if name != "" && !strings.HasPrefix(name, "traceapp") && err == nil {
			httpRouterPage.WriteString("<tr><td><a href='" + pathTemplate + "'>" + name + "</a><td></tr>")
		}
		return nil
	})
	if err != nil {
		logutil.Logger(context.Background()).Error("generate root failed", zap.Error(err))
	}
	httpRouterPage.WriteString("<tr><td><a href='/debug/pprof/'>Debug</a><td></tr>")
	httpRouterPage.WriteString("</table></body></html>")
	router.HandleFunc("/", func(responseWriter http.ResponseWriter, request *http.Request) {
		_, err = responseWriter.Write([]byte(httpRouterPage.String()))
		if err != nil {
			logutil.Logger(context.Background()).Error("write HTTP index page failed", zap.Error(err))
		}
	})

	logutil.Logger(context.Background()).Info("for status and metrics report", zap.String("listening on addr", addr))
	s.statusServer = &http.Server{Addr: addr, Handler: CorsHandler{handler: serverMux, cfg: s.cfg}}

	if len(s.cfg.Security.ClusterSSLCA) != 0 {
		err = s.statusServer.ListenAndServeTLS(s.cfg.Security.ClusterSSLCert, s.cfg.Security.ClusterSSLKey)
	} else {
		err = s.statusServer.ListenAndServe()
	}

	if err != nil {
		logutil.Logger(context.Background()).Info("listen failed", zap.Error(err))
	}
}

// status of Zetta.
type statusData struct {
	Connections int    `json:"connections"`
	Version     string `json:"version"`
	GitHash     string `json:"git_hash"`
}

func (s *Server) handleStatus(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	st := statusData{
		Connections: 0,
		Version:     "",
		GitHash:     "",
	}
	js, err := json.Marshal(st)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		logutil.Logger(context.Background()).Error("encode json failed", zap.Error(err))
	} else {
		_, err = w.Write(js)
		terror.Log(errors.Trace(err))
	}
}

// CorsHandler adds Cors Header if `cors` config is set.
type CorsHandler struct {
	handler http.Handler
	cfg     *config.Config
}

func (h CorsHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if h.cfg.Cors != "" {
		w.Header().Set("Access-Control-Allow-Origin", h.cfg.Cors)
		w.Header().Set("Access-Control-Allow-Methods", "GET")
	}
	h.handler.ServeHTTP(w, req)
}
