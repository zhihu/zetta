PROJECT=zetta

ZETTA_PKG := github.com/zhihu/zetta
GOPATH ?= $(shell go env GOPATH)

PACKAGES := go list ./...
GOCHECKER := awk '{ print } END { if (NR > 0) { exit 1 } }'

GO:= GOPRIVATE="github.com/pingcap" go

LDFLAGS += -X "$(ZETTA_PKG)/tablestore.ReleaseVersion=$(shell git describe --tags --dirty)"
LDFLAGS += -X "$(ZETTA_PKG)/tablestore.BuildTS=$(shell date -u '+%Y-%m-%d %I:%M:%S')"
LDFLAGS += -X "$(ZETTA_PKG)/tablestore.GitHash=$(shell git rev-parse HEAD)"
LDFLAGS += -X "$(ZETTA_PKG)/tablestore.GitBranch=$(shell git rev-parse --abbrev-ref HEAD)"

GOVER_MAJOR := $(shell go version | sed -E -e "s/.*go([0-9]+)[.]([0-9]+).*/\1/")
GOVER_MINOR := $(shell go version | sed -E -e "s/.*go([0-9]+)[.]([0-9]+).*/\2/")
GO111 := $(shell [ $(GOVER_MAJOR) -gt 1 ] || [ $(GOVER_MAJOR) -eq 1 ] && [ $(GOVER_MINOR) -ge 11 ]; echo $$?)
ifeq ($(GO111), 1)
$(error "go below 1.11 does not support modules")
endif

default: build

all: dev

dev: build tools check test

ci: build check basic-test

build: zetta-server

zetta-server: export GO111MODULE=on
zetta-server:
ifeq ("$(WITH_RACE)", "1")
	$(GO) build -race -gcflags '$(GCFLAGS)' -ldflags '$(LDFLAGS)' -o bin/zetta-server cmd/zetta-server/main.go
else
	$(GO) build -gcflags '$(GCFLAGS)' -ldflags '$(LDFLAGS)' -o bin/zetta-server cmd/zetta-server/main.go
endif

.PHONY: all ci vendor
