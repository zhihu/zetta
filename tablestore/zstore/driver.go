package zstore

import (
	"context"
	"net/url"
	"strings"
	"time"

	"crypto/tls"

	"github.com/pingcap/errors"
	pd "github.com/pingcap/pd/v4/client"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/multierr"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

var (
	GrpcConnectionCount  = 4
	GrpcKeepAliveTime    = 10
	GrpcKeepAliveTimeout = 3
)

type Driver struct {
}

func (d Driver) Open(path string) (kv.Storage, error) {
	etcdAddrs, _, err := parsePath(path)
	if err != nil {
		return nil, errors.Trace(err)
	}
	tikvDriver := tikv.Driver{}
	tikvStore, err := tikvDriver.Open(path)
	if err != nil {
		return nil, errors.Trace(err)
	}

	var tikvs tikv.Storage
	var ok bool

	if tikvs, ok = tikvStore.(tikv.Storage); !ok {
		panic("tikv storage open failed")
	}

	rawkvStore, err := tikv.NewRawKVClient(etcdAddrs, config.Security{}, pd.WithGRPCDialOptions(
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:    time.Duration(GrpcKeepAliveTime) * time.Second,
			Timeout: time.Duration(GrpcKeepAliveTimeout) * time.Second,
		})))
	if err != nil {
		return nil, errors.Trace(err)
	}
	zstore := &ZStore{
		Storage: tikvs,
		rawkv:   rawkvStore,
	}
	return zstore, nil
}

// ZStore xxxxx
type ZStore struct {
	tikv.Storage
	rawkv *tikv.RawKVClient
}

// Begin transaction
func (s *ZStore) Begin() (kv.Transaction, error) {
	return s.Storage.Begin()
}

// BeginWithStartTS begins transaction with startTS.
func (s *ZStore) BeginWithStartTS(startTS uint64) (kv.Transaction, error) {
	if startTS == 0 {
		return s.BeginWithRawKV(0)
	}
	return s.Storage.BeginWithStartTS(startTS)
}

func (s *ZStore) GetRawClient() *tikv.RawKVClient {
	return s.rawkv
}

func (s *ZStore) BeginWithRawKV(startTS uint64) (kv.Transaction, error) {
	return newZStoreTxn(s, startTS)
}

// Close store
func (s *ZStore) Close() error {
	var err1, err2 error
	if s.Storage != nil {
		err1 = s.Storage.Close()
	}
	if s.rawkv != nil {
		err2 = s.rawkv.Close()
	}
	return multierr.Combine(err1, err2)
}

// Name gets the name of the storage engine
func (s *ZStore) Name() string {
	return "ZSTORE"
}

// Describe returns of brief introduction of the storage
func (s *ZStore) Describe() string {
	return "A awesome combined kv store"
}

func (s *ZStore) SplitRegions(ctx context.Context, splitKey [][]byte, scatter bool) (regionID []uint64, err error) {
	if splitStore, ok := s.Storage.(kv.SplittableStore); !ok {
		return nil, errors.New("do not support split")
	} else {
		return splitStore.SplitRegions(ctx, splitKey, scatter)
	}
}

func (s *ZStore) WaitScatterRegionFinish(ctx context.Context, regionID uint64, backOff int) error {
	if splitStore, ok := s.Storage.(kv.SplittableStore); !ok {
		return errors.New("do not support split")
	} else {
		return splitStore.WaitScatterRegionFinish(ctx, regionID, backOff)
	}
}

func (s *ZStore) CheckRegionInScattering(regionID uint64) (bool, error) {
	if splitStore, ok := s.Storage.(kv.SplittableStore); !ok {
		return false, errors.New("do not support split")
	} else {
		return splitStore.CheckRegionInScattering(regionID)
	}
}

func (s *ZStore) EtcdAddrs() []string {
	etcdStore := s.Storage.(tikv.EtcdBackend)
	return etcdStore.EtcdAddrs()
}

func (s *ZStore) TLSConfig() *tls.Config {
	etcdStore := s.Storage.(tikv.EtcdBackend)
	return etcdStore.TLSConfig()
}

func (s *ZStore) StartGCWorker() error {
	etcdStore := s.Storage.(tikv.EtcdBackend)
	return etcdStore.StartGCWorker()
}

// ParsePath parses this path.
func parsePath(path string) (etcdAddrs []string, disableGC bool, err error) {
	var u *url.URL
	u, err = url.Parse(path)
	if err != nil {
		err = errors.Trace(err)
		return
	}
	if strings.ToLower(u.Scheme) != "tikv" && strings.ToLower(u.Scheme) != "zstore" {
		err = errors.Errorf("Uri scheme expected [tikv] but found [%s]", u.Scheme)
		logutil.BgLogger().Error("parsePath error", zap.Error(err))
		return
	}
	switch strings.ToLower(u.Query().Get("disableGC")) {
	case "true":
		disableGC = true
	case "false", "":
	default:
		err = errors.New("disableGC flag should be true/false")
		return
	}
	etcdAddrs = strings.Split(u.Host, ",")
	return
}
