package infosync

import (
	"context"
	"encoding/json"
	"os"
	"path"
	"strconv"

	"fmt"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/ddl/util"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/owner"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tidb/util/hack"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
	"google.golang.org/grpc"

	// "github.com/pingcap/tidb/util/printer"
	"github.com/zhihu/zetta/tablestore/config"
	"github.com/zhihu/zetta/tablestore/sessionctx/variable"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/clientv3/concurrency"
)

var (
	// ServerInformationPath store server information such as IP, port and so on.
	ServerInformationPath = "/tidb/server/info"
	// ServerMinStartTSPath store the server min start timestamp.
	ServerMinStartTSPath = "/tidb/server/minstartts"

	keyOpDefaultRetryCnt = 5
	keyOpDefaultTimeout  = 1 * time.Second
	// keyOpDefaultTimeout is the default time out for etcd store.
	// keyOpDefaultTimeout = 1 * time.Second

	// InfoSessionTTL is the ETCD session's TTL in seconds.
	InfoSessionTTL = 10 * 60 // 10 minutes
	// ReportInterval is interval of infoSyncerKeeper reporting min startTS.
	ReportInterval = 30 * time.Second
	// TopologyInformationPath means etcd path for storing topology info.
	TopologyInformationPath = "/topology/tidb"

	// TopologyInfoZettaPath means etcd path for storing zetta topology info.
	TopologyInfoZettaPath = "/topology/zetta"

	// TopologySessionTTL is ttl for topology, ant it's the ETCD session's TTL in seconds.
	TopologySessionTTL = 45
	// TopologyTimeToRefresh means time to refresh etcd.
	TopologyTimeToRefresh = 30 * time.Second
)

// ServerInfo is server static information.
// It will not be updated when tidb-server running. So please only put static information in ServerInfo struct.
type ServerInfo struct {
	ServerVersionInfo
	ID             string `json:"ddl_id"`
	IP             string `json:"ip"`
	Port           uint   `json:"listening_port"`
	StatusPort     uint   `json:"status_port"`
	Lease          string `json:"lease"`
	BinlogStatus   string `json:"binlog_status"`
	StartTimestamp int64  `json:"start_timestamp"`
}

// ServerVersionInfo is the server version and git_hash.
type ServerVersionInfo struct {
	Version string `json:"version"`
	GitHash string `json:"git_hash"`
}

type InfoSyncer struct {
	etcdCli        *clientv3.Client
	info           *ServerInfo
	serverInfoPath string
	minStartTS     uint64
	minStartTSPath string

	session         *concurrency.Session
	topologySession *concurrency.Session
	prometheusAddr  string
	modifyTime      time.Time
}

// globalInfoSyncer stores the global infoSyncer.
// Use a global variable for simply the code, use the domain.infoSyncer will have circle import problem in some pkg.
// Use atomic.Value to avoid data race in the test.
var globalInfoSyncer atomic.Value

func getGlobalInfoSyncer() (*InfoSyncer, error) {
	v := globalInfoSyncer.Load()
	if v == nil {
		return nil, errors.New("infoSyncer is not initialized")
	}
	return v.(*InfoSyncer), nil
}

func setGlobalInfoSyncer(is *InfoSyncer) {
	globalInfoSyncer.Store(is)
}

// GlobalInfoSyncerInit return a new InfoSyncer. It is exported for testing.
func GlobalInfoSyncerInit(ctx context.Context, id string, etcdCli *clientv3.Client) (*InfoSyncer, error) {
	is := &InfoSyncer{
		etcdCli:        etcdCli,
		info:           getServerInfo(id),
		serverInfoPath: fmt.Sprintf("%s/%s", ServerInformationPath, id),
		minStartTSPath: fmt.Sprintf("%s/%s", ServerMinStartTSPath, id),
	}
	err := is.init(ctx)
	if err != nil {
		return nil, err
	}
	setGlobalInfoSyncer(is)
	return is, nil
}

// Init creates a new etcd session and stores server info to etcd.
func (is *InfoSyncer) init(ctx context.Context) error {
	err := is.newSessionAndStoreServerInfo(ctx, owner.NewSessionDefaultRetryCnt)
	if err != nil {
		return err
	}
	return is.newTopologySessionAndStoreServerInfo(ctx, owner.NewSessionDefaultRetryCnt)
}

// ReportMinStartTS reports self server min start timestamp to ETCD.
func (is *InfoSyncer) ReportMinStartTS(store kv.Storage) {

	// Calculate the lower limit of the start timestamp to avoid extremely old transaction delaying GC.
	currentVer, err := store.CurrentVersion()
	if err != nil {
		logutil.Logger(context.Background()).Error("update minStartTS failed", zap.Error(err))
		return
	}
	now := time.Unix(0, oracle.ExtractPhysical(currentVer.Ver)*1e6)
	// startTSLowerLimit := variable.GoTimeToTS(now.Add(-time.Duration(kv.MaxTxnTimeUse) * time.Millisecond))

	minStartTS := variable.GoTimeToTS(now)
	// for _, info := range pl {
	// 	if info.CurTxnStartTS > startTSLowerLimit && info.CurTxnStartTS < minStartTS {
	// 		minStartTS = info.CurTxnStartTS
	// 	}
	// }

	is.minStartTS = minStartTS
	err = is.storeMinStartTS(context.Background())
	if err != nil {
		logutil.Logger(context.Background()).Error("update minStartTS failed", zap.Error(err))
	}
}

// storeMinStartTS stores self server min start timestamp to etcd.
func (is *InfoSyncer) storeMinStartTS(ctx context.Context) error {
	if is.etcdCli == nil {
		return nil
	}
	return util.PutKVToEtcd(ctx, is.etcdCli, keyOpDefaultRetryCnt, is.minStartTSPath,
		strconv.FormatUint(is.minStartTS, 10),
		clientv3.WithLease(is.session.Lease()))
}

// getServerInfo gets self tidb server information.
func getServerInfo(id string) *ServerInfo {
	cfg := config.GetGlobalConfig()
	info := &ServerInfo{
		ID:         id,
		IP:         cfg.AdvertiseAddress,
		Port:       cfg.Port,
		StatusPort: cfg.Status.StatusPort,
		Lease:      cfg.Lease,
		// BinlogStatus:   binloginfo.GetStatus().String(),
		StartTimestamp: time.Now().Unix(),
	}
	// info.Version = mysql.ServerVersion
	// info.GitHash = printer.TiDBGitHash

	return info
}

// GetServerInfoByID gets specified server static information from etcd.
func GetServerInfoByID(ctx context.Context, id string) (*ServerInfo, error) {
	is, err := getGlobalInfoSyncer()
	if err != nil {
		return nil, err
	}
	return is.getServerInfoByID(ctx, id)
}

func (is *InfoSyncer) getServerInfoByID(ctx context.Context, id string) (*ServerInfo, error) {
	if is.etcdCli == nil || id == is.info.ID {
		return is.info, nil
	}
	key := fmt.Sprintf("%s/%s", ServerInformationPath, id)
	infoMap, err := getInfo(ctx, is.etcdCli, key, keyOpDefaultRetryCnt, keyOpDefaultTimeout)
	if err != nil {
		return nil, err
	}
	info, ok := infoMap[id]
	if !ok {
		return nil, errors.Errorf("[info-syncer] get %s failed", key)
	}
	return info, nil
}

// getInfo gets server information from etcd according to the key and opts.
func getInfo(ctx context.Context, etcdCli *clientv3.Client, key string, retryCnt int, timeout time.Duration, opts ...clientv3.OpOption) (map[string]*ServerInfo, error) {
	var err error
	var resp *clientv3.GetResponse
	allInfo := make(map[string]*ServerInfo)
	for i := 0; i < retryCnt; i++ {
		select {
		case <-ctx.Done():
			err = errors.Trace(ctx.Err())
			return nil, err
		default:
		}
		childCtx, cancel := context.WithTimeout(ctx, timeout)
		resp, err = etcdCli.Get(childCtx, key, opts...)
		cancel()
		if err != nil {
			logutil.Logger(ctx).Info("get key failed", zap.String("key", key), zap.Error(err))
			time.Sleep(200 * time.Millisecond)
			continue
		}
		for _, kv := range resp.Kvs {
			info := &ServerInfo{
				// BinlogStatus: binloginfo.BinlogStatusUnknown.String(),
			}
			err = json.Unmarshal(kv.Value, info)
			if err != nil {
				logutil.Logger(ctx).Info("get key failed", zap.String("key", string(kv.Key)), zap.ByteString("value", kv.Value),
					zap.Error(err))
				return nil, errors.Trace(err)
			}
			allInfo[info.ID] = info
		}
		return allInfo, nil
	}
	return nil, errors.Trace(err)
}

// Done returns a channel that closes when the info syncer is no longer being refreshed.
func (is *InfoSyncer) Done() <-chan struct{} {
	if is.etcdCli == nil {
		return make(chan struct{}, 1)
	}
	return is.session.Done()
}

// TopologyDone returns a channel that closes when the topology syncer is no longer being refreshed.
func (is *InfoSyncer) TopologyDone() <-chan struct{} {
	if is.etcdCli == nil {
		return make(chan struct{}, 1)
	}
	return is.topologySession.Done()
}

// Restart restart the info syncer with new session leaseID and store server info to etcd again.
func (is *InfoSyncer) Restart(ctx context.Context) error {
	return is.newSessionAndStoreServerInfo(ctx, NewSessionDefaultRetryCnt)
}

// RestartTopology restart the topology syncer with new session leaseID and store server info to etcd again.
func (is *InfoSyncer) RestartTopology(ctx context.Context) error {
	return is.newTopologySessionAndStoreServerInfo(ctx, NewSessionDefaultRetryCnt)
}

// newSessionAndStoreServerInfo creates a new etcd session and stores server info to etcd.
func (is *InfoSyncer) newSessionAndStoreServerInfo(ctx context.Context, retryCnt int) error {
	if is.etcdCli == nil {
		return nil
	}
	logPrefix := fmt.Sprintf("[Info-syncer] %s", is.serverInfoPath)
	session, err := NewSession(ctx, logPrefix, is.etcdCli, retryCnt, InfoSessionTTL)
	if err != nil {
		return err
	}
	is.session = session

	return is.storeServerInfo(ctx)
}

// newTopologySessionAndStoreServerInfo creates a new etcd session and stores server info to etcd.
func (is *InfoSyncer) newTopologySessionAndStoreServerInfo(ctx context.Context, retryCnt int) error {
	if is.etcdCli == nil {
		return nil
	}
	logPrefix := fmt.Sprintf("[topology-syncer] %s/%s:%d", TopologyInformationPath, is.info.IP, is.info.Port)
	session, err := NewSession(ctx, logPrefix, is.etcdCli, retryCnt, TopologySessionTTL)
	if err != nil {
		return err
	}

	is.topologySession = session
	return is.StoreTopologyInfo(ctx)
}

// storeServerInfo stores self server static information to etcd.
func (is *InfoSyncer) storeServerInfo(ctx context.Context) error {
	if is.etcdCli == nil {
		return nil
	}
	infoBuf, err := json.Marshal(is.info)
	if err != nil {
		return errors.Trace(err)
	}
	str := string(hack.String(infoBuf))
	err = util.PutKVToEtcd(ctx, is.etcdCli, keyOpDefaultRetryCnt, is.serverInfoPath, str, clientv3.WithLease(is.session.Lease()))
	return err
}

// StoreTopologyInfo  stores the topology of tidb to etcd.
func (is *InfoSyncer) StoreTopologyInfo(ctx context.Context) error {
	if is.etcdCli == nil {
		return nil
	}
	topologyInfo := is.getTopologyInfo()
	infoBuf, err := json.Marshal(topologyInfo)
	if err != nil {
		return errors.Trace(err)
	}
	str := string(hack.String(infoBuf))
	key := fmt.Sprintf("%s/%s:%v/info", TopologyInformationPath, is.info.IP, is.info.Port)
	// Note: no lease is required here.
	err = util.PutKVToEtcd(ctx, is.etcdCli, keyOpDefaultRetryCnt, key, str)
	if err != nil {
		return err
	}
	// Initialize ttl.
	return is.updateTopologyAliveness(ctx)
}

// refreshTopology refreshes etcd topology with ttl stored in "/topology/tidb/ip:port/ttl".
func (is *InfoSyncer) updateTopologyAliveness(ctx context.Context) error {
	if is.etcdCli == nil {
		return nil
	}
	key := fmt.Sprintf("%s/%s:%v/ttl", TopologyInformationPath, is.info.IP, is.info.Port)
	return util.PutKVToEtcd(ctx, is.etcdCli, keyOpDefaultRetryCnt, key,
		fmt.Sprintf("%v", time.Now().UnixNano()),
		clientv3.WithLease(is.topologySession.Lease()))
}

// RemoveServerInfo remove self server static information from etcd.
func (is *InfoSyncer) RemoveServerInfo() {
	if is.etcdCli == nil {
		return
	}
	err := util.DeleteKeyFromEtcd(is.serverInfoPath, is.etcdCli, keyOpDefaultRetryCnt, keyOpDefaultTimeout)
	if err != nil {
		logutil.Logger(context.Background()).Error("remove server info failed", zap.Error(err))
	}
}

// RemoveMinStartTS removes self server min start timestamp from etcd.
func (is *InfoSyncer) RemoveMinStartTS() {
	if is.etcdCli == nil {
		return
	}
	err := util.DeleteKeyFromEtcd(is.minStartTSPath, is.etcdCli, keyOpDefaultRetryCnt, keyOpDefaultTimeout)
	if err != nil {
		logutil.Logger(context.Background()).Error("remove minStartTS failed", zap.Error(err))
	}
}

type topologyInfo struct {
	ServerVersionInfo
	StatusPort     uint   `json:"status_port"`
	DeployPath     string `json:"deploy_path"`
	StartTimestamp int64  `json:"start_timestamp"`
}

func (is *InfoSyncer) getTopologyInfo() topologyInfo {
	s, err := os.Executable()
	if err != nil {
		s = ""
	}
	dir := path.Dir(s)
	return topologyInfo{
		ServerVersionInfo: ServerVersionInfo{
			// Version: mysql.TiDBReleaseVersion,
			GitHash: is.info.ServerVersionInfo.GitHash,
		},
		StatusPort:     is.info.StatusPort,
		DeployPath:     dir,
		StartTimestamp: is.info.StartTimestamp,
	}
}

const (
	NewSessionDefaultRetryCnt = 3
	newSessionRetryInterval   = 200 * time.Millisecond
	logIntervalCnt            = int(3 * time.Second / newSessionRetryInterval)
)

// NewSession creates a new etcd session.
func NewSession(ctx context.Context, logPrefix string, etcdCli *clientv3.Client, retryCnt, ttl int) (*concurrency.Session, error) {
	var err error

	var etcdSession *concurrency.Session
	failedCnt := 0
	for i := 0; i < retryCnt; i++ {
		if err = contextDone(ctx, err); err != nil {
			return etcdSession, errors.Trace(err)
		}

		etcdSession, err = concurrency.NewSession(etcdCli,
			concurrency.WithTTL(ttl), concurrency.WithContext(ctx))
		if err == nil {
			break
		}
		if failedCnt%logIntervalCnt == 0 {
			logutil.Logger(ctx).Warn("failed to new session to etcd", zap.String("ownerInfo", logPrefix), zap.Error(err))
		}
		time.Sleep(newSessionRetryInterval)
		failedCnt++
	}
	return etcdSession, errors.Trace(err)
}

func contextDone(ctx context.Context, err error) error {
	select {
	case <-ctx.Done():
		return errors.Trace(ctx.Err())
	default:
	}
	// Sometime the ctx isn't closed, but the etcd client is closed,
	// we need to treat it as if context is done.
	// TODO: Make sure ctx is closed with etcd client.
	if terror.ErrorEqual(err, context.Canceled) ||
		terror.ErrorEqual(err, context.DeadlineExceeded) ||
		terror.ErrorEqual(err, grpc.ErrClientConnClosing) {
		return errors.Trace(err)
	}

	return nil
}
