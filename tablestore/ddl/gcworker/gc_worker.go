package gcworker

import (
	"context"
	"strconv"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"

	pd "github.com/pingcap/pd/v4/client"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/store/tikv/gcworker"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
	"github.com/pingcap/tidb/util/logutil"

	// pd "github.com/tikv/pd/client"
	"github.com/zhihu/zetta/tablestore/domain"
	"github.com/zhihu/zetta/tablestore/zstore"
	"go.uber.org/zap"
)

type GCWorker struct {
	uuid           string
	store          tikv.Storage
	pdClient       pd.Client
	isRunning      bool
	lastFinishTime time.Time
	cancel         context.CancelFunc
	done           chan error
}

type DeleteRangesManager interface {
	GetRangesToDelete(context.Context, uint64) ([]kv.KeyRange, error)
	ClearRangeRecord(context.Context, kv.KeyRange) error
}

const (
	gcWorkerTickInterval = time.Minute * 10
	gcDefaultRunInterval = time.Minute * 10
	// gcMaxExecutionTime means the max time a transaction costs in zetta.
	// Simple use time.Now() - gcMaxExecutionTime as a safepoint now.
	// Replace it with more accute and flexible safepoint calculation in the future.
	gcMaxExecutionTime   = time.Minute * 10
	gcDefaultConcurrency = 2
)

func NewGCWorker(store tikv.Storage, pdClient pd.Client) (tikv.GCHandler, error) {
	ver, err := store.CurrentVersion()
	if err != nil {
		return nil, errors.Trace(err)
	}

	worker := &GCWorker{
		uuid:     strconv.FormatUint(ver.Ver, 16),
		store:    store,
		pdClient: pdClient,
		done:     make(chan error),
	}
	return worker, nil
}

func (w *GCWorker) Start() {
	var ctx context.Context
	ctx, w.cancel = context.WithCancel(context.Background())
	go w.start(ctx)
}

func (w *GCWorker) Close() {
	w.cancel()
}

// DDL owner to be the gc leader.
func (w *GCWorker) isLeader() bool {
	do := domain.GetOnlyDomain()
	return do.DDL().IsOwner()
}

func (w *GCWorker) start(ctx context.Context) {
	defer func() {
		r := recover()
		if r != nil {
			logutil.Logger(ctx).Error("gcWorker",
				zap.Reflect("r", r),
				zap.Stack("stack"))
		}
	}()
	ticker := time.NewTicker(gcWorkerTickInterval)
	for {
		select {
		case <-ticker.C:
			w.tick(ctx)
		case err := <-w.done:
			w.isRunning = false
			w.lastFinishTime = time.Now()
			if err != nil {
				logutil.Logger(ctx).Error("[gc worker] runGCJob", zap.Error(err))
			}
		case <-ctx.Done():
			logutil.Logger(ctx).Info("[gc worker] quit", zap.String("uuid", w.uuid))
			return
		}
	}
}

func (w *GCWorker) tick(ctx context.Context) {
	if !w.isLeader() || w.isRunning {
		return
	}
	safePoint, err := w.calculateSafePoint(ctx)
	if err != nil {
		logutil.Logger(ctx).Error("[gc worker] get safe point error", zap.Error(err))
		return
	}
	go func() {
		w.done <- w.runGC(ctx, safePoint, gcDefaultConcurrency)
	}()
}

func (w *GCWorker) deleteRanges(ctx context.Context, safepoint uint64, concurrency int) error {
	rangesMgr := domain.GetOnlyDomain().DDL().(DeleteRangesManager)
	rangesToDelete, err := rangesMgr.GetRangesToDelete(ctx, safepoint)
	if err != nil {
		return errors.Trace(err)
	}

	for _, rg := range rangesToDelete {
		if zs, ok := w.store.(*zstore.ZStore); ok {
			err = zs.GetRawClient().DeleteRange(rg.StartKey, rg.EndKey)
			if err != nil {
				logutil.Logger(ctx).Error("[gc worker] gc delete ranges error", zap.Error(err))
			}
		}
		err = w.doUnsafeDestroyRangeRequest(ctx, rg.StartKey, rg.EndKey, concurrency)
		if err != nil {
			return errors.Trace(err)
		}
		err = rangesMgr.ClearRangeRecord(ctx, rg)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// needsGCOperationForStore checks if the store-level requests related to GC needs to be sent to the store. The store-level
// requests includes UnsafeDestroyRange, PhysicalScanLock, etc.
func needsGCOperationForStore(store *metapb.Store) (bool, error) {
	// TombStone means the store has been removed from the cluster and there isn't any peer on the store, so needn't do GC for it.
	// Offline means the store is being removed from the cluster and it becomes tombstone after all peers are removed from it,
	// so we need to do GC for it.
	if store.State == metapb.StoreState_Tombstone {
		return false, nil
	}

	return true, nil
}

// getStoresForGC gets the list of stores that needs to be processed during GC.
func (w *GCWorker) getStoresForGC(ctx context.Context) ([]*metapb.Store, error) {
	stores, err := w.pdClient.GetAllStores(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}

	upStores := make([]*metapb.Store, 0, len(stores))
	for _, store := range stores {
		needsGCOp, err := needsGCOperationForStore(store)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if needsGCOp {
			upStores = append(upStores, store)
		}
	}
	return upStores, nil
}

func (w *GCWorker) doUnsafeDestroyRangeRequest(ctx context.Context, startKey []byte, endKey []byte, concurrency int) error {
	// Get all stores every time deleting a region. So the store list is less probably to be stale.
	stores, err := w.getStoresForGC(ctx)
	if err != nil {
		logutil.Logger(ctx).Error("[gc worker] delete ranges: got an error while trying to get store list from PD",
			zap.String("uuid", w.uuid),
			zap.Error(err))
		return errors.Trace(err)
	}

	req := tikvrpc.NewRequest(tikvrpc.CmdUnsafeDestroyRange, &kvrpcpb.UnsafeDestroyRangeRequest{
		StartKey: startKey,
		EndKey:   endKey,
	})

	var wg sync.WaitGroup
	errChan := make(chan error, len(stores))

	for _, store := range stores {
		address := store.Address
		storeID := store.Id
		wg.Add(1)
		go func() {
			defer wg.Done()

			resp, err1 := w.store.GetTiKVClient().SendRequest(ctx, address, req, tikv.UnsafeDestroyRangeTimeout)
			if err1 == nil {
				if resp == nil || resp.Resp == nil {
					err1 = errors.Errorf("unsafe destroy range returns nil response from store %v", storeID)
				} else {
					errStr := (resp.Resp.(*kvrpcpb.UnsafeDestroyRangeResponse)).Error
					if len(errStr) > 0 {
						err1 = errors.Errorf("unsafe destroy range failed on store %v: %s", storeID, errStr)
					}
				}
			}
			errChan <- err1
		}()
	}

	var errs []string
	for range stores {
		err1 := <-errChan
		if err1 != nil {
			errs = append(errs, err1.Error())
		}
	}

	wg.Wait()

	if len(errs) > 0 {
		return errors.Errorf("[gc worker] destroy range finished with errors: %v", errs)
	}

	// Notify all affected regions in the range that UnsafeDestroyRange occurs.
	notifyTask := tikv.NewNotifyDeleteRangeTask(w.store, startKey, endKey, concurrency)
	err = notifyTask.Execute(ctx)
	if err != nil {
		return errors.Annotate(err, "[gc worker] failed notifying regions affected by UnsafeDestroyRange")
	}

	return nil
}

func (w *GCWorker) runGC(ctx context.Context, safePoint uint64, concurrency int) error {
	w.isRunning = true
	err := gcworker.RunDistributedGCJob(ctx, w.store, w.pdClient, safePoint, w.uuid, concurrency)
	if err != nil {
		return err
	}
	err = w.deleteRanges(ctx, safePoint, concurrency)
	if err != nil {
		logutil.Logger(ctx).Error("[gc worker] delete ranges error", zap.Error(err))
		return err
	}
	return err
}

func (w *GCWorker) calculateSafePoint(ctx context.Context) (uint64, error) {
	now, err := w.getOracleTime()
	if err != nil {
		return 0, errors.Trace(err)
	}
	safePoint := now.Add(-gcMaxExecutionTime)
	safePointValue := oracle.ComposeTS(oracle.GetPhysical(safePoint), 0)
	return safePointValue, nil
}

func (w *GCWorker) getOracleTime() (time.Time, error) {
	currentVer, err := w.store.CurrentVersion()
	if err != nil {
		return time.Time{}, errors.Trace(err)
	}
	physical := oracle.ExtractPhysical(currentVer.Ver)
	sec, nsec := physical/1e3, (physical%1e3)*1e6
	return time.Unix(sec, nsec), nil
}
