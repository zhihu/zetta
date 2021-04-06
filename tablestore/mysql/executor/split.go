package executor

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/cznic/mathutil"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/zhihu/zetta/pkg/model"
	"github.com/zhihu/zetta/pkg/tablecodec"
	"github.com/zhihu/zetta/tablestore/mysql/sctx"
	"go.uber.org/zap"
)

type SplitIndexRegionExec struct {
	baseExecutor

	tableInfo *model.TableMeta
	//If isPrimary == true, then the indexInfo is nil.
	//Only support primarykey split for now.
	//isPrimary bool
	indexInfo *model.IndexMeta
	lower     []types.Datum
	upper     []types.Datum
	num       int

	done         bool
	valueLists   [][]types.Datum
	splitIdxKeys [][]byte
	splitRegionResult
}

type splitRegionResult struct {
	splitRegions     int
	finishScatterNum int
}

func (e *SplitIndexRegionExec) Open(ctx context.Context) (err error) {
	//Only support primary keys for now.
	//e.isPrimary = true
	e.splitIdxKeys, err = e.getSplitIdxKeys()
	return err
}

func (e *SplitIndexRegionExec) Next(ctx context.Context, chk *chunk.Chunk) error {
	chk.Reset()
	if e.done {
		return nil
	}
	e.done = true
	if err := e.splitIndexRegion(ctx); err != nil {
		return err
	}
	return nil
}

func (e *SplitIndexRegionExec) splitIndexRegion(ctx context.Context) error {
	store := e.ctx.GetStore()
	s, ok := store.(kv.SplittableStore)
	if !ok {
		return errors.New("Do not support split")
	}

	start := time.Now()
	ctxWithTimeout, cancel := context.WithTimeout(ctx, e.ctx.GetSessionVars().GetSplitRegionTimeout())
	defer cancel()
	regionIDs, err := s.SplitRegions(ctxWithTimeout, e.splitIdxKeys, true)
	if err != nil {
		return errors.New("Split region failed")
	}
	e.splitRegions = len(regionIDs)
	fmt.Println("split regions length:", e.splitRegions, len(e.splitIdxKeys))
	if e.splitRegions == 0 {
		return nil
	}

	e.finishScatterNum = waitScatterRegionFinish(ctxWithTimeout, e.ctx, start, s, regionIDs)
	return nil
}

func isCtxDone(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return true
	default:
		return false
	}
}

// checkScatterRegionFinishBackOff is the back off time that used to check if a region has finished scattering before split region timeout.
const checkScatterRegionFinishBackOff = 5

func waitScatterRegionFinish(ctxWithTimeout context.Context, sctx sctx.Context, startTime time.Time, store kv.SplittableStore, regionIDs []uint64) int {
	remainMillisecond := 0
	finishScatterNum := 0
	for _, regionID := range regionIDs {
		if isCtxDone(ctxWithTimeout) {
			// Do not break here for checking remain regions scatter finished with a very short backoff time.
			// Consider this situation -  Regions 1, 2, and 3 are to be split.
			// Region 1 times out before scattering finishes, while Region 2 and Region 3 have finished scattering.
			// In this case, we should return 2 Regions, instead of 0, have finished scattering.
			remainMillisecond = checkScatterRegionFinishBackOff
		} else {
			remainMillisecond = int((sctx.GetSessionVars().GetSplitRegionTimeout().Seconds() - time.Since(startTime).Seconds()) * 1000)
		}

		err := store.WaitScatterRegionFinish(ctxWithTimeout, regionID, remainMillisecond)
		if err == nil {
			finishScatterNum++
		} else {
			logutil.BgLogger().Warn("wait scatter region failed",
				zap.Uint64("regionID", regionID),
				zap.Error(err))
		}
	}
	return finishScatterNum
}

func (e *SplitIndexRegionExec) getSplitIdxKeys() (keys [][]byte, err error) {
	// Split index regions by user specified value lists.
	if len(e.valueLists) > 0 {
		return e.getSplitIdxKeysFromValueList()
	}
	return e.getSplitIdxKeysFromBound()
}

func (e *SplitIndexRegionExec) getSplitIdxKeysFromValueList() (keys [][]byte, err error) {
	pi := e.tableInfo.GetPartitionInfo()
	if pi == nil {
		keys = make([][]byte, 0, len(e.valueLists)+1)
		return e.getSplitIdxPhysicalKeysFromValueList(e.tableInfo.ID, keys)
	}

	return keys, nil
}

func (e *SplitIndexRegionExec) getSplitIdxPhysicalKeysFromValueList(physicalID int64, keys [][]byte) ([][]byte, error) {
	for _, v := range e.valueLists {
		idxKey, err := tablecodec.EncodeRecordKeyWide(nil, physicalID, v, 0)
		if err != nil {
			return nil, err
		}
		keys = append(keys, idxKey)
	}
	return keys, nil
}

func (e *SplitIndexRegionExec) getSplitIdxKeysFromBound() (keys [][]byte, err error) {
	tableId := e.tableInfo.Id
	keys = make([][]byte, 0, e.num)
	var (
		lowerIdx []byte
		upperIdx []byte
	)

	if e.indexInfo == nil {
		fmt.Println("split on primary key.", e.lower, e.upper)
		lowerIdx, err = tablecodec.EncodeRecordKeyWide(nil, tableId, e.lower, 0)
		if err != nil {
			return
		}
		upperIdx, err = tablecodec.EncodeRecordKeyWide(nil, tableId, e.upper, 0)
		if err != nil {
			return
		}
	} else {
		lowerIdx, err = tablecodec.EncodeIndexPrefix(nil, tableId, e.indexInfo.Id, e.lower)
		if err != nil {
			return
		}
		upperIdx, err = tablecodec.EncodeIndexPrefix(nil, tableId, e.indexInfo.Id, e.upper)
		if err != nil {
			return
		}
	}
	if bytes.Compare(lowerIdx, upperIdx) >= 0 {
		return nil, errors.Errorf("Split region lower value %v should less than the upper value %v", e.lower, e.upper)
	}
	return getValuesList(lowerIdx, upperIdx, e.num, keys), nil
}

// getValuesList is used to get `num` values between lower and upper value.
// To Simplify the explain, suppose lower and upper value type is int64, and lower=0, upper=100, num=10,
// then calculate the step=(upper-lower)/num=10, then the function should return 0+10, 10+10, 20+10... all together 9 (num-1) values.
// Then the function will return [10,20,30,40,50,60,70,80,90].
// The difference is the value type of upper,lower is []byte, So I use getUint64FromBytes to convert []byte to uint64.
func getValuesList(lower, upper []byte, num int, valuesList [][]byte) [][]byte {
	commonPrefixIdx := longestCommonPrefixLen(lower, upper)
	step := getStepValue(lower[commonPrefixIdx:], upper[commonPrefixIdx:], num)
	startV := getUint64FromBytes(lower[commonPrefixIdx:], 0)
	// To get `num` regions, only need to split `num-1` idx keys.
	buf := make([]byte, 8)
	for i := 0; i < num-1; i++ {
		value := make([]byte, 0, commonPrefixIdx+8)
		value = append(value, lower[:commonPrefixIdx]...)
		startV += step
		binary.BigEndian.PutUint64(buf, startV)
		value = append(value, buf...)
		valuesList = append(valuesList, value)
	}
	return valuesList
}

// longestCommonPrefixLen gets the longest common prefix byte length.
func longestCommonPrefixLen(s1, s2 []byte) int {
	l := mathutil.Min(len(s1), len(s2))
	i := 0
	for ; i < l; i++ {
		if s1[i] != s2[i] {
			break
		}
	}
	return i
}

// getStepValue gets the step of between the lower and upper value. step = (upper-lower)/num.
// Convert byte slice to uint64 first.
func getStepValue(lower, upper []byte, num int) uint64 {
	lowerUint := getUint64FromBytes(lower, 0)
	upperUint := getUint64FromBytes(upper, 0xff)
	fmt.Println("lower,upper, num:", lowerUint, upperUint, num)
	return (upperUint - lowerUint) / uint64(num)
}

// getUint64FromBytes gets a uint64 from the `bs` byte slice.
// If len(bs) < 8, then padding with `pad`.
func getUint64FromBytes(bs []byte, pad byte) uint64 {
	buf := bs
	if len(buf) < 8 {
		buf = make([]byte, 0, 8)
		buf = append(buf, bs...)
		for i := len(buf); i < 8; i++ {
			buf = append(buf, pad)
		}
	}
	return binary.BigEndian.Uint64(buf)
}
