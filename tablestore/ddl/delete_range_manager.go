package ddl

import (
	"context"
	"encoding/hex"
	"fmt"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/zhihu/zetta/pkg/model"
	"github.com/zhihu/zetta/pkg/tablecodec"
)

const (
	insertSQL = `INSERT INTO mysql.gc_delete_range VALUES(%d,%d,"%s","%s",%d);`
	selectSQL = `SELECT start_key, end_key FROM mysql.gc_delete_range WHERE ts<%d;`
	deleteSQL = `DELETE FROM mysql.gc_delete_range where start_key="%s" and end_key="%s";`
)

type deleteRangeManager struct {
	store    kv.Storage
	sessPool *sessionPool
}

func newDelRangeManager(store kv.Storage, sessPool *sessionPool) *deleteRangeManager {
	return &deleteRangeManager{
		store:    store,
		sessPool: sessPool,
	}
}

func (d *deleteRangeManager) clearRangeInfo(ctx context.Context, rg kv.KeyRange) error {
	startKeyEncoded := hex.EncodeToString(rg.StartKey)
	endKeyEncoded := hex.EncodeToString(rg.EndKey)
	sql := fmt.Sprintf(deleteSQL, startKeyEncoded, endKeyEncoded)
	se, err := d.sessPool.get()
	if err != nil {
		return errors.Trace(err)
	}
	defer d.sessPool.put(se)
	_, err = se.Execute(ctx, sql)
	return err
}

func (d *deleteRangeManager) getRangesToDelete(ctx context.Context, ts uint64) ([]kv.KeyRange, error) {
	ranges := make([]kv.KeyRange, 0)
	//sql := fmt.Sprintf(selectSQL, ts)
	sql := "SELECT start_key, end_key FROM mysql.gc_delete_range;"
	se, err := d.sessPool.get()
	if err != nil {
		return ranges, errors.Trace(err)
	}
	defer d.sessPool.put(se)
	recordSet, err := se.Execute(ctx, sql)
	if err != nil {
		return ranges, errors.Trace(err)
	}
	chk := recordSet.NewChunk()
	it := chunk.NewIterator4Chunk(chk)
	for {
		err = recordSet.Next(ctx, chk)
		if err != nil {
			return ranges, errors.Trace(err)
		}
		if chk.NumRows() == 0 {
			return ranges, nil
		}
		for row := it.Begin(); row != it.End(); row = it.Next() {
			startKey, err := hex.DecodeString(row.GetString(0))
			if err != nil {
				return nil, errors.Trace(err)
			}
			endKey, err := hex.DecodeString(row.GetString(1))
			if err != nil {
				return nil, errors.Trace(err)
			}
			rg := kv.KeyRange{
				StartKey: kv.Key(startKey),
				EndKey:   kv.Key(endKey),
			}
			ranges = append(ranges, rg)
		}
	}
}

func (d *deleteRangeManager) addDelRangeJob(job *model.Job) error {
	now, err := d.getNowTSO()
	if err != nil {
		return errors.Trace(err)
	}
	switch job.Type {
	case model.ActionDropTable, model.ActionTruncateTable:
		tableID := job.TableID
		startKey := tablecodec.EncodeTablePrefix(tableID)
		endKey := tablecodec.EncodeTablePrefix(tableID + 1)
		return d.doInsert(job.ID, tableID, startKey, endKey, now)
	}
	return nil
}

func (d *deleteRangeManager) doInsert(jobID, eleID int64, startKey, endKey kv.Key, ts uint64) error {
	se, err := d.sessPool.get()
	if err != nil {
		return err
	}
	defer d.sessPool.put(se)
	startKeyEncoded := hex.EncodeToString(startKey)
	endKeyEncoded := hex.EncodeToString(endKey)
	sql := fmt.Sprintf(insertSQL, jobID, eleID, startKeyEncoded, endKeyEncoded, ts)
	_, err = se.Execute(context.Background(), sql)
	return err
}

func (d *deleteRangeManager) getNowTSO() (uint64, error) {
	ver, err := d.store.CurrentVersion()
	if err != nil {
		return 0, err
	}
	return ver.Ver, nil
}
