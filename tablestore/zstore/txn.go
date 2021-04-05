package zstore

import (
	"context"
	"fmt"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/kv"
)

var (
	_ kv.Transaction = (*zstoreTxn)(nil)
)

type zstoreTxn struct {
	kv.MemBuffer
	zstore   *ZStore
	vars     *kv.Variables
	mu       sync.Mutex // For thread-safe LockKeys function.
	valid    bool
	startTS  uint64
	readOnly bool
}

func newZStoreTxn(store *ZStore, startTS uint64) (*zstoreTxn, error) {
	return &zstoreTxn{
		zstore:    store,
		startTS:   startTS,
		valid:     true,
		MemBuffer: kv.NewMemDbBuffer(),
	}, nil
}

func (txn *zstoreTxn) NewStagingBuffer() kv.MemBuffer {
	return txn.MemBuffer.NewStagingBuffer()
}

func (txn *zstoreTxn) Get(ctx context.Context, k kv.Key) ([]byte, error) {
	v, err := txn.MemBuffer.Get(ctx, k)
	if kv.IsErrNotFound(err) {
		v, err = txn.zstore.rawkv.Get(k)
	}
	if err != nil {
		return v, err
	}
	if len(v) == 0 {
		return nil, kv.ErrNotExist
	}
	return v, nil
}

func (txn *zstoreTxn) Iter(k kv.Key, upperBound kv.Key) (kv.Iterator, error) {
	bufferIt, err := txn.MemBuffer.Iter(k, upperBound)
	if err != nil {
		return nil, err
	}

	scanner, err := NewScanner(txn.zstore.rawkv, k, upperBound, scanBatchSize, false)
	if err != nil {
		return nil, err
	}
	return kv.NewUnionIter(bufferIt, scanner, false)
}

func (txn *zstoreTxn) IterReverse(k kv.Key) (kv.Iterator, error) {
	bufferIt, err := txn.MemBuffer.IterReverse(k)
	if err != nil {
		return nil, err
	}
	scanner, err := NewScanner(txn.zstore.rawkv, nil, k, scanBatchSize, true)
	if err != nil {
		return nil, err
	}
	return kv.NewUnionIter(bufferIt, scanner, false)
}

func (txn *zstoreTxn) WalkBuffer(f func(k kv.Key, v []byte) error) error {
	return kv.WalkMemBuffer(txn.MemBuffer, f)
}

func (txn *zstoreTxn) Set(k kv.Key, v []byte) error {
	return txn.MemBuffer.Set(k, v)
}

func (txn *zstoreTxn) Delete(k kv.Key) error {
	return txn.MemBuffer.Delete(k)
}

func (txn *zstoreTxn) BatchGet(ctx context.Context, keys []kv.Key) (map[string][]byte, error) {
	var (
		storageValues = map[string][]byte{}
		bufferValues  = make([][]byte, len(keys))
		shrinkKeys    = make([][]byte, 0, len(keys))
	)

	for i, key := range keys {
		val, err := txn.MemBuffer.Get(ctx, key)
		if kv.IsErrNotFound(err) {
			shrinkKeys = append(shrinkKeys, key)
			continue
		}
		if err != nil {
			return nil, errors.Trace(err)
		}

		if len(val) != 0 {
			bufferValues[i] = val
		}
	}

	vals, err := txn.zstore.rawkv.BatchGet(shrinkKeys)
	if err != nil {
		return nil, errors.Trace(err)
	}

	for i, key := range shrinkKeys {
		storageValues[string(key)] = vals[i]
	}
	for i, key := range keys {
		if len(bufferValues[i]) == 0 {
			continue
		}
		storageValues[string(key)] = bufferValues[i]

	}
	return storageValues, nil
}

func (txn *zstoreTxn) Commit(context.Context) error {
	txn.mu.Lock()
	defer txn.mu.Unlock()

	var (
		putMuts *KvPairs
		delMuts *KvPairs
		err     error
	)

	sizeHint := txn.MemBuffer.Len()

	putMuts = &KvPairs{
		Keys: make([][]byte, 0, sizeHint),
		Vals: make([][]byte, 0, sizeHint),
		Err:  nil,
	}
	delMuts = &KvPairs{
		Keys: make([][]byte, 0, sizeHint),
		Err:  nil,
	}

	err = txn.WalkBuffer(func(k kv.Key, v []byte) error {
		if len(v) == 0 {
			delMuts.Keys = append(delMuts.Keys, k)
			return nil
		}
		putMuts.Keys = append(putMuts.Keys, k)
		putMuts.Vals = append(putMuts.Vals, v)
		return nil
	})
	if err != nil {
		return errors.Trace(err)
	}

	if len(putMuts.Keys) == 1 {
		if err = txn.zstore.rawkv.Put(putMuts.Keys[0], putMuts.Vals[0]); err != nil {
			return errors.Trace(err)
		}
	} else if len(putMuts.Keys) > 1 {
		if err = txn.zstore.rawkv.BatchPut(putMuts.Keys, putMuts.Vals); err != nil {
			return errors.Trace(err)
		}
	}
	if len(delMuts.Keys) == 1 {
		if err = txn.zstore.rawkv.Delete(delMuts.Keys[0]); err != nil {
			return errors.Trace(err)
		}
	} else if len(delMuts.Keys) > 1 {
		if err = txn.zstore.rawkv.BatchDelete(delMuts.Keys); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (txn *zstoreTxn) close() {
	txn.valid = false
}

// Rollback undoes the transaction operations to KV store.
func (txn *zstoreTxn) Rollback() error {
	if !txn.valid {
		return kv.ErrInvalidTxn
	}
	txn.Discard()
	txn.close()
	return nil
}

// String implements fmt.Stringer interface.
func (txn *zstoreTxn) String() string {
	return fmt.Sprintf("zstore-txn-%v", txn.startTS)
}

// LockKeys tries to lock the entries with the keys in KV store.
func (txn *zstoreTxn) LockKeys(ctx context.Context, lockCtx *kv.LockCtx, keys ...kv.Key) error {
	return nil
}

// SetOption sets an option with a value, when val is nil, uses the default
// value of this option.
func (txn *zstoreTxn) SetOption(opt kv.Option, val interface{}) {
}

// DelOption deletes an option.
func (txn *zstoreTxn) DelOption(opt kv.Option) {
}

// IsReadOnly checks if the transaction has only performed read operations.
func (txn *zstoreTxn) IsReadOnly() bool {
	return txn.readOnly
}

// StartTS returns the transaction start timestamp.
func (txn *zstoreTxn) StartTS() uint64 {
	return txn.startTS
}

// Valid returns if the transaction is valid.
// A transaction become invalid after commit or rollback.
func (txn *zstoreTxn) Valid() bool {
	return txn.valid
}

// GetMemBuffer return the MemBuffer binding to this transaction.
func (txn *zstoreTxn) GetMemBuffer() kv.MemBuffer {
	return txn.MemBuffer
}

func (txn *zstoreTxn) GetMemBufferSnapshot() kv.MemBuffer {
	return txn.MemBuffer
}

// GetSnapshot returns the Snapshot binding to this transaction.
func (txn *zstoreTxn) GetSnapshot() kv.Snapshot {
	return nil
}

// SetVars sets variables to the transaction.
func (txn *zstoreTxn) SetVars(vars *kv.Variables) {
	txn.vars = vars
}

// GetVars gets variables from the transaction.
func (txn *zstoreTxn) GetVars() *kv.Variables {
	return txn.vars
}

func (txn *zstoreTxn) IsPessimistic() bool {
	return true
}
