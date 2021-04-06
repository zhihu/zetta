package zstore

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/tikv"
)

var (
	scanBatchSize = 100
	batchGetSize  = 5120
)

type KvPair struct {
	Key   []byte
	Value []byte
	Err   error
}
type KvPairs struct {
	Keys [][]byte
	Vals [][]byte
	Err  error
}
type Scanner struct {
	rawkv        *tikv.RawKVClient
	batchSize    int
	cache        *KvPairs
	idx          int
	nextStartKey []byte
	endKey       []byte

	// Use for reverse scan.
	nextEndKey []byte
	reverse    bool

	valid bool
	eof   bool
}

func NewScanner(rawkv *tikv.RawKVClient, startKey []byte, endKey []byte, batchSize int, reverse bool) (*Scanner, error) {
	// It must be > 1. Otherwise scanner won't skipFirst.
	if batchSize <= 1 {
		batchSize = scanBatchSize
	}
	scanner := &Scanner{
		rawkv:        rawkv,
		batchSize:    batchSize,
		valid:        true,
		nextStartKey: startKey,
		endKey:       endKey,
		reverse:      reverse,
		nextEndKey:   endKey,
	}
	err := scanner.Next()
	if kv.IsErrNotFound(err) {
		return scanner, nil
	}
	return scanner, errors.Trace(err)
}

// Valid return valid.
func (s *Scanner) Valid() bool {
	return s.valid
}

// Key return key.
func (s *Scanner) Key() kv.Key {
	if s.valid {
		return s.cache.Keys[s.idx]
	}
	return nil
}

// Value return value.
func (s *Scanner) Value() []byte {
	if s.valid {
		return s.cache.Vals[s.idx]
	}
	return nil
}

// Next return next element.
func (s *Scanner) Next() error {
	if !s.valid {
		return errors.New("scanner iterator is invalid")
	}
	var err error
	for {
		s.idx++
		if s.cache == nil || s.idx >= len(s.cache.Keys) {
			if s.eof {
				s.Close()
				return nil
			}
			err = s.getData()
			if err != nil {
				s.Close()
				return errors.Trace(err)
			}
			if s.idx >= len(s.cache.Keys) {
				continue
			}
		}
		curKey := s.cache.Keys[s.idx]
		if (!s.reverse && (len(s.endKey) > 0 && kv.Key(curKey).Cmp(kv.Key(s.endKey)) >= 0)) ||
			(s.reverse && len(s.nextStartKey) > 0 && kv.Key(curKey).Cmp(kv.Key(s.nextStartKey)) < 0) {
			s.eof = true
			s.Close()
			return nil
		}
		return nil
	}
}

// Close close iterator.
func (s *Scanner) Close() {
	s.valid = false
}
func (s *Scanner) getData() error {
	var (
		reqEndKey, reqStartKey []byte
		keys, vals             [][]byte
		err                    error
	)
	for {
		if !s.reverse {
			reqStartKey = s.nextStartKey
			reqEndKey = s.endKey
			// if len(reqEndKey) >0 &&  bytes.Compare(reqEndKey, reqStartEnd)
			keys, vals, err = s.rawkv.Scan(reqStartKey, reqEndKey, s.batchSize)
			if err != nil {
				return errors.Trace(err)
			}
		} else {
			reqEndKey = s.nextEndKey
			keys, vals, err = s.rawkv.ReverseScan(reqEndKey, reqStartKey, s.batchSize)
			if err != nil {
				return errors.Trace(err)
			}
		}
		s.cache = &KvPairs{
			Keys: keys,
			Vals: vals,
		}
		s.idx = 0
		if len(s.cache.Keys) < s.batchSize {
			// No more data in future
			s.eof = true
			return nil
		}
		lastKey := keys[len(keys)-1]
		if !s.reverse {
			s.nextStartKey = []byte(kv.Key(lastKey).Next())
		} else {
			s.nextEndKey = lastKey
		}
		return nil
	}
}
