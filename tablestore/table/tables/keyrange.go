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

package tables

import (
	"bytes"
	"fmt"

	"github.com/pingcap/tidb/kv"
	tspb "github.com/zhihu/zetta-proto/pkg/tablestore"
)

type keyRange struct {
	start, end             *tspb.ListValue
	startClosed, endClosed bool

	// These are populated during an operation
	// when we know what table this keyRange applies to.
	startKey, endKey []interface{}
}

func (r *keyRange) String() string {
	var sb bytes.Buffer // TODO: Switch to strings.Builder when we drop support for Go 1.9.
	if r.startClosed {
		sb.WriteString("[")
	} else {
		sb.WriteString("(")
	}
	fmt.Fprintf(&sb, "%v,%v", r.start, r.end)
	if r.endClosed {
		sb.WriteString("]")
	} else {
		sb.WriteString(")")
	}
	return sb.String()
}

type keyRangeList []*keyRange

func makeKeyRangeList(ranges []*tspb.KeyRange) keyRangeList {
	var krl keyRangeList
	for _, r := range ranges {
		krl = append(krl, makeKeyRange(r))
	}
	return krl
}

func makeKeyRange(r *tspb.KeyRange) *keyRange {
	var kr keyRange
	switch s := r.StartKeyType.(type) {
	case *tspb.KeyRange_StartClosed:
		kr.start = s.StartClosed
		kr.startClosed = true
	case *tspb.KeyRange_StartOpen:
		kr.start = s.StartOpen
	}
	switch e := r.EndKeyType.(type) {
	case *tspb.KeyRange_EndClosed:
		kr.end = e.EndClosed
		kr.endClosed = true
	case *tspb.KeyRange_EndOpen:
		kr.end = e.EndOpen
	}

	return &kr
}

type KeyValue struct {
	Key kv.Key
	Val []byte
}
