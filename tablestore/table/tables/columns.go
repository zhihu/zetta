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
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/types"
	tspb "github.com/zhihu/zetta-proto/pkg/tablestore"
)

type columnEntry struct {
	id   int64
	name string
	t    *tspb.Type
	ft   *types.FieldType
	cfID int64
	cf   string
	idx  int
}

func (c columnEntry) ColumnName() string {
	if c.cf != DefaultColumnFamily {
		return c.cf + ":" + c.name
	}
	return c.name
}

type colSparseEntry struct {
	id     int64
	key    kv.Key
	val    []byte
	column string
	cf     string
	cfID   int64
	data   types.Datum
}

type Field struct {
	Family string
	Name   string
	Type   *tspb.Type
	Value  *tspb.Value
}
