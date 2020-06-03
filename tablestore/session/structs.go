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

package session

import (
	"context"

	"github.com/pingcap/tidb/util/chunk"

	tspb "github.com/zhihu/zetta-proto/pkg/tablestore"
)

// ColumnInfo contains information of a column
type ColumnInfo struct {
	Schema             string
	Table              string
	OrgTable           string
	Name               string
	OrgName            string
	ColumnLength       uint32
	Decimal            uint8
	Type               uint8
	DefaultValueLength uint64
	DefaultValue       []byte
}

// ResultSet is the result set of an query.
type ResultSet interface {
	Columns() []*ColumnInfo
	NewChunk() *chunk.Chunk
	Next(context.Context, *chunk.Chunk) error
	StoreFetchedRows(rows []chunk.Row)
	GetFetchedRows() []chunk.Row
	Close() error
}

type RecordSet interface {
	Columns() []*tspb.ColumnMeta
	// Next(context.Context )
	Next(context.Context) (interface{}, error)

	LastErr() error
	Close() error
}

type recordSet struct {
}

func (r *recordSet) Columns() []*ColumnInfo {
	return nil
}

func (r *recordSet) NewChunk() *chunk.Chunk {
	return nil
}

func (r *recordSet) Next(ctx context.Context, req *chunk.Chunk) error {
	return nil
}

func (r *recordSet) StoreFetchedRows(rows []chunk.Row) {

}

func (r *recordSet) GetFetchedRows() []chunk.Row {
	return nil
}

func (r *recordSet) Close() error {
	return nil
}
