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
	"context"
	"io"
	"sync/atomic"

	tspb "github.com/zhihu/zetta-proto/pkg/tablestore"
	"github.com/zhihu/zetta/tablestore/table"
)

var ()

type resultIter struct {
	columns   []*tspb.ColumnMeta
	rowChan   chan *tspb.SliceCell
	rowsCount int64
	limit     int64
	lastErr   error
}

func (ri *resultIter) Columns() []*tspb.ColumnMeta {

	return ri.columns
}

func (ri *resultIter) LastErr() error {
	return ri.lastErr
}

func (ri *resultIter) Next(ctx context.Context) (interface{}, error) {
	row, ok := <-ri.rowChan
	if !ok {
		return nil, io.EOF
	}
	return row, nil
}

func (ri *resultIter) Close() error {
	close(ri.rowChan)
	return ri.lastErr
}

func (ri *resultIter) sendData(row *tspb.SliceCell) error {
	if ri.limit > 0 && atomic.LoadInt64(&ri.rowsCount) > ri.limit {
		return table.ErrUserLimitReached
	}
	ri.rowChan <- row
	atomic.AddInt64(&ri.rowsCount, 1)
	return nil
}

func (ri *resultIter) clearup() {

}
