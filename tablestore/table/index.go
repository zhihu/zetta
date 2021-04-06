// Copyright 2016 PingCAP, Inc.
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

package table

import (
	"context"
	"fmt"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/types"
	"strings"

	"bytes"
	parser_model "github.com/pingcap/parser/model"
	"github.com/zhihu/zetta/pkg/codec"

	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
)

// Handle is the ID of a row.
type Handle interface {
	// IsInt returns if the handle type is int64.
	IsInt() bool
	// IntValue returns the int64 value if IsInt is true, it panics if IsInt returns false.
	IntValue() int64
	// Next returns the minimum handle that is greater than this handle.
	Next() Handle
	// Equal returns if the handle equals to another handle, it panics if the types are different.
	Equal(h Handle) bool
	// Compare returns the comparison result of the two handles, it panics if the types are different.
	Compare(h Handle) int
	// Encoded returns the encoded bytes.
	Encoded() []byte
	// Len returns the length of the encoded bytes.
	Len() int
	// NumCols returns the number of columns of the handle,
	NumCols() int
	// EncodedCol returns the encoded column value at the given column index.
	EncodedCol(idx int) []byte
	// String implements the fmt.Stringer interface.
	String() string
}

// CommonHandle implements the Handle interface for non-int64 type handle.
type CommonHandle struct {
	encoded       []byte
	colEndOffsets []uint16
}

// NewCommonHandle creates a CommonHandle from a encoded bytes which is encoded by code.EncodeKey.
func NewCommonHandle(encoded []byte) (*CommonHandle, error) {
	ch := &CommonHandle{encoded: encoded}
	remain := encoded
	endOff := uint16(0)
	for len(remain) > 0 {
		var err error
		var col []byte
		col, remain, err = codec.CutOne(remain)
		if err != nil {
			return nil, err
		}
		endOff += uint16(len(col))
		ch.colEndOffsets = append(ch.colEndOffsets, endOff)
	}
	return ch, nil
}

// IsInt implements the Handle interface.
func (ch *CommonHandle) IsInt() bool {
	return false
}

// IntValue implements the Handle interface, not supported for CommonHandle type.
func (ch *CommonHandle) IntValue() int64 {
	panic("not supported in CommonHandle")
}

// Next implements the Handle interface.
func (ch *CommonHandle) Next() Handle {
	return &CommonHandle{
		encoded:       kv.Key(ch.encoded).PrefixNext(),
		colEndOffsets: ch.colEndOffsets,
	}
}

// Equal implements the Handle interface.
func (ch *CommonHandle) Equal(h Handle) bool {
	return !h.IsInt() && bytes.Equal(ch.encoded, h.Encoded())
}

// Compare implements the Handle interface.
func (ch *CommonHandle) Compare(h Handle) int {
	if h.IsInt() {
		panic("CommonHandle compares to IntHandle")
	}
	return bytes.Compare(ch.encoded, h.Encoded())
}

// Encoded implements the Handle interface.
func (ch *CommonHandle) Encoded() []byte {
	return ch.encoded
}

// Len implements the Handle interface.
func (ch *CommonHandle) Len() int {
	return len(ch.encoded)
}

// NumCols implements the Handle interface.
func (ch *CommonHandle) NumCols() int {
	return len(ch.colEndOffsets)
}

// EncodedCol implements the Handle interface.
func (ch *CommonHandle) EncodedCol(idx int) []byte {
	colStartOffset := uint16(0)
	if idx > 0 {
		colStartOffset = ch.colEndOffsets[idx-1]
	}
	return ch.encoded[colStartOffset:ch.colEndOffsets[idx]]
}

// String implements the Handle interface.
func (ch *CommonHandle) String() string {
	strs := make([]string, 0, ch.NumCols())
	for i := 0; i < ch.NumCols(); i++ {
		encodedCol := ch.EncodedCol(i)
		_, d, err := codec.DecodeOne(encodedCol)
		if err != nil {
			return err.Error()
		}
		str, err := d.ToString()
		if err != nil {
			return err.Error()
		}
		strs = append(strs, str)
	}
	return fmt.Sprintf("{%s}", strings.Join(strs, ", "))
}

// IndexIterator is the interface for iterator of index data on KV store.
type IndexIterator interface {
	Next() (k []types.Datum, h Handle, err error)
	Close()
}

// CreateIdxOpt contains the options will be used when creating an index.
type CreateIdxOpt struct {
	Ctx             context.Context
	SkipHandleCheck bool // If true, skip the handle constraint check.
	Untouched       bool // If true, the index key/value is no need to commit.
}

// CreateIdxOptFunc is defined for the Create() method of Index interface.
// Here is a blog post about how to use this pattern:
// https://dave.cheney.net/2014/10/17/functional-options-for-friendly-apis
type CreateIdxOptFunc func(*CreateIdxOpt)

// SkipHandleCheck is a defined value of CreateIdxFunc.
var SkipHandleCheck CreateIdxOptFunc = func(opt *CreateIdxOpt) {
	opt.SkipHandleCheck = true
}

// IndexIsUntouched uses to indicate the index kv is untouched.
var IndexIsUntouched CreateIdxOptFunc = func(opt *CreateIdxOpt) {
	opt.Untouched = true
}

// WithCtx returns a CreateIdxFunc.
// This option is used to pass context.Context.
func WithCtx(ctx context.Context) CreateIdxOptFunc {
	return func(opt *CreateIdxOpt) {
		opt.Ctx = ctx
	}
}

// Index is the interface for index data on KV store.
type Index interface {
	// Meta returns IndexInfo.
	Meta() *parser_model.IndexInfo
	// Create supports insert into statement.
	Create(ctx sessionctx.Context, rm kv.RetrieverMutator, indexedValues []types.Datum, h Handle, opts ...CreateIdxOptFunc) (Handle, error)
	// Delete supports delete from statement.
	Delete(sc *stmtctx.StatementContext, m kv.Mutator, indexedValues []types.Datum, h Handle) error
	// Drop supports drop table, drop index statements.
	Drop(rm kv.RetrieverMutator) error
	// Exist supports check index exists or not.
	Exist(sc *stmtctx.StatementContext, rm kv.RetrieverMutator, indexedValues []types.Datum, h Handle) (bool, Handle, error)
	// GenIndexKey generates an index key.
	GenIndexKey(sc *stmtctx.StatementContext, indexedValues []types.Datum, h Handle, buf []byte) (key []byte, distinct bool, err error)
	// Seek supports where clause.
	Seek(sc *stmtctx.StatementContext, r kv.Retriever, indexedValues []types.Datum) (iter IndexIterator, hit bool, err error)
	// SeekFirst supports aggregate min and ascend order by.
	SeekFirst(r kv.Retriever) (iter IndexIterator, err error)
	// FetchValues fetched index column values in a row.
	// Param columns is a reused buffer, if it is not nil, FetchValues will fill the index values in it,
	// and return the buffer, if it is nil, FetchValues will allocate the buffer instead.
	FetchValues(row []types.Datum, columns []types.Datum) ([]types.Datum, error)
}
