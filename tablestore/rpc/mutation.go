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

package rpc

import (
	"google.golang.org/grpc/codes"

	tspb "github.com/zhihu/zetta-proto/pkg/tablestore"
)

// Op is the mutation operation.
type OP int

const (
	// OpUnknown indicate unknown operation
	OpUnknown OP = iota
	// OpDelete removes a row from a table.  Succeeds whether or not the
	// key was present.
	OpDelete
	// OpInsert inserts a row into a table.  If the row already exists, the
	// write or transaction fails.
	OpInsert
	// OpInsertOrUpdate inserts a row into a table. If the row already
	// exists, it updates it instead.  Any column values not explicitly
	// written are preserved.
	OpInsertOrUpdate
	// OpReplace inserts a row into a table, deleting any existing row.
	// Unlike InsertOrUpdate, this means any values not explicitly written
	// become NULL.
	OpReplace
	// OpUpdate updates a row in a table.  If the row does not already
	// exist, the write or transaction fails.
	OpUpdate
)

// A Mutation describes a modification to one or more Cloud Spanner rows.  The
// mutation represents an insert, update, delete, etc on a table.
type Mutation struct {
	// op is the operation type of the mutation.
	// See documentation for spanner.op for more details.
	Op OP
	// Table is the name of the target table to be modified.
	Table string
	// keySet is a set of primary keys that names the rows
	// in a delete operation.
	KeySet *tspb.KeySet
	// columns names the set of columns that are going to be
	// modified by Insert, InsertOrUpdate, Replace or Update
	// operations.
	Columns []string
	// values specifies the new values for the target columns
	// named by Columns.
	Values     []interface{}
	ListValues []*tspb.ListValue
}

func (m *Mutation) FromProto(mutation *tspb.Mutation) error {
	switch mutation.GetOperation().(type) {
	case *tspb.Mutation_Insert:
		m.Op = OpInsert
		m.prepareMutationWrite(mutation.GetInsert())
	case *tspb.Mutation_InsertOrUpdate:
		m.Op = OpInsertOrUpdate
		m.prepareMutationWrite(mutation.GetInsertOrUpdate())
	case *tspb.Mutation_Update:
		m.Op = OpUpdate
		m.prepareMutationWrite(mutation.GetUpdate())
	case *tspb.Mutation_Replace:
		m.Op = OpReplace
		m.prepareMutationWrite(mutation.GetReplace())
	case *tspb.Mutation_Delete_:
		m.Op = OpDelete
		m.prepareMutationDelete(mutation.GetDelete())
	default:
		m.Op = OpUnknown
		return ErrInvdMutationOp(m.Op)
	}
	return nil
}

func (m *Mutation) prepareMutationWrite(w *tspb.Mutation_Write) {
	m.Table = w.Table
	m.Columns = w.Columns
	m.ListValues = w.Values
}

func (m *Mutation) prepareMutationDelete(d *tspb.Mutation_Delete) {
	m.Table = d.GetTable()
	m.KeySet = d.GetKeySet()
}

// ErrInvdMutationOp returns error for unrecognized mutation operation.
func ErrInvdMutationOp(op OP) error {
	return zettaErrorf(codes.InvalidArgument, "Unknown op type: %d", op)
}
