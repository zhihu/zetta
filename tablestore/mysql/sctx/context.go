package sctx

import (
	"context"
	//"fmt"

	//"github.com/pingcap/tidb/sessionctx/variable"

	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx"
)

type Context interface {
	sessionctx.Context
	// NewTxn creates a new transaction for further execution.
	// If old transaction is valid, it is committed first.
	// It's used in BEGIN statement and DDL statements to commit old transaction.
	//NewTxn(context.Context) error

	// Txn returns the current transaction which is created before executing a statement.
	// The returned kv.Transaction is not nil, but it maybe pending or invalid.
	// If the active parameter is true, call this function will wait for the pending txn
	// to become valid.
	GetTxn(active bool, raw bool) (kv.Transaction, error)
	CommitTxnWrapper(ctx context.Context) error

	// SetValue saves a value associated with this context for key.
	//SetValue(key fmt.Stringer, value interface{})

	// Value returns the value associated with this context for key.
	//Value(key fmt.Stringer) interface{}
	//GetSessionVars() *variable.SessionVars

	// PrepareTSFuture uses to prepare timestamp by future.
	//PrepareTSFuture(ctx context.Context)
	//GetStore() kv.Storage
}
