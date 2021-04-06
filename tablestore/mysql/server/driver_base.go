package server

import (
	"context"
	"fmt"
	"time"

	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/auth"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util"
)

type baseContext struct{}

// Status implements QueryCtx Status method.
func (tc *baseContext) Status() uint16 {
	return 1
}

func (tc *baseContext) GetHistorySQL() string {
	return ""
}

// LastInsertID implements QueryCtx LastInsertID method.
func (tc *baseContext) LastInsertID() uint64 {
	return 0
}

// Value implements QueryCtx Value method.
func (tc *baseContext) Value(key fmt.Stringer) interface{} {
	return nil
}

// SetValue implements QueryCtx SetValue method.
func (tc *baseContext) SetValue(key fmt.Stringer, value interface{}) {
}

// CommitTxn implements QueryCtx CommitTxn method.
func (tc *baseContext) CommitTxn(ctx context.Context) error {
	return nil
}

// SetProcessInfo implements QueryCtx SetProcessInfo method.
func (tc *baseContext) SetProcessInfo(sql string, t time.Time, command byte, maxExecutionTime uint64) {
}

// RollbackTxn implements QueryCtx RollbackTxn method.
func (tc *baseContext) RollbackTxn() {
}

// AffectedRows implements QueryCtx AffectedRows method.
func (tc *baseContext) AffectedRows() uint64 {
	return 0
}

// LastMessage implements QueryCtx LastMessage method.
func (tc *baseContext) LastMessage() string {
	return ""
}

// CurrentDB implements QueryCtx CurrentDB method.
func (tc *baseContext) CurrentDB() string {
	return ""
}

// WarningCount implements QueryCtx WarningCount method.
func (tc *baseContext) WarningCount() uint16 {
	return 0
}

// ExecuteStmt implements QueryCtx interface.
func (tc *baseContext) ExecuteStmt(ctx context.Context, stmt ast.StmtNode) (ResultSet, error) {
	return nil, nil
}

// Parse implements QueryCtx interface.
func (tc *baseContext) Parse(ctx context.Context, sql string) ([]ast.StmtNode, error) {
	return nil, nil
}

// SetSessionManager implements the QueryCtx interface.
func (tc *baseContext) SetSessionManager(sm util.SessionManager) {
}

// SetClientCapability implements QueryCtx SetClientCapability method.
func (tc *baseContext) SetClientCapability(flags uint32) {
}

// Close implements QueryCtx Close method.
func (tc *baseContext) Close() error {
	return nil
}

// Auth implements QueryCtx Auth method.
func (tc *baseContext) Auth(user *auth.UserIdentity, auth []byte, salt []byte) bool {
	return false
}

// FieldList implements QueryCtx FieldList method.
func (tc *baseContext) FieldList(table string) (columns []*ColumnInfo, err error) {
	return nil, nil
}

// GetStatement implements QueryCtx GetStatement method.
func (tc *baseContext) GetStatement(stmtID int) PreparedStatement {
	return nil
}

// Prepare implements QueryCtx Prepare method.
func (tc *baseContext) Prepare(sql string) (statement PreparedStatement, columns, params []*ColumnInfo, err error) {
	return
}

// ShowProcess implements QueryCtx ShowProcess method.
func (tc *baseContext) ShowProcess() *util.ProcessInfo {
	return nil
}

// SetCommandValue implements QueryCtx SetCommandValue method.
func (tc *baseContext) SetCommandValue(command byte) {
}

// GetSessionVars return SessionVars.
func (tc *baseContext) GetSessionVars() *variable.SessionVars {
	return nil
}
