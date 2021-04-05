package executor

import (
	"context"

	"github.com/pingcap/parser/ast"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/mock"
	"github.com/zhihu/zetta/pkg/model"
	"github.com/zhihu/zetta/tablestore/domain"
	"github.com/zhihu/zetta/tablestore/infoschema"
)

// DDLExec represents a DDL executor.
// It grabs a DDL instance from Domain, calling the DDL methods to do the work.
type DDLExec struct {
	baseExecutor

	stmt ast.StmtNode
	is   infoschema.InfoSchema
	done bool
}

func (e *DDLExec) Next(ctx context.Context, req *chunk.Chunk) (err error) {
	if e.done {
		return nil
	}
	e.done = true
	// TODO: For each DDL, we should commit the previous transaction and create a new transaction.
	switch x := e.stmt.(type) {
	case *ast.CreateDatabaseStmt:
		err = e.executeCreateDatabase(x)
	case *ast.CreateTableStmt:
		err = e.executeCreateTable(x)
	case *ast.DropTableStmt:
		err = e.executeDropTable(x)
	case *ast.CreateIndexStmt:
		err = e.executeCreateIndex(x)
	case *ast.AlterTableStmt:
		err = e.executeAlterTable(x)
	}
	return err
}

//Ony support add column for now.
func (e *DDLExec) executeAlterTable(s *ast.AlterTableStmt) error {
	tblName := s.Table.Name.L
	dbName := s.Table.Schema.L
	if dbName == "" {
		dbName = e.ctx.GetSessionVars().CurrentDB
	}
	colMeta := toAddCol(dbName, tblName, s.Specs[0].NewColumns[0])
	return domain.GetOnlyDomain().DDL().AddColumn(mock.NewContext(), dbName, tblName, []*model.ColumnMeta{colMeta})
}

func (e *DDLExec) executeCreateIndex(s *ast.CreateIndexStmt) error {
	idxMeta := toCreateIndex(s)
	currentDB := e.ctx.GetSessionVars().CurrentDB
	tbName := idxMeta.TableName
	err := domain.GetOnlyDomain().DDL().CreateIndex(mock.NewContext(), currentDB, tbName, idxMeta, s.IfNotExists)
	return err
}

func (e *DDLExec) executeCreateDatabase(s *ast.CreateDatabaseStmt) error {
	dbMeta := toCreateDatabase(s)
	err := domain.GetOnlyDomain().DDL().CreateSchema(mock.NewContext(), dbMeta)
	return err
}

func (e *DDLExec) executeCreateTable(s *ast.CreateTableStmt) error {
	currentDB := e.ctx.GetSessionVars().CurrentDB
	tbMeta := toCreateTable(s)
	tbMeta.Database = currentDB
	if len(tbMeta.Rules) != 0 {
		tbMeta.Rules[0].GroupID = "pd"
		tbMeta.Rules[0].ID = currentDB + ":" + tbMeta.TableName
	}
	ctx := mock.NewContext()
	err := domain.GetOnlyDomain().DDL().CreateTable(ctx, tbMeta, s.IfNotExists)
	return err
}

func (e *DDLExec) executeDropTable(s *ast.DropTableStmt) error {
	currentDB := e.ctx.GetSessionVars().CurrentDB
	for _, table := range s.Tables {
		err := domain.GetOnlyDomain().DDL().DropTable(mock.NewContext(), currentDB, table.Name.L, s.IfExists)
		if err != nil {
			return err
		}
	}
	return nil
}
