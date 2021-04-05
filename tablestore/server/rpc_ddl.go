package server

import (
	"context"

	"github.com/pingcap/tidb/util/mock"
	tspb "github.com/zhihu/zetta-proto/pkg/tablestore"
	"github.com/zhihu/zetta/pkg/model"
	"github.com/zhihu/zetta/tablestore/domain"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// CreateDatabase xxx
func (rs *RPCServer) CreateDatabase(ctx context.Context, req *tspb.CreateDatabaseRequest) (*tspb.CreateDatabaseResponse, error) {
	do := domain.GetOnlyDomain()
	dbMeta := model.NewDatabaseMetaFromPbReq(req)
	sctx := mock.NewContext() // Use mock to provide a default session context.
	err := do.DDL().CreateSchema(sctx, dbMeta)
	if err != nil {
		return nil, status.Errorf(codes.Internal, err.Error())
	}
	return &tspb.CreateDatabaseResponse{}, nil
}

// DeleteDatabase xxx
func (rs *RPCServer) DeleteDatabase(ctx context.Context, req *tspb.DeleteDatabaseRequest) (*tspb.DeleteDatabaseResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method DeleteDatabase not implemented")
}

func (rs *RPCServer) ListDatabase(ctx context.Context, req *tspb.ListDatabaseRequest) (*tspb.ListDatabaseResponse, error) {
	dbMetas := make([]*tspb.DatabaseMeta, 0)
	do := domain.GetOnlyDomain()
	dbs := do.InfoSchema().ListDatabases()
	for _, d := range dbs {
		dbMetas = append(dbMetas, &d.DatabaseMeta)
	}
	return &tspb.ListDatabaseResponse{
		Databases: dbMetas,
	}, nil
}

// CreateTable xxx
func (rs *RPCServer) CreateTable(ctx context.Context, req *tspb.CreateTableRequest) (*tspb.CreateTableResponse, error) {
	do := domain.GetOnlyDomain()
	tbMeta := model.NewTableMetaFromPbReq(req)
	sctx := mock.NewContext() // Use mock to provide a default session context.
	err := do.DDL().CreateTable(sctx, tbMeta, false)
	if err != nil {
		return nil, status.Errorf(codes.Internal, err.Error())
	}
	return &tspb.CreateTableResponse{}, nil
}

func (rs *RPCServer) AddColumn(ctx context.Context, req *tspb.AddColumnRequest) (*tspb.AddColumnResponse, error) {
	do := domain.GetOnlyDomain()
	sctx := mock.NewContext()
	colMetas := make([]*model.ColumnMeta, len(req.Columns))
	for i := range colMetas {
		colMetas[i] = model.NewColumnMetaFromPb(req.Columns[i])
	}
	err := do.DDL().AddColumn(sctx, req.Database, req.Table, colMetas)
	if err != nil {
		return nil, status.Errorf(codes.Internal, err.Error())
	}
	return &tspb.AddColumnResponse{}, nil
}

func (rs *RPCServer) ListTables(ctx context.Context,
	req *tspb.ListTableByDatabaseRequest) (*tspb.ListTableByDatabaseResponse, error) {
	do := domain.GetOnlyDomain()
	database := req.Database
	tbls := do.InfoSchema().ListTablesByDatabase(database)
	res := &tspb.ListTableByDatabaseResponse{}
	for _, t := range tbls {
		pbTable := t.TableMeta
		columns := make([]*tspb.ColumnMeta, 0)
		columnFamilies := make([]*tspb.ColumnFamilyMeta, 0)
		for _, c := range t.Columns {
			columns = append(columns, &c.ColumnMeta)
		}
		for _, cf := range t.ColumnFamilies {
			columnFamilies = append(columnFamilies, &cf.ColumnFamilyMeta)
		}
		pbTable.Columns = columns
		pbTable.ColumnFamilies = columnFamilies
		res.Tables = append(res.Tables, &pbTable)
	}
	return res, nil
}

// DropTable xxx
func (rs *RPCServer) DropTable(ctx context.Context, req *tspb.DropTableRequest) (*tspb.DropTableResponse, error) {
	do := domain.GetOnlyDomain()
	sctx := mock.NewContext()
	err := do.DDL().DropTable(sctx, req.Database, req.Table, false)
	if err != nil {
		return nil, status.Errorf(codes.Internal, err.Error())
	}
	return &tspb.DropTableResponse{}, nil
}

// AlterTable xxx
func (rs *RPCServer) AlterTable(ctx context.Context, req *tspb.AlterTableRequest) (*tspb.AlterTableResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method AlterTable not implemented")
}

// CreateIndex xxx
func (rs *RPCServer) CreateIndex(ctx context.Context, req *tspb.CreateIndexRequest) (*tspb.CreateIndexResponse, error) {
	do := domain.GetOnlyDomain()
	indexMeta := model.NewIndexMetaFromPbReq(req)
	sctx := mock.NewContext()
	err := do.DDL().CreateIndex(sctx, req.Database, req.Table, indexMeta, true)
	if err != nil {
		return nil, status.Errorf(codes.Internal, err.Error())
	}
	return &tspb.CreateIndexResponse{}, nil
}

// DropIndex xxx
func (rs *RPCServer) DropIndex(ctx context.Context, req *tspb.DropIndexRequest) (*tspb.DropIndexResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method DropIndex not implemented")
}
