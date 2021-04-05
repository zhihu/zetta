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

package hthrift

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/mock"
	"github.com/spf13/cast"
	"github.com/uber/jaeger-client-go/thrift"
	tspb "github.com/zhihu/zetta-proto/pkg/tablestore"
	"github.com/zhihu/zetta/pkg/meta"
	"github.com/zhihu/zetta/pkg/model"
	"github.com/zhihu/zetta/tablestore/domain"
	"github.com/zhihu/zetta/tablestore/server/hthrift/hbase"
	"github.com/zhihu/zetta/tablestore/session"
	tst "github.com/zhihu/zetta/tablestore/table"
	"github.com/zhihu/zetta/tablestore/table/tables"
	"go.uber.org/zap"
)

const (
	LATEST_TIMESTAMP = math.MaxInt64
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

type HBaseHandler struct {
	ts            *TServer
	nextScannerID int64
	scannerMap    map[int64]*TScanner
	mu            sync.RWMutex
}

func NewHBaseHandler(ts *TServer) *HBaseHandler {
	return &HBaseHandler{
		ts:         ts,
		scannerMap: make(map[int64]*TScanner),
	}
}

func (h *HBaseHandler) addScanner(scanner *TScanner) int64 {
	scannerID := atomic.AddInt64(&h.nextScannerID, 1)
	h.mu.Lock()
	defer h.mu.Unlock()
	h.scannerMap[scannerID] = scanner
	scanner.scannerID = scannerID
	return scannerID

}

func (h *HBaseHandler) getScanner(id int64) *TScanner {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.scannerMap[int64(id)]
}

func (h *HBaseHandler) removeScanner(id int64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.scannerMap, int64(id))
}

// Brings a table on-line (enables it)
//
// Parameters:
//  - TableName: name of the table

func (h *HBaseHandler) EnableTable(ctx context.Context, tableName hbase.Bytes) (err error) {
	return nil
}

// Disables a table (takes it off-line) If it is being served, the master
// will tell the servers to stop serving it.
//
// Parameters:
//  - TableName: name of the table
func (h *HBaseHandler) DisableTable(ctx context.Context, tableName hbase.Bytes) (err error) {
	return nil
}

// @return true if table is on-line
//
// Parameters:
//  - TableName: name of the table to check
func (h *HBaseHandler) IsTableEnabled(ctx context.Context, tableName hbase.Bytes) (r bool, err error) {
	return true, nil
}

// Parameters:
//  - TableNameOrRegionName
func (h *HBaseHandler) Compact(ctx context.Context, tableNameOrRegionName hbase.Bytes) (err error) {
	return nil
}

// Parameters:
//  - TableNameOrRegionName
func (h *HBaseHandler) MajorCompact(ctx context.Context, tableNameOrRegionName hbase.Bytes) (err error) {
	return nil
}

// List all the userspace tables.
//
// @return returns a list of names
func (h *HBaseHandler) GetTableNames(ctx context.Context) (r []hbase.Text, err error) {
	domain := domain.GetOnlyDomain()
	dbs := domain.InfoSchema().ListDatabases()
	r = []hbase.Text{}
	for _, db := range dbs {
		tables := domain.InfoSchema().ListTablesByDatabase(db.Database)
		for _, t := range tables {
			tableName := db.Database + ":" + t.TableName
			r = append(r, hbase.Text(tableName))
		}
	}
	return r, nil

}

// List all the column families assoicated with a table.
//
// @return list of column family descriptors
//
// Parameters:
//  - TableName: table name
func (h *HBaseHandler) GetColumnDescriptors(ctx context.Context, tableName hbase.Text) (r map[string]*hbase.ColumnDescriptor, err error) {
	db, tn, err := getDBTable(tableName)
	if err != nil {
		return nil, buildIOError(err)
	}
	domain := domain.GetOnlyDomain()
	tt, err := domain.InfoSchema().GetTableByName(db, tn)
	if err != nil {
		logutil.Logger(ctx).Error("prepare zetta table info error", zap.Error(err))
		return nil, buildIOError(err.Error())
	}
	// tt.SetTxn(txn)
	tableMeta := tt.Meta()
	r = make(map[string]*hbase.ColumnDescriptor)
	for _, cf := range tableMeta.ColumnFamilies {
		if !tables.IsFlexible(cf) {
			continue
		}

		colDesc := &hbase.ColumnDescriptor{
			Name:       hbase.Text(cf.Name),
			TimeToLive: 604800,
		}
		colDesc.BlockCacheEnabled = cast.ToBool(cf.Attributes["BLOCKCACHE"])
		colDesc.TimeToLive = cast.ToInt32(cf.Attributes["TTL"])
		colDesc.BloomFilterType = cf.Attributes["BLOOMFILTER"]
		colDesc.Compression = cf.Attributes["COMPRESSION"]
		colDesc.MaxVersions = cast.ToInt32(cf.Attributes["VERSION"])
		colDesc.InMemory = cast.ToBool(cf.Attributes["IN_MEMORY"])
		r[cf.Name] = colDesc
	}
	return r, nil
}

// List the regions associated with a table.
//
// @return list of region descriptors
//
// Parameters:
//  - TableName: table name
func (h *HBaseHandler) GetTableRegions(ctx context.Context, tableName hbase.Text) (r []*hbase.TRegionInfo, err error) {
	return nil, nil
}

// Create a table with the specified column families.  The name
// field for each ColumnDescriptor must be set and must end in a
// colon (:). All other fields are optional and will get default
// values if not explicitly specified.
//
// @throws IllegalArgument if an input parameter is invalid
//
// @throws AlreadyExists if the table name already exists
//
// Parameters:
//  - TableName: name of table to create
//  - ColumnFamilies: list of column family descriptors
func (h *HBaseHandler) CreateTable(ctx context.Context, tableName hbase.Text, columnFamilies []*hbase.ColumnDescriptor) (err error) {
	db, tn, err := getDBTable(tableName)
	if err != nil {
		return buildIOError(err)
	}

	do := domain.GetOnlyDomain()
	_, err = do.InfoSchema().GetTableMetaByName(db, tn)
	if err == meta.ErrDBNotExists {
		return &hbase.IllegalArgument{
			Message: fmt.Sprintf("%s not exists", db),
		}
	} else if err == nil {
		return &hbase.AlreadyExists{
			Message: fmt.Sprintf("%s already exists", string(tableName)),
		}
	}
	req := &tspb.CreateTableRequest{
		Database: db,
		TableMeta: &tspb.TableMeta{
			Database:       db,
			TableName:      tn,
			ColumnFamilies: []*tspb.ColumnFamilyMeta{},
			Columns: []*tspb.ColumnMeta{
				{Name: "rowkey", ColumnType: &tspb.Type{Code: tspb.TypeCode_BYTES}, Family: "default"},
			},
			PrimaryKey: []string{"rowkey"},
		},
	}
	for _, cf := range columnFamilies {
		if bytes.HasSuffix(cf.Name, []byte(":")) {
			cf.Name = bytes.TrimRight(cf.Name, ":")
		}
		cfMeta := &tspb.ColumnFamilyMeta{
			Name: string(cf.Name),
			Attributes: map[string]string{
				"mode":   "flexible",
				"layout": "sparse",
			},
		}
		cfMeta.Attributes["BLOCKCACHE"] = cast.ToString(cf.BlockCacheEnabled)
		cfMeta.Attributes["BLOOMFILTER"] = cf.BloomFilterType
		cfMeta.Attributes["COMPRESSION"] = cf.Compression
		cfMeta.Attributes["VERSION"] = cast.ToString(cf.MaxVersions)
		cfMeta.Attributes["TTL"] = cast.ToString(cf.TimeToLive)
		cfMeta.Attributes["IN_MEMORY"] = cast.ToString(cf.InMemory)

		if cf.TimeToLive == -1 {
			cfMeta.Attributes["TTL"] = cast.ToString(math.MaxInt32)
		}
		req.TableMeta.ColumnFamilies = append(req.TableMeta.ColumnFamilies, cfMeta)
	}

	tabMeta := model.NewTableMetaFromPbReq(req)
	if err := do.DDL().CreateTable(mock.NewContext(), tabMeta, false); err != nil {
		return &hbase.IOError{
			Message: err.Error(),
		}
	}
	return nil
}

// Deletes a table
//
// @throws IOError if table doesn't exist on server or there was some other
// problem
//
// Parameters:
//  - TableName: name of table to delete
func (h *HBaseHandler) DeleteTable(ctx context.Context, tableName hbase.Text) (err error) {
	return nil
}

// Get a single TCell for the specified table, row, and column at the
// latest timestamp. Returns an empty list if no such value exists.
//
// @return value for specified row/column
//
// Parameters:
//  - TableName: name of table
//  - Row: row key
//  - Column: column name
//  - Attributes: Get attributes
func (h *HBaseHandler) Get(ctx context.Context, tableName hbase.Text, row hbase.Text, column hbase.Text, attributes map[string]hbase.Text) (r []*hbase.TCell, err error) {
	return h.GetVerTs(ctx, tableName, row, column, LATEST_TIMESTAMP, 1, attributes)
}

// Get the specified number of versions for the specified table,
// row, and column.
//
// @return list of cells for specified row/column
//
// Parameters:
//  - TableName: name of table
//  - Row: row key
//  - Column: column name
//  - NumVersions: number of versions to retrieve
//  - Attributes: Get attributes
func (h *HBaseHandler) GetVer(ctx context.Context, tableName hbase.Text, row hbase.Text, column hbase.Text, numVersions int32, attributes map[string]hbase.Text) (r []*hbase.TCell, err error) {
	return h.GetVerTs(ctx, tableName, row, column, LATEST_TIMESTAMP, 1, attributes)
}

// Get the specified number of versions for the specified table,
// row, and column.  Only versions less than or equal to the specified
// timestamp will be returned.
//
// @return list of cells for specified row/column
//
// Parameters:
//  - TableName: name of table
//  - Row: row key
//  - Column: column name
//  - Timestamp: timestamp
//  - NumVersions: number of versions to retrieve
//  - Attributes: Get attributes
func (h *HBaseHandler) GetVerTs(ctx context.Context, tableName hbase.Text, row hbase.Text, column hbase.Text, timestamp int64, numVersions int32, attributes map[string]hbase.Text) (r []*hbase.TCell, err error) {

	if column == nil || len(column) == 0 {
		return nil, buildIOError("column should be specific")
	}

	family, col, err := getFamilyQualifier([]byte(column))
	if err != nil {
		return nil, buildIOError(err)
	}
	db, tn, err := getDBTable(tableName)
	if err != nil {
		return nil, buildIOError(err)
	}
	resource, err := h.ts.sessPool.Get()
	if err != nil {
		return nil, buildIOError(err)
	}
	defer h.ts.sessPool.Put(resource)
	txnSel := &tspb.TransactionSelector{
		Selector: &tspb.TransactionSelector_SingleUse{
			SingleUse: &tspb.TransactionOptions{
				Mode: &tspb.TransactionOptions_ReadWrite_{},
			},
		},
	}
	sess := resource.(session.Session)
	sess.SetDB(db)

	isRawkv, err := sess.RawkvAccess(ctx, tn)
	if err != nil {
		return nil, buildIOError(err.Error())
	}
	rtOpt := session.RetrieveTxnOpt{
		Committable: false,
		IsRawKV:     isRawkv,
	}

	txn, err := sess.RetrieveTxn(ctx, txnSel, &rtOpt)
	if err != nil {
		return nil, buildIOError(err)
	}
	rkey := &tspb.ListValue{
		Values: []*tspb.Value{
			{Kind: &tspb.Value_BytesValue{BytesValue: []byte(row)}},
		},
	}
	tsrow := &tspb.Row{
		Keys:       rkey,
		Qualifiers: nil,
	}
	if col != "" {
		tsrow.Qualifiers = []string{col}
	} else {
		tsrow.Qualifiers = []string{}
	}
	req := &tspb.SparseReadRequest{
		Table:  tn,
		Family: family,
		Rows:   []*tspb.Row{tsrow},
	}

	ri, err := sess.HandleRead(ctx, req, txn)
	if err != nil {
		logutil.BgLogger().Error("getVerTs error", zap.Error(err))
		return nil, thrift.NewTApplicationException(thrift.INTERNAL_ERROR, err.Error())
	}
	r = []*hbase.TCell{}
	for {
		rsliceCell, err := ri.Next(ctx)
		if err != nil {
			break
		}
		rscell := rsliceCell.(*tspb.SliceCell)
		for _, cell := range rscell.Cells {
			tcell := &hbase.TCell{Value: cell.Value.GetBytesValue()}
			r = append(r, tcell)
		}
	}
	if ri.LastErr() != nil {
		return nil, buildIOError(ri.LastErr())
	}

	return r, nil
}

// Get all the data for the specified table and row at the latest
// timestamp. Returns an empty list if the row does not exist.
//
// @return TRowResult containing the row and map of columns to TCells
//
// Parameters:
//  - TableName: name of table
//  - Row: row key
//  - Attributes: Get attributes
func (h *HBaseHandler) GetRow(ctx context.Context, tableName hbase.Text, row hbase.Text, attributes map[string]hbase.Text) (r []*hbase.TRowResult_, err error) {
	// log.Printf("GetRow tableName: %v, row: %v", string(tableName), string(row))
	return h.GetRowWithColumnsTs(ctx, tableName, row, nil, LATEST_TIMESTAMP, attributes)
}

// Get the specified columns for the specified table and row at the latest
// timestamp. Returns an empty list if the row does not exist.
//
// @return TRowResult containing the row and map of columns to TCells
//
// Parameters:
//  - TableName: name of table
//  - Row: row key
//  - Columns: List of columns to return, null for all columns
//  - Attributes: Get attributes
func (h *HBaseHandler) GetRowWithColumns(ctx context.Context, tableName hbase.Text, row hbase.Text, columns []hbase.Text, attributes map[string]hbase.Text) (r []*hbase.TRowResult_, err error) {
	// log.Printf("GetRowWithColumns tableName: %v, row: %v", string(tableName), string(row))
	return h.GetRowWithColumnsTs(ctx, tableName, row, columns, LATEST_TIMESTAMP, attributes)

}

// Get all the data for the specified table and row at the specified
// timestamp. Returns an empty list if the row does not exist.
//
// @return TRowResult containing the row and map of columns to TCells
//
// Parameters:
//  - TableName: name of the table
//  - Row: row key
//  - Timestamp: timestamp
//  - Attributes: Get attributes
func (h *HBaseHandler) GetRowTs(ctx context.Context, tableName hbase.Text, row hbase.Text, timestamp int64, attributes map[string]hbase.Text) (r []*hbase.TRowResult_, err error) {
	// log.Printf("GetRowTs tableName: %v, row: %v", string(tableName), string(row))
	return h.GetRowWithColumnsTs(ctx, tableName, row, nil, timestamp, attributes)
}

// Get the specified columns for the specified table and row at the specified
// timestamp. Returns an empty list if the row does not exist.
//
// @return TRowResult containing the row and map of columns to TCells
//
// Parameters:
//  - TableName: name of table
//  - Row: row key
//  - Columns: List of columns to return, null for all columns
//  - Timestamp
//  - Attributes: Get attributes
func (h *HBaseHandler) GetRowWithColumnsTs(ctx context.Context, tableName hbase.Text, row hbase.Text, columns []hbase.Text, timestamp int64, attributes map[string]hbase.Text) (r []*hbase.TRowResult_, err error) {
	// startAt := time.Now()
	// log.Printf("GetRowWithColumnsTs, tableName: %v, row: %v, timestamp: %v", string(tableName), string(row), timestamp)

	db, tn, err := getDBTable(tableName)
	if err != nil {
		return nil, buildIOError(err)
	}

	reqs, err := buildSparseReadReq(tn, []hbase.Text{row}, columns)
	if err != nil {
		return nil, buildIOError(err)
	}

	resource, err := h.ts.sessPool.Get()
	if err != nil {
		return nil, buildIOError(err)
	}
	defer h.ts.sessPool.Put(resource)
	sess := resource.(session.Session)
	sess.SetDB(db)

	txnSel := &tspb.TransactionSelector{
		Selector: &tspb.TransactionSelector_SingleUse{
			SingleUse: &tspb.TransactionOptions{
				Mode: &tspb.TransactionOptions_ReadWrite_{},
			},
		},
	}

	isRawkv, err := sess.RawkvAccess(ctx, tn)
	if err != nil {
		return nil, buildIOError(err.Error())
	}
	rtOpt := session.RetrieveTxnOpt{
		Committable: false,
		IsRawKV:     isRawkv,
	}

	txn, err := sess.RetrieveTxn(ctx, txnSel, &rtOpt)
	if err != nil {
		return nil, buildIOError(err)
	}

	results := make([]*hbase.TRowResult_, 1)
	result := &hbase.TRowResult_{
		Row:     row,
		Columns: make(map[string]*hbase.TCell),
	}
	for _, req := range reqs {
		ri, err := sess.HandleRead(ctx, req, txn)
		if err != nil {
			logutil.BgLogger().Error("GetRowWithColumnsTs", zap.Error(err))
			return nil, buildIOError(err)
		}

		for {
			rsliceCell, err := ri.Next(ctx)
			if err != nil {
				break
			}

			rscell := rsliceCell.(*tspb.SliceCell)

			for _, cell := range rscell.Cells {
				tcell := &hbase.TCell{Value: cell.Value.GetBytesValue()}
				columnName := cell.Family + ":" + cell.Column
				result.Columns[columnName] = tcell
			}
		}
		if ri.LastErr() != nil {
			return nil, buildIOError(ri.LastErr())
		}
	}
	results[0] = result
	return results, nil
}

// Get all the data for the specified table and rows at the latest
// timestamp. Returns an empty list if no rows exist.
//
// @return TRowResult containing the rows and map of columns to TCells
//
// Parameters:
//  - TableName: name of table
//  - Rows: row keys
//  - Attributes: Get attributes
func (h *HBaseHandler) GetRows(ctx context.Context, tableName hbase.Text, rows []hbase.Text, attributes map[string]hbase.Text) (r []*hbase.TRowResult_, err error) {

	return h.GetRowsWithColumnsTs(ctx, tableName, rows, nil, LATEST_TIMESTAMP, attributes)
}

// Get the specified columns for the specified table and rows at the latest
// timestamp. Returns an empty list if no rows exist.
//
// @return TRowResult containing the rows and map of columns to TCells
//
// Parameters:
//  - TableName: name of table
//  - Rows: row keys
//  - Columns: List of columns to return, null for all columns
//  - Attributes: Get attributes
func (h *HBaseHandler) GetRowsWithColumns(ctx context.Context, tableName hbase.Text, rows []hbase.Text, columns []hbase.Text, attributes map[string]hbase.Text) (r []*hbase.TRowResult_, err error) {
	return h.GetRowsWithColumnsTs(ctx, tableName, rows, columns, LATEST_TIMESTAMP, attributes)
}

// Get all the data for the specified table and rows at the specified
// timestamp. Returns an empty list if no rows exist.
//
// @return TRowResult containing the rows and map of columns to TCells
//
// Parameters:
//  - TableName: name of the table
//  - Rows: row keys
//  - Timestamp: timestamp
//  - Attributes: Get attributes
func (h *HBaseHandler) GetRowsTs(ctx context.Context, tableName hbase.Text, rows []hbase.Text, timestamp int64, attributes map[string]hbase.Text) (r []*hbase.TRowResult_, err error) {
	return h.GetRowsWithColumnsTs(ctx, tableName, rows, nil, LATEST_TIMESTAMP, attributes)
}

// Get the specified columns for the specified table and rows at the specified
// timestamp. Returns an empty list if no rows exist.
//
// @return TRowResult containing the rows and map of columns to TCells
//
// Parameters:
//  - TableName: name of table
//  - Rows: row keys
//  - Columns: List of columns to return, null for all columns
//  - Timestamp
//  - Attributes: Get attributes
func (h *HBaseHandler) GetRowsWithColumnsTs(ctx context.Context, tableName hbase.Text, rows []hbase.Text, columns []hbase.Text, timestamp int64, attributes map[string]hbase.Text) (r []*hbase.TRowResult_, err error) {
	db, tn, err := getDBTable(tableName)
	if err != nil {
		return nil, buildIOError(err)
	}

	reqs, err := buildSparseReadReq(tn, rows, columns)
	if err != nil {
		return nil, buildIOError(err)
	}

	resource, err := h.ts.sessPool.Get()
	if err != nil {
		return nil, buildIOError(err)
	}
	defer h.ts.sessPool.Put(resource)
	sess := resource.(session.Session)
	sess.SetDB(db)

	txnSel := &tspb.TransactionSelector{
		Selector: &tspb.TransactionSelector_SingleUse{
			SingleUse: &tspb.TransactionOptions{
				Mode: &tspb.TransactionOptions_ReadWrite_{},
			},
		},
	}

	isRawkv, err := sess.RawkvAccess(ctx, tn)
	if err != nil {
		return nil, buildIOError(err.Error())
	}
	rtOpt := session.RetrieveTxnOpt{
		Committable: false,
		IsRawKV:     isRawkv,
	}
	txn, err := sess.RetrieveTxn(ctx, txnSel, &rtOpt)
	if err != nil {
		return nil, buildIOError(err)
	}

	results := make([]*hbase.TRowResult_, 0)
	sliceCells := make([]*tspb.SliceCell, 0)
	rowCells := make(map[string]map[string]*hbase.TCell)

	for _, req := range reqs {
		ri, err := sess.HandleRead(ctx, req, txn)
		if err != nil {
			logutil.BgLogger().Error("GetRowsWithColumnsTs", zap.Error(err))
			return nil, buildIOError(err)
		}
		for {
			rsliceCell, err := ri.Next(ctx)
			if err != nil {
				break
			}
			rscell := rsliceCell.(*tspb.SliceCell)
			sliceCells = append(sliceCells, rscell)
		}
		if ri.LastErr() != nil {
			return nil, buildIOError(ri.LastErr())
		}
	}
	for _, scell := range sliceCells {
		rowkey := scell.PrimaryKeys[0].GetBytesValue()
		columns, ok := rowCells[string(rowkey)]
		if !ok {
			columns = map[string]*hbase.TCell{}
			rowCells[string(rowkey)] = columns
		}
		for _, cell := range scell.Cells {
			tcell := &hbase.TCell{Value: cell.Value.GetBytesValue()}
			columnName := cell.Family + ":" + cell.Column
			columns[columnName] = tcell
		}
	}
	for _, row := range rows {
		columns := rowCells[string(row)]
		if columns == nil {
			continue
		}
		result := &hbase.TRowResult_{
			Row:     row,
			Columns: columns,
		}
		results = append(results, result)
	}
	return results, nil
}

// Apply a series of mutations (updates/deletes) to a row in a
// single transaction.  If an exception is thrown, then the
// transaction is aborted.  Default current timestamp is used, and
// all entries will have an identical timestamp.
//
// Parameters:
//  - TableName: name of table
//  - Row: row key
//  - Mutations: list of mutation commands
//  - Attributes: Mutation attributes
func (h *HBaseHandler) MutateRow(ctx context.Context, tableName hbase.Text, row hbase.Text, mutations []*hbase.Mutation, attributes map[string]hbase.Text) (err error) {
	log.Printf("MutateRow Table: %v, Row: %v \n", string(tableName), string(row))

	return h.MutateRowTs(ctx, tableName, row, mutations, LATEST_TIMESTAMP, attributes)
}

// Apply a series of mutations (updates/deletes) to a row in a
// single transaction.  If an exception is thrown, then the
// transaction is aborted.  The specified timestamp is used, and
// all entries will have an identical timestamp.
//
// Parameters:
//  - TableName: name of table
//  - Row: row key
//  - Mutations: list of mutation commands
//  - Timestamp: timestamp
//  - Attributes: Mutation attributes
func (h *HBaseHandler) MutateRowTs(ctx context.Context, tableName hbase.Text, row hbase.Text, mutations []*hbase.Mutation, timestamp int64, attributes map[string]hbase.Text) (err error) {
	// log.Printf("MutateRowTs Table: %v, Row: %v\n", string(tableName), string(row))
	rowBatchs := []*hbase.BatchMutation{
		{
			Row:       row,
			Mutations: mutations,
		},
	}
	return h.MutateRowsTs(ctx, tableName, rowBatchs, timestamp, attributes)
}

// Apply a series of batches (each a series of mutations on a single row)
// in a single transaction.  If an exception is thrown, then thel
// transaction is aborted.  Default current timestamp is used, and
// all entries will have an identical timestamp.
//
// Parameters:
//  - TableName: name of table
//  - RowBatches: list of row batches
//  - Attributes: Mutation attributes
func (h *HBaseHandler) MutateRows(ctx context.Context, tableName hbase.Text, rowBatches []*hbase.BatchMutation, attributes map[string]hbase.Text) (err error) {
	return h.MutateRowsTs(ctx, tableName, rowBatches, LATEST_TIMESTAMP, attributes)
}

// Apply a series of batches (each a series of mutations on a single row)
// in a single transaction.  If an exception is thrown, then the
// transaction is aborted.  The specified timestamp is used, and
// all entries will have an identical timestamp.
//
// Parameters:
//  - TableName: name of table
//  - RowBatches: list of row batches
//  - Timestamp: timestamp
//  - Attributes: Mutation attributes
func (h *HBaseHandler) MutateRowsTs(ctx context.Context, tableName hbase.Text, rowBatches []*hbase.BatchMutation, timestamp int64, attributes map[string]hbase.Text) (err error) {
	// if !domain.GetOnlyDomain().InfoSchema().TableExists("", string(tableName)) {
	db, table, err := getDBTable([]byte(tableName))
	if err != nil {
		return buildIOError(err)
	}
	mutReq := &tspb.MutationRequest{
		Table:     table,
		Mutations: make([]*tspb.Mutation, 0),
	}
	if rowBatches != nil {
		for _, batch := range rowBatches {
			for _, mutation := range batch.GetMutations() {
				zmut, err := buildZettaMutation(table, batch.GetRow(), mutation)
				if err != nil {
					log.Println(err)
					return err
				}
				mutReq.Mutations = append(mutReq.Mutations, zmut)
			}
		}
	}
	resource, err := h.ts.sessPool.Get()
	if err != nil {
		return buildIOError(err)
	}
	defer h.ts.sessPool.Put(resource)
	txnSel := &tspb.TransactionSelector{
		Selector: &tspb.TransactionSelector_SingleUse{
			SingleUse: &tspb.TransactionOptions{
				Mode: &tspb.TransactionOptions_ReadWrite_{},
			},
		},
	}
	sess := resource.(session.Session)
	sess.SetDB(db)

	isRawkv, err := sess.RawkvAccess(ctx, table)
	if err != nil {
		return buildIOError(err.Error())
	}
	rtOpt := session.RetrieveTxnOpt{
		Committable: true,
		IsRawKV:     isRawkv,
	}

	txn, err := sess.RetrieveTxn(ctx, txnSel, &rtOpt)
	if err != nil {
		return buildIOError(err)
	}
	startTs := time.Now()
	_, err = sess.HandleMutate(ctx, mutReq, txn)
	if err != nil {
		return buildIOError(err)
	}
	duration := time.Now().Sub(startTs)
	if rand.Intn(1000)%10 == 0 {
		logutil.BgLogger().Info("handle mutate consume time", zap.Duration("dur", duration))
	}
	return nil
}

// Atomically increment the column value specified.  Returns the next value post increment.
//
// Parameters:
//  - TableName: name of table
//  - Row: row to increment
//  - Column: name of column
//  - Value: amount to increment by
func (h *HBaseHandler) AtomicIncrement(ctx context.Context, tableName hbase.Text, row hbase.Text, column hbase.Text, value int64) (r int64, err error) {
	return -1, thrift.NewTApplicationException(thrift.INTERNAL_ERROR, "not implement yet")
}

// Delete all cells that match the passed row and column.
//
// Parameters:
//  - TableName: name of table
//  - Row: Row to update
//  - Column: name of column whose value is to be deleted
//  - Attributes: Delete attributes
func (h *HBaseHandler) DeleteAll(ctx context.Context, tableName hbase.Text, row hbase.Text, column hbase.Text, attributes map[string]hbase.Text) (err error) {
	return thrift.NewTApplicationException(thrift.INTERNAL_ERROR, "not implement yet")
}

// Delete all cells that match the passed row and column and whose
// timestamp is equal-to or older than the passed timestamp.
//
// Parameters:
//  - TableName: name of table
//  - Row: Row to update
//  - Column: name of column whose value is to be deleted
//  - Timestamp: timestamp
//  - Attributes: Delete attributes
func (h *HBaseHandler) DeleteAllTs(ctx context.Context, tableName hbase.Text, row hbase.Text, column hbase.Text, timestamp int64, attributes map[string]hbase.Text) (err error) {
	return thrift.NewTApplicationException(thrift.INTERNAL_ERROR, "not implement yet")
}

// Completely delete the row's cells.
//
// Parameters:
//  - TableName: name of table
//  - Row: key of the row to be completely deleted.
//  - Attributes: Delete attributes
func (h *HBaseHandler) DeleteAllRow(ctx context.Context, tableName hbase.Text, row hbase.Text, attributes map[string]hbase.Text) (err error) {
	return thrift.NewTApplicationException(thrift.INTERNAL_ERROR, "not implement yet")
}

// Increment a cell by the ammount.
// Increments can be applied async if hbase.regionserver.thrift.coalesceIncrement is set to true.
// False is the default.  Turn to true if you need the extra performance and can accept some
// data loss if a thrift server dies with increments still in the queue.
//
// Parameters:
//  - Increment: The single increment to apply
func (h *HBaseHandler) Increment(ctx context.Context, increment *hbase.TIncrement) (err error) {
	return thrift.NewTApplicationException(thrift.INTERNAL_ERROR, "not implement yet")
}

// Parameters:
//  - Increments: The list of increments
func (h *HBaseHandler) IncrementRows(ctx context.Context, increments []*hbase.TIncrement) (err error) {
	return thrift.NewTApplicationException(thrift.INTERNAL_ERROR, "not implement yet")
}

// Completely delete the row's cells marked with a timestamp
// equal-to or older than the passed timestamp.
//
// Parameters:
//  - TableName: name of table
//  - Row: key of the row to be completely deleted.
//  - Timestamp: timestamp
//  - Attributes: Delete attributes
func (h *HBaseHandler) DeleteAllRowTs(ctx context.Context, tableName hbase.Text, row hbase.Text, timestamp int64, attributes map[string]hbase.Text) (err error) {
	return thrift.NewTApplicationException(thrift.INTERNAL_ERROR, "not implement yet")
}

// Get a scanner on the current table, using the Scan instance
// for the scan parameters.
//
// Parameters:
//  - TableName: name of table
//  - Scan: Scan instance
//  - Attributes: Scan attributes
func (h *HBaseHandler) ScannerOpenWithScan(ctx context.Context, tableName hbase.Text, scan *hbase.TScan, attributes map[string]hbase.Text) (r hbase.ScannerID, err error) {
	var (
		cfmap = map[string]struct{}{}
		qlmap = map[string]string{}
	)

	db, table, err := getDBTable([]byte(tableName))
	if err != nil {
		return -1, buildIOError(err)
	}

	for _, column := range scan.Columns {
		cf, qlf, err := getFamilyQualifier(column)
		if err != nil {
			return -1, buildIOError(err)
		}
		cfmap[cf] = struct{}{}
		if qlf != "" {
			qlmap[qlf] = cf
		}
	}
	scanReq := &tst.ScanRequest{
		Table:        table,
		StartRow:     scan.StartRow,
		StopRow:      scan.StopRow,
		Timestamp:    scan.GetTimestamp(),
		Columns:      nil,
		Caching:      scan.GetCaching(),
		BatchSize:    scan.GetBatchSize(),
		ColFamilyMap: cfmap,
		QualifierMap: qlmap,
	}

	txnSel := &tspb.TransactionSelector{
		Selector: &tspb.TransactionSelector_SingleUse{
			SingleUse: &tspb.TransactionOptions{
				Mode: &tspb.TransactionOptions_ReadOnly_{},
			},
		},
	}
	resource, err := h.ts.sessPool.Get()
	if err != nil {
		return -1, buildIOError(err)
	}
	defer h.ts.sessPool.Put(resource)
	sess := resource.(session.Session)
	sess.SetDB(db)
	isRawkv, err := sess.RawkvAccess(ctx, string(table))
	if err != nil {
		return -1, buildIOError(err.Error())
	}
	rtOpt := session.RetrieveTxnOpt{
		Committable: false,
		IsRawKV:     isRawkv,
	}
	txn, err := sess.RetrieveTxn(ctx, txnSel, &rtOpt)
	if err != nil {
		return -1, buildIOError(err)
	}
	ri, err := sess.HandleRead(ctx, scanReq, txn)
	if err != nil {
		return -1, buildIOError(err)
	}

	scanner := &TScanner{
		ResultIter: ri,
	}
	scannerID := h.addScanner(scanner)
	return hbase.ScannerID(scannerID), nil
}

// Get a scanner on the current table starting at the specified row and
// ending at the last row in the table.  Return the specified columns.
//
// @return scanner id to be used with other scanner procedures
//
// Parameters:
//  - TableName: name of table
//  - StartRow: Starting row in table to scan.
// Send "" (empty string) to start at the first row.
//  - Columns: columns to scan. If column name is a column family, all
// columns of the specified column family are returned. It's also possible
// to pass a regex in the column qualifier.
//  - Attributes: Scan attributes
func (h *HBaseHandler) ScannerOpen(ctx context.Context, tableName hbase.Text, startRow hbase.Text, columns []hbase.Text, attributes map[string]hbase.Text) (r hbase.ScannerID, err error) {
	return h.ScannerOpenWithStopTs(ctx, tableName, startRow, nil, columns, LATEST_TIMESTAMP, attributes)
}

// Get a scanner on the current table starting and stopping at the
// specified rows.  ending at the last row in the table.  Return the
// specified columns.
//
// @return scanner id to be used with other scanner procedures
//
// Parameters:
//  - TableName: name of table
//  - StartRow: Starting row in table to scan.
// Send "" (empty string) to start at the first row.
//  - StopRow: row to stop scanning on. This row is *not* included in the
// scanner's results
//  - Columns: columns to scan. If column name is a column family, all
// columns of the specified column family are returned. It's also possible
// to pass a regex in the column qualifier.
//  - Attributes: Scan attributes
func (h *HBaseHandler) ScannerOpenWithStop(ctx context.Context, tableName hbase.Text, startRow hbase.Text, stopRow hbase.Text, columns []hbase.Text, attributes map[string]hbase.Text) (r hbase.ScannerID, err error) {
	return h.ScannerOpenWithStopTs(ctx, tableName, startRow, nil, columns, LATEST_TIMESTAMP, attributes)
}

// Open a scanner for a given prefix.  That is all rows will have the specified
// prefix. No other rows will be returned.
//
// @return scanner id to use with other scanner calls
//
// Parameters:
//  - TableName: name of table
//  - StartAndPrefix: the prefix (and thus start row) of the keys you want
//  - Columns: the columns you want returned
//  - Attributes: Scan attributes
func (h *HBaseHandler) ScannerOpenWithPrefix(ctx context.Context, tableName hbase.Text, startAndPrefix hbase.Text, columns []hbase.Text, attributes map[string]hbase.Text) (r hbase.ScannerID, err error) {
	return -1, nil
}

// Get a scanner on the current table starting at the specified row and
// ending at the last row in the table.  Return the specified columns.
// Only values with the specified timestamp are returned.
//
// @return scanner id to be used with other scanner procedures
//
// Parameters:
//  - TableName: name of table
//  - StartRow: Starting row in table to scan.
// Send "" (empty string) to start at the first row.
//  - Columns: columns to scan. If column name is a column family, all
// columns of the specified column family are returned. It's also possible
// to pass a regex in the column qualifier.
//  - Timestamp: timestamp
//  - Attributes: Scan attributes
func (h *HBaseHandler) ScannerOpenTs(ctx context.Context, tableName hbase.Text, startRow hbase.Text, columns []hbase.Text, timestamp int64, attributes map[string]hbase.Text) (r hbase.ScannerID, err error) {
	return h.ScannerOpenWithStopTs(ctx, tableName, startRow, nil, columns, timestamp, attributes)
}

// Get a scanner on the current table starting and stopping at the
// specified rows.  ending at the last row in the table.  Return the
// specified columns.  Only values with the specified timestamp are
// returned.
//
// @return scanner id to be used with other scanner procedures
//
// Parameters:
//  - TableName: name of table
//  - StartRow: Starting row in table to scan.
// Send "" (empty string) to start at the first row.
//  - StopRow: row to stop scanning on. This row is *not* included in the
// scanner's results
//  - Columns: columns to scan. If column name is a column family, all
// columns of the specified column family are returned. It's also possible
// to pass a regex in the column qualifier.
//  - Timestamp: timestamp
//  - Attributes: Scan attributes
func (h *HBaseHandler) ScannerOpenWithStopTs(ctx context.Context, tableName hbase.Text, startRow hbase.Text, stopRow hbase.Text, columns []hbase.Text, timestamp int64, attributes map[string]hbase.Text) (r hbase.ScannerID, err error) {
	tscan := &hbase.TScan{
		StartRow:     startRow,
		StopRow:      stopRow,
		Timestamp:    &timestamp,
		Columns:      columns,
		FilterString: nil,
		Caching:      proto.Int(1000),
		BatchSize:    proto.Int(2000),
		SortColumns:  proto.Bool(false),
		Reversed:     proto.Bool(false),
	}
	return h.ScannerOpenWithScan(ctx, tableName, tscan, attributes)
}

// Returns the scanner's current row value and advances to the next
// row in the table.  When there are no more rows in the table, or a key
// greater-than-or-equal-to the scanner's specified stopRow is reached,
// an empty list is returned.
//
// @return a TRowResult containing the current row and a map of the columns to TCells.
//
// @throws IllegalArgument if ScannerID is invalid
//
// @throws NotFound when the scanner reaches the end
//
// Parameters:
//  - ID: id of a scanner returned by scannerOpen
func (h *HBaseHandler) ScannerGet(ctx context.Context, id hbase.ScannerID) (r []*hbase.TRowResult_, err error) {
	return h.ScannerGetList(ctx, id, 1)
}

// Returns, starting at the scanner's current row value nbRows worth of
// rows and advances to the next row in the table.  When there are no more
// rows in the table, or a key greater-than-or-equal-to the scanner's
// specified stopRow is reached,  an empty list is returned.
//
// @return a TRowResult containing the current row and a map of the columns to TCells.
//
// @throws IllegalArgument if ScannerID is invalid
//
// @throws NotFound when the scanner reaches the end
//
// Parameters:
//  - ID: id of a scanner returned by scannerOpen
//  - NbRows: number of results to return
func (h *HBaseHandler) ScannerGetList(ctx context.Context, id hbase.ScannerID, nbRows int32) (r []*hbase.TRowResult_, err error) {
	scanner := h.getScanner(int64(id))
	if scanner == nil {
		return nil, buildIllegalArgument(fmt.Sprintf("not such scanner id %v", id))
	}
	r = []*hbase.TRowResult_{}
	count := nbRows
	// startTime := time.Now()
	for count > 0 {

		rsliceCell, err := scanner.ResultIter.Next(ctx)
		if err == io.EOF {
			break
		}
		count--

		rscell := rsliceCell.(*tspb.SliceCell)
		result := &hbase.TRowResult_{
			Row:     rscell.PrimaryKeys[0].GetBytesValue(),
			Columns: make(map[string]*hbase.TCell),
		}
		for _, cell := range rscell.Cells {
			tcell := &hbase.TCell{Value: cell.Value.GetBytesValue()}
			columnName := cell.Family + ":" + cell.Column
			result.Columns[columnName] = tcell
		}
		r = append(r, result)
	}
	// dur := time.Now().Sub(startTime)
	if err = scanner.ResultIter.LastErr(); err != nil {
		return r, buildIOError(err)
	}
	return r, nil
}

// Closes the server-state associated with an open scanner.
//
// @throws IllegalArgument if ScannerID is invalid
//
// Parameters:
//  - ID: id of a scanner returned by scannerOpen
func (h *HBaseHandler) ScannerClose(ctx context.Context, id hbase.ScannerID) (err error) {
	scanner := h.getScanner(int64(id))
	if scanner == nil {
		return buildIllegalArgument(fmt.Sprintf("not such scanner id %v", id))
	}
	scanner.ResultIter.Close()
	h.removeScanner(scanner.scannerID)
	return nil
}

// Get the row just before the specified one.
//
// @return value for specified row/column
//
// Parameters:
//  - TableName: name of table
//  - Row: row key
//  - Family: column name
func (h *HBaseHandler) GetRowOrBefore(ctx context.Context, tableName hbase.Text, row hbase.Text, family hbase.Text) (r []*hbase.TCell, err error) {
	return nil, thrift.NewTApplicationException(thrift.INTERNAL_ERROR, "not implement yet")
}

// Get the regininfo for the specified row. It scans
// the metatable to find region's start and end keys.
//
// @return value for specified row/column
//
// Parameters:
//  - Row: row key
func (h *HBaseHandler) GetRegionInfo(ctx context.Context, row hbase.Text) (r *hbase.TRegionInfo, err error) {
	return nil, nil
}

// Appends values to one or more columns within a single row.
//
// @return values of columns after the append operation.
//
// Parameters:
//  - Append: The single append operation to apply
func (h *HBaseHandler) Append(ctx context.Context, append *hbase.TAppend) (r []*hbase.TCell, err error) {
	return nil, thrift.NewTApplicationException(thrift.INTERNAL_ERROR, "not implement yet")
}

// Atomically checks if a row/family/qualifier value matches the expected
// value. If it does, it adds the corresponding mutation operation for put.
//
// @return true if the new put was executed, false otherwise
//
// Parameters:
//  - TableName: name of table
//  - Row: row key
//  - Column: column name
//  - Value: the expected value for the column parameter, if not
// provided the check is for the non-existence of the
// column in question
//  - Mput: mutation for the put
//  - Attributes: Mutation attributes
func (h *HBaseHandler) CheckAndPut(ctx context.Context, tableName hbase.Text, row hbase.Text, column hbase.Text, value hbase.Text, mput *hbase.Mutation, attributes map[string]hbase.Text) (r bool, err error) {
	return false, thrift.NewTApplicationException(thrift.INTERNAL_ERROR, "not implement yet")
}
