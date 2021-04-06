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
	"errors"
	"log"

	"github.com/pingcap/tidb/util/logutil"
	tspb "github.com/zhihu/zetta-proto/pkg/tablestore"
	"github.com/zhihu/zetta/tablestore/server/hthrift/hbase"
	"go.uber.org/zap"
)

func getFamilyQualifier(column []byte) (string, string, error) {
	items := bytes.Split(column, []byte(":"))
	if len(items) == 2 {
		if len(items[0]) == 0 {
			return "", "", errors.New("missing family")
		}
		return string(items[0]), string(items[1]), nil
	}
	if len(items) == 1 {
		return string(items[0]), "", nil
	}
	return "", "", errors.New("missing family")
}

func getDBTable(rawData []byte) (database string, table string, err error) {
	items := bytes.Split(rawData, []byte(":"))
	if len(items) == 2 {
		database = string(items[0])
		table = string(items[1])
		err = nil
		return
	}
	return "", string(items[0]), errors.New("missing database")
}

func buildZettaMutation(table string, rowkey []byte, hmut *hbase.Mutation) (*tspb.Mutation, error) {
	family, qualifier, err := getFamilyQualifier([]byte(hmut.Column))
	if err != nil {
		log.Println(err)
		return nil, &hbase.IOError{Message: err.Error()}
	}
	mut := &tspb.Mutation{}
	pkey := &tspb.KeySet{
		Keys: []*tspb.ListValue{
			{
				Values: []*tspb.Value{
					{Kind: &tspb.Value_BytesValue{BytesValue: rowkey}},
				},
			},
		},
	}
	if hmut.IsDelete {
		logutil.BgLogger().Info("delete params", zap.String("table", table), zap.String("family", family), zap.String("qualifier", qualifier))
		mutDel := &tspb.Mutation_Delete{
			Table:   table,
			Family:  family,
			Columns: nil,
			KeySet:  pkey,
		}
		if len(qualifier) > 0 {
			mutDel.Columns = []string{qualifier}
		}
		mut.Operation = &tspb.Mutation_Delete_{
			Delete: mutDel,
		}
	} else {
		mut.Operation = &tspb.Mutation_InsertOrUpdate{
			InsertOrUpdate: &tspb.Mutation_Write{
				Table:   table,
				Family:  family,
				Columns: []string{qualifier},
				Values: []*tspb.ListValue{
					{
						Values: []*tspb.Value{
							{Kind: &tspb.Value_BytesValue{BytesValue: []byte(hmut.Value)}},
						},
					},
				},
				KeySet: pkey,
			},
		}
	}
	return mut, nil
}

func buildZettaRowKey(rowkey []byte) *tspb.ListValue {
	return &tspb.ListValue{
		Values: []*tspb.Value{
			{Kind: &tspb.Value_BytesValue{BytesValue: []byte(rowkey)}},
		},
	}
}

func buildSparseReadReq(table string, rowkeys []hbase.Text, columns []hbase.Text) ([]*tspb.SparseReadRequest, error) {
	var (
		reqList = []*tspb.SparseReadRequest{}
	)

	if columns == nil || len(columns) == 0 {
		req := &tspb.SparseReadRequest{
			Table:  table,
			Family: "",
			Rows:   []*tspb.Row{},
		}
		for _, rowkey := range rowkeys {
			rkeys := buildZettaRowKey(rowkey)
			tsrow := &tspb.Row{Keys: rkeys}
			req.Rows = append(req.Rows, tsrow)
		}
		reqList = append(reqList, req)
	} else {
		colsMap := map[string]map[string]struct{}{}
		for _, column := range columns {
			family, col, err := getFamilyQualifier([]byte(column))
			if err != nil {
				return nil, err
			}
			cols, ok := colsMap[family]
			if !ok {
				colsMap[family] = map[string]struct{}{}
				if col != "" {
					colsMap[family][col] = struct{}{}
				}
			} else {
				if col == "" {
					colsMap[family] = map[string]struct{}{}
				}
				if len(cols) != 0 {
					cols[col] = struct{}{}
				}
			}
		}

		for family, cols := range colsMap {
			req := &tspb.SparseReadRequest{
				Table:  table,
				Family: family,
				Rows:   []*tspb.Row{},
			}
			for _, rowkey := range rowkeys {
				rkeys := buildZettaRowKey(rowkey)
				row := &tspb.Row{
					Keys:       rkeys,
					Qualifiers: make([]string, 0),
				}
				for col := range cols {
					row.Qualifiers = append(row.Qualifiers, col)
				}
				req.Rows = append(req.Rows, row)
			}
			reqList = append(reqList, req)
		}

	}

	return reqList, nil
}

func buildIOError(msg interface{}) error {
	var errMsg string
	switch msg.(type) {
	case error:
		errMsg = msg.(error).Error()
	case string:
		errMsg = msg.(string)
	}
	return &hbase.IOError{Message: errMsg}
}

func buildIllegalArgument(msg interface{}) error {
	var errMsg string
	switch msg.(type) {
	case error:
		errMsg = msg.(error).Error()
	case string:
		errMsg = msg.(string)
	}
	return &hbase.IllegalArgument{Message: errMsg}
}
