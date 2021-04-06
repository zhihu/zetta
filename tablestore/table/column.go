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

// Copyright 2016 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

// Copyright 2015 PingCAP, Inc.
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
	"strings"

	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/hack"

	"github.com/zhihu/zetta/pkg/model"
)

// Column provides meta data describing a table column.
type Column struct {
	// *model.ColumnInfo
	*model.ColumnMeta
}

// String implements fmt.Stringer interface.
func (c *Column) String() string {

	return ""
}

// FindCol finds column in cols by name.
func FindCol(cols []*model.ColumnMeta, name string) *model.ColumnMeta {
	for _, col := range cols {
		if strings.EqualFold(col.ColumnMeta.Name, name) {
			return col
		}
	}
	return nil
}

// ToColumn converts a *model.ColumnInfo to *Column.
func ToColumn(col *model.ColumnMeta) *Column {
	return &Column{
		col,
	}
}

// truncateTrailingSpaces trancates trailing spaces for CHAR[(M)] column.
// fix: https://github.com/pingcap/tidb/issues/3660
func truncateTrailingSpaces(v *types.Datum) {
	if v.Kind() == types.KindNull {
		return
	}
	b := v.GetBytes()
	length := len(b)
	for length > 0 && b[length-1] == ' ' {
		length--
	}
	b = b[:length]
	str := string(hack.String(b))
	v.SetString(str, v.Collation())
}

// ColDesc describes column information like MySQL desc and show columns do.
type ColDesc struct {
	Field string
	Type  string
	Key   string
	// Charset is nil if the column doesn't have a charset, or a string indicating the charset name.
	//Charset interface{}
	// Collation is nil if the column doesn't have a collation, or a string indicating the collation name.
	//Collation interface{}
	//Null         string
	//DefaultValue interface{}
}

const defaultPrivileges = "select,insert,update,references"

// NewColDesc returns a new ColDesc for a column.
func NewColDesc(tbl Table, col *Column) *ColDesc {
	// TODO: if we have no primary key and a unique index which's columns are all not null
	// we will set these columns' flag as PriKeyFlag
	// see https://dev.mysql.com/doc/refman/5.7/en/show-columns.html
	// create table
	name := col.ColumnMeta.ColumnMeta.Name
	idxName := "null"
	_, idxMeta := tbl.Meta().GetIndexByColumnName(name)
	if idxMeta != nil {
		idxName = idxMeta.Name
	}
	if col.IsPrimary {
		idxName = "primary"
	}

	desc := &ColDesc{
		Field: name,
		Type:  col.GetTypeDesc(),
		Key:   idxName,
		//Charset:   col.Charset,
		//Collation: col.Collate,
	}
	return desc
}

func NewCFDesc(cf *model.ColumnFamilyMeta) *ColDesc {
	return &ColDesc{
		Field: cf.Name,
		Type:  "ColumnFamily",
	}
}

// GetTypeDesc gets the description for column type.
func (c *Column) GetTypeDesc() string {
	desc := c.FieldType.CompactStr()
	if mysql.HasUnsignedFlag(c.Flag) && c.Tp != mysql.TypeBit && c.Tp != mysql.TypeYear {
		desc += " unsigned"
	}
	if mysql.HasZerofillFlag(c.Flag) && c.Tp != mysql.TypeYear {
		desc += " zerofill"
	}
	return desc
}

// ColDescFieldNames returns the fields name in result set for desc and show columns.
func ColDescFieldNames(full bool) []string {
	if full {
		return []string{"Field", "Type", "Collation", "Null", "Key", "Default", "Extra", "Privileges", "Comment"}
	}
	//return []string{"Field", "Type", "Null", "Key", "Default", "Extra"}
	return []string{"Field", "Type", "Key"}
}

// CheckOnce checks if there are duplicated column names in cols.
func CheckOnce(cols []*model.ColumnMeta) error {
	m := map[string]struct{}{}
	for _, col := range cols {
		name := col.ColumnMeta.Name
		_, ok := m[name]
		if ok {
			return ErrDuplicateColumn.GenWithStack("column specified twice - %s", name)
		}

		m[name] = struct{}{}
	}
	return nil
}

// CheckNotNull checks if nil value set to a column with NotNull flag is set.
func (c *Column) CheckNotNull(data types.Datum) error {
	if (mysql.HasNotNullFlag(c.Flag) || mysql.HasPreventNullInsertFlag(c.Flag)) && data.IsNull() {
		return ErrColumnCantNull.GenWithStackByArgs(c.ColumnMeta.ColumnMeta.Name)
	}
	return nil
}

// IsPKHandleColumn checks if the column is primary key handle column.
func (c *Column) IsPKHandleColumn(tbInfo *model.TableInfo) bool {
	return mysql.HasPriKeyFlag(c.Flag) && tbInfo.PKIsHandle
}

// CheckNotNull checks if row has nil value set to a column with NotNull flag set.
func CheckNotNull(cols []*Column, row []types.Datum) error {
	for _, c := range cols {
		if err := c.CheckNotNull(row[c.Offset]); err != nil {
			return err
		}
	}
	return nil
}
