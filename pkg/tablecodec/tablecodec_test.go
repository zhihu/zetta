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

package tablecodec

import (
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/testleak"
)

func TestT(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testTableCodecSuite{})

type testTableCodecSuite struct{}

// column is a structure used for test
type column struct {
	id int64
	tp *types.FieldType
}

func getStmtctx() *stmtctx.StatementContext {
	return &stmtctx.StatementContext{TimeZone: time.Local}
}

func (s *testTableCodecSuite) TestRecordKeyCodec(c *C) {
	defer testleak.AfterTest(c)()
	tableID := int64(20)
	columnFamilyID := int64(1)

	c1 := &column{id: 1, tp: types.NewFieldType(mysql.TypeLonglong)}
	c2 := &column{id: 2, tp: types.NewFieldType(mysql.TypeVarchar)}
	cols := []*column{c1, c2}
	primaryKeyFieldTypes := []*types.FieldType{}
	for _, c := range cols {
		primaryKeyFieldTypes = append(primaryKeyFieldTypes, c.tp)
	}

	row := make([]types.Datum, 2)
	row[0] = types.NewIntDatum(100)
	row[1] = types.NewBytesDatum([]byte("abc"))

	sc := getStmtctx()
	encb, err := EncodeRecordKeyWide(sc, tableID, row, columnFamilyID)
	c.Assert(err, IsNil)
	c.Assert(encb, NotNil)

	encodedPrimaryKey, err := EncodePrimaryKey(sc, row)
	c.Assert(err, IsNil)
	extractedPrimaryKey := ExtractEncodedPrimaryKey(encb)
	c.Assert(encodedPrimaryKey, DeepEquals, extractedPrimaryKey)

	tid, pkd, cfID, cName, err := DecodeRecordKey(encb, primaryKeyFieldTypes, sc.TimeZone)
	c.Assert(err, IsNil)
	c.Assert(tid, Equals, tableID)
	c.Assert(cfID, Equals, columnFamilyID)
	c.Assert(cName, IsNil)
	for i, d := range pkd {
		equal, err1 := d.CompareDatum(sc, &row[i])
		c.Assert(err1, IsNil)
		c.Assert(equal, Equals, 0)
	}

	encb, err = EncodeRecordKeyHigh(sc, tableID, row, columnFamilyID, []byte("c1"))
	c.Assert(err, IsNil)
	c.Assert(encb, NotNil)

	tid, pkd, cfID, cName, err = DecodeRecordKey(encb, primaryKeyFieldTypes, sc.TimeZone)
	c.Assert(err, IsNil)
	c.Assert(tid, Equals, tableID)
	c.Assert(cfID, Equals, columnFamilyID)
	c.Assert(cName, DeepEquals, []byte("c1"))
	for i, d := range pkd {
		equal, err1 := d.CompareDatum(sc, &row[i])
		c.Assert(err1, IsNil)
		c.Assert(equal, Equals, 0)
	}
}

func compareDatums(src, dst []types.Datum, c *C) {
	sc := getStmtctx()
	for i, d := range src {
		equal, err := d.CompareDatum(sc, &dst[i])
		c.Assert(err, IsNil)
		c.Assert(equal, Equals, 0)
	}
}

func (s *testTableCodecSuite) TestIndexCodec(c *C) {
	c1 := &column{id: 1, tp: types.NewFieldType(mysql.TypeLonglong)}
	c2 := &column{id: 2, tp: types.NewFieldType(mysql.TypeVarchar)}
	cols := []*column{c1, c2}
	indexFieldTypes := []*types.FieldType{}
	for _, c := range cols {
		indexFieldTypes = append(indexFieldTypes, c.tp)
	}
	row := make([]types.Datum, 2)
	row[0] = types.NewIntDatum(100)
	row[1] = types.NewBytesDatum([]byte("abc"))

	primaryKeyFieldTypes := []*types.FieldType{types.NewFieldType(mysql.TypeLonglong)}
	pkRow := []types.Datum{types.NewIntDatum(200)}

	tableID := int64(1)
	idx := int64(1)
	sc := getStmtctx()

	encUniqIndex, err := EncodeIndexUnique(sc, tableID, idx, row)
	c.Assert(err, IsNil)
	dtid, didx, divalues, err := DecodeUniqIndexKey(encUniqIndex, indexFieldTypes, sc.TimeZone)
	c.Assert(err, IsNil)
	c.Assert(dtid, Equals, tableID)
	c.Assert(didx, Equals, idx)
	compareDatums(divalues, row, c)

	encIndex, err := EncodeIndex(sc, tableID, idx, row, pkRow)
	c.Assert(err, IsNil)
	dtid, didx, divalues, dpkvalues, err := DecodeIndexKey(encIndex, indexFieldTypes, primaryKeyFieldTypes, sc.TimeZone)
	c.Assert(err, IsNil)
	c.Assert(dtid, Equals, tableID)
	c.Assert(didx, Equals, idx)
	compareDatums(divalues, row, c)
	compareDatums(dpkvalues, pkRow, c)
}

func (s *testTableCodecSuite) TestRowCodec(c *C) {
	defer testleak.AfterTest(c)()

	c1 := &column{id: 1, tp: types.NewFieldType(mysql.TypeLonglong)}
	c2 := &column{id: 2, tp: types.NewFieldType(mysql.TypeVarchar)}
	c3 := &column{id: 3, tp: types.NewFieldType(mysql.TypeNewDecimal)}
	c4 := &column{id: 4, tp: &types.FieldType{Tp: mysql.TypeEnum, Elems: []string{"a"}}}
	c5 := &column{id: 5, tp: &types.FieldType{Tp: mysql.TypeSet, Elems: []string{"a"}}}
	c6 := &column{id: 6, tp: &types.FieldType{Tp: mysql.TypeBit, Flen: 8}}
	cols := []*column{c1, c2, c3, c4, c5, c6}

	row := make([]types.Datum, 6)
	row[0] = types.NewIntDatum(100)
	row[1] = types.NewBytesDatum([]byte("abc"))
	row[2] = types.NewDecimalDatum(types.NewDecFromInt(1))
	row[3] = types.NewMysqlEnumDatum(types.Enum{Name: "a", Value: 0})
	row[4] = types.NewDatum(types.Set{Name: "a", Value: 0})
	row[5] = types.NewDatum(types.BinaryLiteral{100})
	// Encode
	colIDs := make([]int64, 0, len(row))
	for _, col := range cols {
		colIDs = append(colIDs, col.id)
	}
	sc := &stmtctx.StatementContext{TimeZone: time.Local}
	bs, err := EncodeRow(sc, row, colIDs, nil, nil)
	c.Assert(err, IsNil)
	c.Assert(bs, NotNil)

	// Decode
	colMap := make(map[int64]*types.FieldType, len(row))
	for _, col := range cols {
		colMap[col.id] = col.tp
	}
	r, err := DecodeRowWide(bs, colMap, time.UTC, nil)
	c.Assert(err, IsNil)
	c.Assert(r, NotNil)
	c.Assert(r, HasLen, len(row))
	// Compare decoded row and original row
	for i, col := range cols {
		v, ok := r[col.id]
		c.Assert(ok, IsTrue)
		equal, err1 := v.CompareDatum(sc, &row[i])
		c.Assert(err1, IsNil)
		c.Assert(equal, Equals, 0)
	}

	// colMap may contains more columns than encoded row.
	colMap[4] = types.NewFieldType(mysql.TypeFloat)
	r, err = DecodeRowWide(bs, colMap, time.UTC, nil)
	c.Assert(err, IsNil)
	c.Assert(r, NotNil)
	c.Assert(r, HasLen, len(row))
	for i, col := range cols {
		v, ok := r[col.id]
		c.Assert(ok, IsTrue)
		equal, err1 := v.CompareDatum(sc, &row[i])
		c.Assert(err1, IsNil)
		c.Assert(equal, Equals, 0)
	}

	// colMap may contains less columns than encoded row.
	delete(colMap, 3)
	delete(colMap, 4)
	r, err = DecodeRowWide(bs, colMap, time.UTC, nil)
	c.Assert(err, IsNil)
	c.Assert(r, NotNil)
	c.Assert(r, HasLen, len(row)-2)
	for i, col := range cols {
		if i > 1 {
			break
		}
		v, ok := r[col.id]
		c.Assert(ok, IsTrue)
		equal, err1 := v.CompareDatum(sc, &row[i])
		c.Assert(err1, IsNil)
		c.Assert(equal, Equals, 0)
	}

	// Make sure empty row return not nil value.
	bs, err = EncodeRow(sc, []types.Datum{}, []int64{}, nil, nil)
	c.Assert(err, IsNil)
	c.Assert(bs, HasLen, 1)

	r, err = DecodeRowWide(bs, colMap, time.UTC, nil)
	c.Assert(err, IsNil)
	c.Assert(len(r), Equals, 0)

	for i, col := range cols {
		bs, err = EncodeRowHigh(sc, row[i], nil)
		c.Assert(err, IsNil)
		v, err := DecodeRowHigh(sc, bs, col.tp, sc.TimeZone)
		c.Assert(err, IsNil)
		equal, err1 := v.CompareDatum(sc, &row[i])
		c.Assert(err1, IsNil)
		c.Assert(equal, Equals, 0)
	}

	flexibleRow := map[string]types.Datum{
		"c1": types.NewBytesDatum([]byte("abc")),
		"c2": types.NewBytesDatum([]byte("def")),
	}
	bs, err = EncodeRowFlexible(sc, flexibleRow, nil, nil)
	c.Assert(err, IsNil)

	res, _, err := DecodeRowFlexible(sc, bs)
	for name, value := range res {
		ovalue := flexibleRow[name]
		value.CompareDatum(sc, &ovalue)
	}
}
