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

package tables

import (
	"math"
	"testing"
	"time"

	"cloud.google.com/go/civil"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	tspb "github.com/zhihu/zetta-proto/pkg/tablestore"
	"github.com/zhihu/zetta/tablestore/rpc"
)

var (
	tBool   = rpc.BoolType()
	tInt    = rpc.IntType()
	tFloat  = rpc.FloatType()
	tTime   = rpc.TimeType()
	tDate   = rpc.DateType()
	tString = rpc.StringType()
	tBytes  = rpc.BytesType()
)

var (
	T1    = mustParseTime("2016-11-15T15:04:05.999999999Z")
	T2    = time.Now().UTC()
	T3, _ = types.RoundFrac(T1, 0)

	Date1 = civil.DateOf(T1)
	Date2 = civil.DateOf(T2)

	dTime1 = buildDatumTime(T1)
	dTime2 = buildDatumTime(T2)

	dDate1 = buildDatumDate(Date1)
	dDate2 = buildDatumDate(Date2)
)

func mustParseTime(s string) time.Time {
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		panic(err)
	}
	return t
}

func buildDatumTime(tt time.Time) types.Time {
	return types.NewTime(types.FromGoTime(tt), mysql.TypeTimestamp, 0)
}

func buildDatumDate(date civil.Date) types.Time {
	return types.NewTime(types.FromGoTime(date.In(time.UTC)), mysql.TypeDate, 0)
}

// func buildFromGoTime(tt time.Time) types.MysqlTime {
// 	return types.MysqlTime{}
// }

func TestT(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testValueSuite{})

type testValueSuite struct{}

func (ts *testValueSuite) TestDatumFromTV(c *C) {
	testCases := []struct {
		pt     *tspb.Type
		pv     *tspb.Value
		wanted types.Datum
	}{
		{tBool, rpc.BoolProto(true), types.NewDatum(true)},

		{tBool, rpc.BoolProto(false), types.NewDatum(false)},
		{tBool, rpc.NullProto(), types.NewDatum(nil)},
		{tInt, rpc.IntProto(math.MaxInt16), types.NewIntDatum(math.MaxInt16)},
		{tInt, rpc.IntProto(math.MaxInt32), types.NewIntDatum(math.MaxInt32)},
		{tInt, rpc.IntProto(math.MaxInt64), types.NewIntDatum(math.MaxInt64)},
		{tInt, rpc.IntProto(math.MinInt64), types.NewIntDatum(math.MinInt64)},
		{tInt, rpc.IntProto(math.MinInt32), types.NewIntDatum(math.MinInt32)},
		{tInt, rpc.IntProto(math.MinInt16), types.NewIntDatum(math.MinInt16)},
		{tInt, rpc.NullProto(), types.NewDatum(nil)},

		{tFloat, rpc.FloatProto(math.MaxFloat64), types.NewFloat64Datum(math.MaxFloat64)},
		{tFloat, rpc.FloatProto(math.MaxFloat32), types.NewFloat64Datum(math.MaxFloat32)},
		{tFloat, rpc.NullProto(), types.NewDatum(nil)},
		{tFloat, rpc.FloatProto(-1.234), types.NewFloat64Datum(-1.234)},

		{tTime, rpc.TimeProto(T1), types.NewTimeDatum(dTime1)},
		{tTime, rpc.TimeProto(T2), types.NewTimeDatum(dTime2)},
		{tTime, rpc.NullProto(), types.NewDatum(nil)},

		{tDate, rpc.DateProto(Date1), types.NewTimeDatum(dDate1)},
		{tDate, rpc.DateProto(Date2), types.NewTimeDatum(dDate2)},
		{tDate, rpc.NullProto(), types.NewDatum(nil)},

		{tString, rpc.StringProto("test_proto_value"), types.NewStringDatum("test_proto_value")},
		{tString, rpc.StringProto(""), types.NewStringDatum("")},
		{tString, rpc.NullProto(), types.NewDatum(nil)},

		{tBytes, rpc.BytesProto([]byte("test_proto_bytes")), types.NewBytesDatum([]byte("test_proto_bytes"))},
		{tBytes, rpc.BytesProto([]byte{}), types.NewBytesDatum([]byte{})},

		{tBytes, rpc.NullProto(), types.NewDatum(nil)},
	}

	for i, tc := range testCases {

		datum, err := datumFromTypeValue(tc.pt, tc.pv)
		ret, er := datum.CompareDatum(&stmtctx.StatementContext{TimeZone: time.UTC}, &tc.wanted)
		c.Assert(er, IsNil)

		c.Logf("%v: compare %v", i, ret)
		c.Logf("%v[%s] %+v %+v", i, tc.pt, datum, tc.wanted)

		c.Assert(err, IsNil)
		c.Assert(datum, DeepEquals, tc.wanted)
	}

}

func (ts *testValueSuite) TestDatumFromTV2(c *C) {
	testCases := []struct {
		pt *tspb.Type
		pv *tspb.Value
	}{
		{tInt, rpc.BoolProto(false)},
		{tString, rpc.BoolProto(false)},
		{tFloat, rpc.BoolProto(true)},
		{tBytes, rpc.BoolProto(false)},
		{tTime, rpc.BoolProto(false)},
		{tDate, rpc.BoolProto(false)},
	}
	for i, tc := range testCases {
		_, err := datumFromTypeValue(tc.pt, tc.pv)
		c.Logf("%d err: %v", i, err)

		c.Assert(err, NotNil)
	}

}

func (ts *testValueSuite) TestProtoValue(c *C) {
	// c.Skip("testing debug")
	testCases := []struct {
		d      types.Datum
		pt     *tspb.Type
		wanted *tspb.Value
	}{
		{types.NewDatum(true), tBool, rpc.BoolProto(true)},
		{types.NewDatum(false), tBool, rpc.BoolProto(false)},
		{types.NewDatum(nil), tBool, rpc.NullProto()},

		{types.NewIntDatum(math.MaxInt16), tInt, rpc.IntProto(math.MaxInt16)},
		{types.NewIntDatum(math.MaxInt32), tInt, rpc.IntProto(math.MaxInt32)},
		{types.NewIntDatum(math.MaxInt64), tInt, rpc.IntProto(math.MaxInt64)},
		{types.NewIntDatum(math.MinInt64), tInt, rpc.IntProto(math.MinInt64)},
		{types.NewIntDatum(math.MinInt32), tInt, rpc.IntProto(math.MinInt32)},
		{types.NewIntDatum(math.MinInt16), tInt, rpc.IntProto(math.MinInt16)},
		{types.NewDatum(nil), tInt, rpc.NullProto()},

		{types.NewFloat64Datum(-1.234), tFloat, rpc.FloatProto(-1.234)},
		{types.NewFloat64Datum(math.MaxFloat64), tFloat, rpc.FloatProto(math.MaxFloat64)},
		{types.NewFloat64Datum(math.MaxFloat32), tFloat, rpc.FloatProto(math.MaxFloat32)},
		{types.NewDatum(nil), tFloat, rpc.NullProto()},

		{types.NewTimeDatum(dTime1), tTime, rpc.TimeProto(T3)},
		{types.NewTimeDatum(dTime2), tTime, rpc.TimeProto(T2)},
		{types.NewDatum(nil), tTime, rpc.NullProto()},

		{types.NewTimeDatum(dDate1), tDate, rpc.DateProto(Date1)},
		{types.NewTimeDatum(dDate2), tDate, rpc.DateProto(Date2)},
		{types.NewDatum(nil), tDate, rpc.NullProto()},

		{types.NewStringDatum("test_proto_value"), tString, rpc.StringProto("test_proto_value")},
		{types.NewStringDatum(""), tString, rpc.StringProto("")},
		{types.NewDatum(nil), tString, rpc.NullProto()},

		{types.NewBytesDatum([]byte("test_proto_bytes")), tBytes, rpc.BytesProto([]byte("test_proto_bytes"))},
		{types.NewBytesDatum([]byte{}), tBytes, rpc.BytesProto([]byte{})},
		{types.NewDatum(nil), tBytes, rpc.NullProto()},
	}

	for i, tc := range testCases {
		pv, err := protoValueFromDatum(tc.d, tc.pt)
		c.Logf("%d:[%s] %+v %+v", i, tc.pt, pv, tc.wanted)
		c.Assert(err, IsNil)
		c.Assert(pv, DeepEquals, tc.wanted)
	}
}

func (ts *testValueSuite) TestProtoValue2ProtoValue(c *C) {
	// c.Skip("testing debug")
	testCases := []struct {
		pt     *tspb.Type
		pv     *tspb.Value
		wanted *tspb.Value
	}{
		{tBool, rpc.BoolProto(true), rpc.BoolProto(true)},
		{tBool, rpc.BoolProto(false), rpc.BoolProto(false)},
		{tBool, rpc.NullProto(), rpc.NullProto()},

		{tInt, rpc.IntProto(math.MaxInt16), rpc.IntProto(math.MaxInt16)},
		{tInt, rpc.IntProto(math.MaxInt32), rpc.IntProto(math.MaxInt32)},
		{tInt, rpc.IntProto(math.MaxInt64), rpc.IntProto(math.MaxInt64)},
		{tInt, rpc.IntProto(math.MinInt64), rpc.IntProto(math.MinInt64)},
		{tInt, rpc.IntProto(math.MinInt32), rpc.IntProto(math.MinInt32)},
		{tInt, rpc.IntProto(math.MinInt16), rpc.IntProto(math.MinInt16)},
		{tInt, rpc.NullProto(), rpc.NullProto()},

		{tFloat, rpc.FloatProto(math.MaxFloat64), rpc.FloatProto(math.MaxFloat64)},
		{tFloat, rpc.FloatProto(math.MaxFloat32), rpc.FloatProto(math.MaxFloat32)},
		{tFloat, rpc.NullProto(), rpc.NullProto()},
		{tFloat, rpc.FloatProto(-1.234), rpc.FloatProto(-1.234)},

		{tTime, rpc.TimeProto(T1), rpc.TimeProto(T3)},
		{tTime, rpc.TimeProto(T2), rpc.TimeProto(T2)},
		{tTime, rpc.NullProto(), rpc.NullProto()},
		{tDate, rpc.DateProto(Date1), rpc.DateProto(Date1)},
		{tDate, rpc.DateProto(Date2), rpc.DateProto(Date2)},
		{tDate, rpc.NullProto(), rpc.NullProto()},

		{tString, rpc.StringProto("test_proto_value"), rpc.StringProto("test_proto_value")},
		{tString, rpc.StringProto(""), rpc.StringProto("")},
		{tString, rpc.NullProto(), rpc.NullProto()},
		{tBytes, rpc.BytesProto([]byte("test_proto_bytes")), rpc.BytesProto([]byte("test_proto_bytes"))},
		{tBytes, rpc.BytesProto([]byte{}), rpc.BytesProto([]byte{})},
		{tBytes, rpc.NullProto(), rpc.NullProto()},
	}
	for i, tc := range testCases {
		datum, err := datumFromTypeValue(tc.pt, tc.pv)
		c.Assert(err, IsNil)
		val, err := protoValueFromDatum(datum, tc.pt)
		c.Assert(err, IsNil)
		c.Logf("%d:[%s] %+v %+v", i, tc.pt, tc.pv, val)
		c.Assert(val, DeepEquals, tc.wanted)
	}
}
