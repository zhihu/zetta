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
	"context"
	"encoding/base64"
	"fmt"
	"strconv"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"cloud.google.com/go/civil"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/logutil"

	tspb "github.com/zhihu/zetta-proto/pkg/tablestore"
	"github.com/zhihu/zetta/tablestore/rpc"
)

func datumFromTypeValue(pt *tspb.Type, v *tspb.Value) (types.Datum, error) {
	var rd types.Datum

	if _, ok := v.Kind.(*tspb.Value_NullValue); ok {
		rd = types.NewDatum(nil)
		return rd, nil
	}
	switch pt.GetCode() {
	case tspb.TypeCode_BOOL:
		val, ok := v.Kind.(*tspb.Value_BoolValue)
		if !ok {
			return rd, status.Errorf(codes.FailedPrecondition, "invaild value-type %T for column type %v", v.Kind, pt.String())
		}
		rd = types.NewDatum(val.BoolValue)

	case tspb.TypeCode_INT64:
		var val int64
		if err := rpc.DecodeValue(v, pt, &val); err != nil {
			logutil.Logger(context.Background()).Error("decode value err", zap.Error(err))
			return rd, err
		}
		rd = types.NewIntDatum(val)

	case tspb.TypeCode_FLOAT64:
		val, ok := v.Kind.(*tspb.Value_NumberValue)
		if !ok {
			return rd, status.Errorf(codes.FailedPrecondition, "invaild value-type %T for column type %v", v.Kind, pt.String())
		}
		rd = types.NewFloat64Datum(val.NumberValue)

	case tspb.TypeCode_TIMESTAMP:
		var ts time.Time

		if err := rpc.DecodeValue(v, rpc.TimeType(), &ts); err != nil {
			logutil.Logger(context.Background()).Error("decode value err", zap.Error(err))
			return rd, err
		}
		val := types.NewTime(types.FromGoTime(ts), mysql.TypeTimestamp, types.DefaultFsp)
		rd = types.NewTimeDatum(val)

	case tspb.TypeCode_DATE:
		var date civil.Date
		if err := rpc.DecodeValue(v, rpc.DateType(), &date); err != nil {
			logutil.Logger(context.Background()).Error("decode value err", zap.Error(err))
			return rd, err
		}
		coreTime := types.FromDate(date.Year, int(date.Month), date.Day, 0, 0, 0, 0)
		tt := types.NewTime(coreTime, mysql.TypeDate, types.DefaultFsp)
		rd = types.NewTimeDatum(tt)

	case tspb.TypeCode_STRING:
		val, ok := v.Kind.(*tspb.Value_StringValue)
		if !ok {
			return rd, status.Errorf(codes.FailedPrecondition, "invaild value-type %T for column type %v", v.Kind, pt.String())
		}
		rd = types.NewStringDatum(val.StringValue)

	case tspb.TypeCode_BYTES:

		var val []byte
		if err := rpc.DecodeValue(v, rpc.BytesType(), &val); err != nil {
			logutil.Logger(context.Background()).Error("decode value err", zap.Error(err))
			return rd, err
		}
		rd = types.NewBytesDatum(val)

	default:
		rd = types.NewDatum(nil)
		return rd, status.Errorf(codes.InvalidArgument, "unspecified type code: %v", pt)

	}
	return rd, nil
}

func protoValueFromDatum(d types.Datum, pt *tspb.Type) (*tspb.Value, error) {
	if d.IsNull() {
		return rpc.NullProto(), nil
	}
	switch pt.Code {
	case tspb.TypeCode_BOOL:
		x := (d.GetInt64() > 0)
		return rpc.BoolProto(x), nil
	case tspb.TypeCode_INT64:
		s := d.GetInt64()
		return rpc.IntProto(s), nil
	case tspb.TypeCode_FLOAT64:
		x := d.GetFloat64()
		return rpc.FloatProto(x), nil
	case tspb.TypeCode_TIMESTAMP:
		x := d.GetMysqlTime()
		switch x.Type() {
		case mysql.TypeTimestamp:
			tt, err := x.GoTime(time.Local)
			if err != nil {
				return nil, status.Errorf(codes.FailedPrecondition, "datum wasn't correctly encoded: <%v>", err)
			}
			return rpc.TimeProto(tt), nil
		}
		return nil, status.Errorf(codes.FailedPrecondition, "datum wasn't correctly encoded for mysqlTimestamp")

	case tspb.TypeCode_DATE:
		x := d.GetMysqlTime()
		switch x.Type() {
		case mysql.TypeDate, mysql.TypeDatetime:
			tt, err := x.GoTime(time.Local)
			if err != nil {
				return nil, status.Errorf(codes.FailedPrecondition, "datum wasn't correctly encoded: <%v>", err)
			}
			cdate := civil.DateOf(tt)
			return rpc.DateProto(cdate), nil
		}
		return nil, status.Errorf(codes.FailedPrecondition, "datum wasn't correctly encoded for mysqlDate")
	case tspb.TypeCode_STRING:
		x := d.GetString()
		return rpc.StringProto(x), nil

	case tspb.TypeCode_BYTES:
		x := d.GetBytes()
		return rpc.BytesProto(x), nil
	default:
		return nil, status.Errorf(codes.Aborted, "unrecognized datum type (%v)", pt.Code)
	}
}

func flexibleProtoValueFromDatum(d types.Datum) (*tspb.Value, *tspb.Type, error) {

	switch d.Kind() {
	case types.KindBytes:
		return rpc.BytesProto(d.GetBytes()), rpc.BytesType(), nil
	case types.KindInt64:
		return rpc.IntProto(d.GetInt64()), rpc.IntType(), nil
	case types.KindFloat64:
		return rpc.FloatProto(d.GetFloat64()), rpc.FloatType(), nil
	case types.KindNull:
		return rpc.NullProto(), nil, nil
	}
	return nil, nil, status.Errorf(codes.FailedPrecondition, "invalid type (%v) for sparse column", d.Kind())
}

func flexibleDatumFromProtoValue(v *tspb.Value) (types.Datum, error) {
	switch v.Kind.(type) {
	case *tspb.Value_IntegerValue:
		return types.NewIntDatum(v.GetIntegerValue()), nil
	case *tspb.Value_NumberValue:
		return types.NewFloat64Datum(v.GetNumberValue()), nil
	case *tspb.Value_BytesValue:
		return types.NewBytesDatum(v.GetBytesValue()), nil
	}
	return types.Datum{}, status.Errorf(codes.FailedPrecondition, "unsupport type (%T) for sparse proto value", v.Kind)
}

// func protoValueFromDatum(d types.Datum, pt *types.T)
func protoTypeFromField(ft *types.FieldType) *tspb.Type {
	switch ft.Tp {
	case mysql.TypeTiny:
		return rpc.BoolType()
	case mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong, mysql.TypeShort:
		return rpc.IntType()
	case mysql.TypeFloat, mysql.TypeDouble, mysql.TypeDecimal:
		return rpc.FloatType()
	case mysql.TypeTimestamp, mysql.TypeDuration:
		return rpc.TimeType()
	case mysql.TypeDate, mysql.TypeDatetime:
		return rpc.DateType()
	case mysql.TypeVarString, mysql.TypeVarchar, mysql.TypeString:
		return rpc.StringType()
	case mysql.TypeBit, mysql.TypeBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob, mysql.TypeTinyBlob:
		return rpc.BytesType()
	case mysql.TypeSet:
		return rpc.ListType(rpc.StringType())
	case mysql.TypeJSON:
		return &tspb.Type{Code: tspb.TypeCode_STRUCT}
	default:
		return &tspb.Type{Code: tspb.TypeCode_TYPE_CODE_UNSPECIFIED}
	}
}

func protoValueFromValue(x interface{}) (*tspb.Value, error) {
	switch x := x.(type) {
	default:
		return nil, fmt.Errorf("unhandled database value type %T", x)
	case bool:
		return &tspb.Value{Kind: &tspb.Value_BoolValue{BoolValue: x}}, nil
	case int64:
		// The Spanner int64 is actually a decimal string.
		s := strconv.FormatInt(x, 10)
		return &tspb.Value{Kind: &tspb.Value_StringValue{StringValue: s}}, nil
	case float64:
		return &tspb.Value{Kind: &tspb.Value_NumberValue{NumberValue: x}}, nil
	case string:
		return &tspb.Value{Kind: &tspb.Value_StringValue{StringValue: x}}, nil
	case []byte:
		return &tspb.Value{Kind: &tspb.Value_StringValue{StringValue: base64.StdEncoding.EncodeToString(x)}}, nil
	case nil:
		return &tspb.Value{Kind: &tspb.Value_NullValue{}}, nil
	case []interface{}:
		var vs []*tspb.Value
		for _, elem := range x {
			v, err := protoValueFromValue(elem)
			if err != nil {
				return nil, err
			}
			vs = append(vs, v)
		}
		return &tspb.Value{Kind: &tspb.Value_ListValue{
			ListValue: &tspb.ListValue{Values: vs},
		}}, nil
	}
}

func protoType2FieldType(pt *tspb.Type) *types.FieldType {
	switch pt.Code {
	case tspb.TypeCode_BOOL:
		return types.NewFieldType(mysql.TypeTiny)
	case tspb.TypeCode_INT64:
		return types.NewFieldType(mysql.TypeLonglong)
	case tspb.TypeCode_FLOAT64:
		return types.NewFieldType(mysql.TypeDouble)
	case tspb.TypeCode_TIMESTAMP:
		return types.NewFieldType(mysql.TypeTimestamp)
	case tspb.TypeCode_DATE:
		return types.NewFieldType(mysql.TypeDate)
	case tspb.TypeCode_STRING:
		return types.NewFieldType(mysql.TypeVarchar)
	case tspb.TypeCode_BYTES:
		return types.NewFieldType(mysql.TypeBlob)
	case tspb.TypeCode_STRUCT:
		return types.NewFieldType(mysql.TypeJSON)
	default:
		return types.NewFieldType(mysql.TypeNull)
	}
}
