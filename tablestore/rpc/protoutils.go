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

package rpc

import (
	"time"

	"cloud.google.com/go/civil"
	"github.com/gogo/protobuf/types"
	tspb "github.com/zhihu/zetta-proto/pkg/tablestore"
	// sppb "google.golang.org/genproto/googleapis/spanner/v1"
)

// Helpers to generate protobuf values and Cloud Spanner types.
func StringProto(s string) *tspb.Value {
	return stringProto(s)
}

func StringType() *tspb.Type {
	return stringType()
}

func stringProto(s string) *tspb.Value {
	return &tspb.Value{Kind: stringKind(s)}
}

func stringKind(s string) *tspb.Value_StringValue {
	return &tspb.Value_StringValue{StringValue: s}
}

func stringType() *tspb.Type {
	return &tspb.Type{Code: tspb.TypeCode_STRING}
}

func BoolProto(b bool) *tspb.Value {
	return boolProto(b)
}

func BoolType() *tspb.Type {
	return boolType()
}

func boolProto(b bool) *tspb.Value {
	return &tspb.Value{Kind: &tspb.Value_BoolValue{BoolValue: b}}
}

func boolType() *tspb.Type {
	return &tspb.Type{Code: tspb.TypeCode_BOOL}
}

func IntProto(n int64) *tspb.Value {
	return intProto(n)
}

func IntType() *tspb.Type {
	return &tspb.Type{Code: tspb.TypeCode_INT64}
}

func intProto(n int64) *tspb.Value {
	return &tspb.Value{Kind: &tspb.Value_IntegerValue{IntegerValue: n}}
}

func intType() *tspb.Type {
	return &tspb.Type{Code: tspb.TypeCode_INT64}
}

func FloatProto(n float64) *tspb.Value {
	return floatProto(n)
}

func FloatType() *tspb.Type {
	return floatType()
}

func floatProto(n float64) *tspb.Value {
	return &tspb.Value{Kind: &tspb.Value_NumberValue{NumberValue: n}}
}

func floatType() *tspb.Type {
	return &tspb.Type{Code: tspb.TypeCode_FLOAT64}
}

func BytesProto(b []byte) *tspb.Value {
	return bytesProto(b)
}

func BytesType() *tspb.Type {
	return &tspb.Type{Code: tspb.TypeCode_BYTES}
}

func bytesProto(b []byte) *tspb.Value {
	return &tspb.Value{Kind: &tspb.Value_BytesValue{BytesValue: b}}
}

func bytesType() *tspb.Type {
	return &tspb.Type{Code: tspb.TypeCode_BYTES}
}

func bytesKind(b []byte) *tspb.Value_BytesValue {
	return &tspb.Value_BytesValue{BytesValue: b}
}

func TimeProto(t time.Time) *tspb.Value {
	return timeProto(t)
}

func TimeType() *tspb.Type {
	return timeType()
}

func timeKind(t time.Time) *tspb.Value_TimestampValue {
	tsv := &types.Timestamp{
		Seconds: t.Unix(),
		Nanos:   int32(t.UnixNano() % 1e9),
	}
	return &tspb.Value_TimestampValue{TimestampValue: tsv}
}

func timeProto(t time.Time) *tspb.Value {
	tsv := &types.Timestamp{
		Seconds: t.Unix(),
		Nanos:   int32(t.UnixNano() % 1e9),
	}
	return &tspb.Value{Kind: &tspb.Value_TimestampValue{TimestampValue: tsv}}
}

func timeType() *tspb.Type {
	return &tspb.Type{Code: tspb.TypeCode_TIMESTAMP}
}

func DateProto(d civil.Date) *tspb.Value {
	return dateProto(d)
}

func DateType() *tspb.Type {
	return dateType()
}

func DateKind(d civil.Date) *tspb.Value_TimestampValue {
	tt := d.In(time.Local)
	tsv := &types.Timestamp{
		Seconds: tt.Unix(),
		Nanos:   int32(tt.UnixNano() % 1e9),
	}
	return &tspb.Value_TimestampValue{TimestampValue: tsv}
}

func dateProto(d civil.Date) *tspb.Value {
	tt := d.In(time.Local)
	return timeProto(tt)
}

func dateType() *tspb.Type {
	return &tspb.Type{Code: tspb.TypeCode_DATE}
}

func listProto(p ...*tspb.Value) *tspb.Value {
	return &tspb.Value{Kind: &tspb.Value_ListValue{ListValue: &tspb.ListValue{Values: p}}}
}

func listValueProto(p ...*tspb.Value) *tspb.ListValue {
	return &tspb.ListValue{Values: p}
}

func ListType(t *tspb.Type) *tspb.Type {
	return listType(t)
}

func listType(t *tspb.Type) *tspb.Type {
	return &tspb.Type{Code: tspb.TypeCode_ARRAY, ArrayElementType: t}
}

func mkField(n string, t *tspb.Type) *tspb.StructType_Field {
	return &tspb.StructType_Field{Name: n, Type: t}
}

func structType(fields ...*tspb.StructType_Field) *tspb.Type {
	return &tspb.Type{Code: tspb.TypeCode_STRUCT, StructType: &tspb.StructType{Fields: fields}}
}

func NullProto() *tspb.Value {
	return nullProto()
}

func nullProto() *tspb.Value {
	return &tspb.Value{Kind: &tspb.Value_NullValue{NullValue: tspb.NullValue_NULL_VALUE}}
}
