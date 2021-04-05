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
	"fmt"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/charset"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
	"github.com/pingcap/tidb/util/timeutil"
	tspb "github.com/zhihu/zetta-proto/pkg/tablestore"
	"github.com/zhihu/zetta/tablestore/mysql/expression"
	"strings"
	"time"
)

type columnEntry struct {
	id        int64
	name      string
	t         *tspb.Type
	ft        *types.FieldType
	cfID      int64
	cf        string
	idx       int
	IsPrimary bool
}

func (c columnEntry) ColumnName() string {
	if c.cf != DefaultColumnFamily {
		return c.cf + ":" + c.name
	}
	return c.name
}

type colSparseEntry struct {
	id     int64
	key    kv.Key
	val    []byte
	column string
	cf     string
	cfID   int64
	data   types.Datum
}

type Field struct {
	Family string
	Name   string
	Type   *tspb.Type
	Value  *tspb.Value
}

// CastValue casts a value based on column type.
// If forceIgnoreTruncate is true, truncated errors will be ignored.
// If returnOverflow is true, don't handle overflow errors in this function.
// It's safe now and it's the same as the behavior of select statement.
// Set it to true only in FillVirtualColumnValue and UnionScanExec.Next()
// If the handle of err is changed latter, the behavior of forceIgnoreTruncate also need to change.
// TODO: change the third arg to TypeField. Not pass ColumnInfo.
func CastValue(ctx sessionctx.Context, val types.Datum, col *model.ColumnInfo, returnOverflow, forceIgnoreTruncate bool) (casted types.Datum, err error) {
	sc := ctx.GetSessionVars().StmtCtx
	casted, err = val.ConvertTo(sc, &col.FieldType)
	return casted, err
}

// GetColDefaultValue gets default value of the column.
func GetColDefaultValue(ctx sessionctx.Context, col *model.ColumnInfo) (types.Datum, error) {
	defaultValue := col.GetDefaultValue()
	if !col.DefaultIsExpr {
		return getColDefaultValue(ctx, col, defaultValue)
	}
	return getColDefaultExprValue(ctx, col, defaultValue.(string))
}

// EvalColDefaultExpr eval default expr node to explicit default value.
func EvalColDefaultExpr(ctx sessionctx.Context, col *model.ColumnInfo, defaultExpr ast.ExprNode) (types.Datum, error) {
	d, err := expression.EvalAstExpr(ctx, defaultExpr)
	if err != nil {
		return types.Datum{}, err
	}
	// Check the evaluated data type by cast.
	value, err := CastValue(ctx, d, col, false, false)
	if err != nil {
		return types.Datum{}, err
	}
	return value, nil
}

func getColDefaultExprValue(ctx sessionctx.Context, col *model.ColumnInfo, defaultValue string) (types.Datum, error) {
	var defaultExpr ast.ExprNode
	expr := fmt.Sprintf("select %s", defaultValue)
	stmts, _, err := parser.New().Parse(expr, "", "")
	if err == nil {
		defaultExpr = stmts[0].(*ast.SelectStmt).Fields.Fields[0].Expr
	}
	d, err := expression.EvalAstExpr(ctx, defaultExpr)
	if err != nil {
		return types.Datum{}, err
	}
	// Check the evaluated data type by cast.
	value, err := CastValue(ctx, d, col, false, false)
	if err != nil {
		return types.Datum{}, err
	}
	return value, nil
}

func getColDefaultValue(ctx sessionctx.Context, col *model.ColumnInfo, defaultVal interface{}) (types.Datum, error) {
	if defaultVal == nil {
		return getColDefaultValueFromNil(ctx, col)
	}

	if col.Tp != mysql.TypeTimestamp && col.Tp != mysql.TypeDatetime {
		value, err := CastValue(ctx, types.NewDatum(defaultVal), col, false, false)
		if err != nil {
			return types.Datum{}, err
		}
		return value, nil
	}

	// Check and get timestamp/datetime default value.
	sc := ctx.GetSessionVars().StmtCtx
	var needChangeTimeZone bool
	// If the column's default value is not ZeroDatetimeStr nor CurrentTimestamp, should use the time zone of the default value itself.
	if col.Tp == mysql.TypeTimestamp {
		if vv, ok := defaultVal.(string); ok && vv != types.ZeroDatetimeStr && !strings.EqualFold(vv, ast.CurrentTimestamp) {
			needChangeTimeZone = true
			originalTZ := sc.TimeZone
			// For col.Version = 0, the timezone information of default value is already lost, so use the system timezone as the default value timezone.
			sc.TimeZone = timeutil.SystemLocation()
			if col.Version >= model.ColumnInfoVersion1 {
				sc.TimeZone = time.UTC
			}
			defer func() { sc.TimeZone = originalTZ }()
		}
	}
	value, err := expression.GetTimeValue(ctx, defaultVal, col.Tp, int8(col.Decimal))
	if err != nil {
		return types.Datum{}, err
	}
	// If the column's default value is not ZeroDatetimeStr or CurrentTimestamp, convert the default value to the current session time zone.
	if needChangeTimeZone {
		t := value.GetMysqlTime()
		err = t.ConvertTimeZone(sc.TimeZone, ctx.GetSessionVars().Location())
		if err != nil {
			return value, err
		}
		value.SetMysqlTime(t)
	}
	return value, nil
}

func getColDefaultValueFromNil(ctx sessionctx.Context, col *model.ColumnInfo) (types.Datum, error) {
	if !mysql.HasNotNullFlag(col.Flag) {
		return types.Datum{}, nil
	}
	if col.Tp == mysql.TypeEnum {
		// For enum type, if no default value and not null is set,
		// the default value is the first element of the enum list
		defEnum, err := types.ParseEnumValue(col.FieldType.Elems, 1)
		if err != nil {
			return types.Datum{}, err
		}
		return types.NewCollateMysqlEnumDatum(defEnum, col.Collate), nil
	}
	if mysql.HasAutoIncrementFlag(col.Flag) {
		// Auto increment column doesn't has default value and we should not return error.
		return GetZeroValue(col), nil
	}
	vars := ctx.GetSessionVars()
	sc := vars.StmtCtx
	if sc.BadNullAsWarning {
		return GetZeroValue(col), nil
	}
	if !vars.StrictSQLMode {
		return GetZeroValue(col), nil
	}
	return types.Datum{}, fmt.Errorf("no default value: %s", col.Name.L)
}

// GetZeroValue gets zero value for given column type.
func GetZeroValue(col *model.ColumnInfo) types.Datum {
	var d types.Datum
	switch col.Tp {
	case mysql.TypeTiny, mysql.TypeInt24, mysql.TypeShort, mysql.TypeLong, mysql.TypeLonglong, mysql.TypeYear:
		if mysql.HasUnsignedFlag(col.Flag) {
			d.SetUint64(0)
		} else {
			d.SetInt64(0)
		}
	case mysql.TypeFloat:
		d.SetFloat32(0)
	case mysql.TypeDouble:
		d.SetFloat64(0)
	case mysql.TypeNewDecimal:
		d.SetLength(col.Flen)
		d.SetFrac(col.Decimal)
		d.SetMysqlDecimal(new(types.MyDecimal))
	case mysql.TypeString:
		if col.Flen > 0 && col.Charset == charset.CharsetBin {
			d.SetBytes(make([]byte, col.Flen))
		} else {
			d.SetString("", col.Collate)
		}
	case mysql.TypeVarString, mysql.TypeVarchar:
		d.SetString("", col.Collate)
	case mysql.TypeBlob, mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:
		d.SetBytes([]byte{})
	case mysql.TypeDuration:
		d.SetMysqlDuration(types.ZeroDuration)
	case mysql.TypeDate:
		d.SetMysqlTime(types.ZeroDate)
	case mysql.TypeTimestamp:
		d.SetMysqlTime(types.ZeroTimestamp)
	case mysql.TypeDatetime:
		d.SetMysqlTime(types.ZeroDatetime)
	case mysql.TypeBit:
		d.SetMysqlBit(types.ZeroBinaryLiteral)
	case mysql.TypeSet:
		d.SetMysqlSet(types.Set{}, col.Collate)
	case mysql.TypeEnum:
		d.SetMysqlEnum(types.Enum{}, col.Collate)
	case mysql.TypeJSON:
		d.SetMysqlJSON(json.CreateBinary(nil))
	}
	return d
}
