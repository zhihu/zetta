// Copyright 2013 The ql Authors. All rights reserved.
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

//go:generate go run generator/compare_vec.go
//go:generate go run generator/control_vec.go
//go:generate go run generator/other_vec.go
//go:generate go run generator/string_vec.go
//go:generate go run generator/time_vec.go

package expression

import (
	"sort"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/charset"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/opcode"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
	"github.com/pingcap/tidb/util/chunk"
	tipb "github.com/pingcap/tipb/go-tipb"
	"github.com/zhihu/zetta/tablestore/mysql/sctx"
)

// baseBuiltinFunc will be contained in every struct that implement builtinFunc interface.
type baseBuiltinFunc struct {
	args   []Expression
	ctx    sctx.Context
	tp     *types.FieldType
	pbCode tipb.ScalarFuncSig
	collationInfo
}

func newBaseBuiltinFunc(ctx sctx.Context, funcName string, args []Expression) (baseBuiltinFunc, error) {
	if ctx == nil {
		panic("ctx should not be nil")
	}
	bf := baseBuiltinFunc{
		args: args,
		ctx:  ctx,
		tp:   types.NewFieldType(mysql.TypeUnspecified),
	}
	return bf, nil
}

// newBaseBuiltinFuncWithTp creates a built-in function signature with specified types of arguments and the return type of the function.
// argTps indicates the types of the args, retType indicates the return type of the built-in function.
// Every built-in function needs determined argTps and retType when we create it.
func newBaseBuiltinFuncWithTp(ctx sctx.Context, funcName string, args []Expression, retType types.EvalType, argTps ...types.EvalType) (bf baseBuiltinFunc, err error) {
	if len(args) != len(argTps) {
		panic("unexpected length of args and argTps")
	}
	if ctx == nil {
		panic("ctx should not be nil")
	}

	var fieldType *types.FieldType
	switch retType {
	case types.ETInt:
		fieldType = &types.FieldType{
			Tp:      mysql.TypeLonglong,
			Flen:    mysql.MaxIntWidth,
			Decimal: 0,
			Flag:    mysql.BinaryFlag,
		}
	case types.ETReal:
		fieldType = &types.FieldType{
			Tp:      mysql.TypeDouble,
			Flen:    mysql.MaxRealWidth,
			Decimal: types.UnspecifiedLength,
			Flag:    mysql.BinaryFlag,
		}
	case types.ETDecimal:
		fieldType = &types.FieldType{
			Tp:      mysql.TypeNewDecimal,
			Flen:    11,
			Decimal: 0,
			Flag:    mysql.BinaryFlag,
		}
	case types.ETString:
		fieldType = &types.FieldType{
			Tp:      mysql.TypeVarString,
			Decimal: types.UnspecifiedLength,
			Flen:    types.UnspecifiedLength,
		}
	case types.ETDatetime:
		fieldType = &types.FieldType{
			Tp:      mysql.TypeDatetime,
			Flen:    mysql.MaxDatetimeWidthWithFsp,
			Decimal: int(types.MaxFsp),
			Flag:    mysql.BinaryFlag,
		}
	case types.ETTimestamp:
		fieldType = &types.FieldType{
			Tp:      mysql.TypeTimestamp,
			Flen:    mysql.MaxDatetimeWidthWithFsp,
			Decimal: int(types.MaxFsp),
			Flag:    mysql.BinaryFlag,
		}
	case types.ETDuration:
		fieldType = &types.FieldType{
			Tp:      mysql.TypeDuration,
			Flen:    mysql.MaxDurationWidthWithFsp,
			Decimal: int(types.MaxFsp),
			Flag:    mysql.BinaryFlag,
		}
	case types.ETJson:
		fieldType = &types.FieldType{
			Tp:      mysql.TypeJSON,
			Flen:    mysql.MaxBlobWidth,
			Decimal: 0,
			Charset: mysql.DefaultCharset,
			Collate: mysql.DefaultCollationName,
			Flag:    mysql.BinaryFlag,
		}
	}
	if mysql.HasBinaryFlag(fieldType.Flag) && fieldType.Tp != mysql.TypeJSON {
		fieldType.Charset, fieldType.Collate = charset.CharsetBin, charset.CollationBin
	}
	bf = baseBuiltinFunc{
		args: args,
		ctx:  ctx,
		tp:   fieldType,
	}
	return bf, nil
}

func (b *baseBuiltinFunc) getArgs() []Expression {
	return b.args
}

func (b *baseBuiltinFunc) setPbCode(c tipb.ScalarFuncSig) {
	b.pbCode = c
}

func (b *baseBuiltinFunc) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("baseBuiltinFunc.vecEvalInt() should never be called, please contact the TiDB team for help")
}

func (b *baseBuiltinFunc) vecEvalReal(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("baseBuiltinFunc.vecEvalReal() should never be called, please contact the TiDB team for help")
}

func (b *baseBuiltinFunc) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("baseBuiltinFunc.vecEvalString() should never be called, please contact the TiDB team for help")
}

func (b *baseBuiltinFunc) vecEvalDecimal(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("baseBuiltinFunc.vecEvalDecimal() should never be called, please contact the TiDB team for help")
}

func (b *baseBuiltinFunc) vecEvalTime(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("baseBuiltinFunc.vecEvalTime() should never be called, please contact the TiDB team for help")
}

func (b *baseBuiltinFunc) vecEvalDuration(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("baseBuiltinFunc.vecEvalDuration() should never be called, please contact the TiDB team for help")
}

func (b *baseBuiltinFunc) vecEvalJSON(input *chunk.Chunk, result *chunk.Column) error {
	return errors.Errorf("baseBuiltinFunc.vecEvalJSON() should never be called, please contact the TiDB team for help")
}

func (b *baseBuiltinFunc) evalInt(row chunk.Row) (int64, bool, error) {
	return 0, false, errors.Errorf("baseBuiltinFunc.evalInt() should never be called, please contact the TiDB team for help")
}

func (b *baseBuiltinFunc) evalReal(row chunk.Row) (float64, bool, error) {
	return 0, false, errors.Errorf("baseBuiltinFunc.evalReal() should never be called, please contact the TiDB team for help")
}

func (b *baseBuiltinFunc) evalString(row chunk.Row) (string, bool, error) {
	return "", false, errors.Errorf("baseBuiltinFunc.evalString() should never be called, please contact the TiDB team for help")
}

func (b *baseBuiltinFunc) evalDecimal(row chunk.Row) (*types.MyDecimal, bool, error) {
	return nil, false, errors.Errorf("baseBuiltinFunc.evalDecimal() should never be called, please contact the TiDB team for help")
}

func (b *baseBuiltinFunc) evalTime(row chunk.Row) (types.Time, bool, error) {
	return types.ZeroTime, false, errors.Errorf("baseBuiltinFunc.evalTime() should never be called, please contact the TiDB team for help")
}

func (b *baseBuiltinFunc) evalDuration(row chunk.Row) (types.Duration, bool, error) {
	return types.Duration{}, false, errors.Errorf("baseBuiltinFunc.evalDuration() should never be called, please contact the TiDB team for help")
}

func (b *baseBuiltinFunc) evalJSON(row chunk.Row) (json.BinaryJSON, bool, error) {
	return json.BinaryJSON{}, false, errors.Errorf("baseBuiltinFunc.evalJSON() should never be called, please contact the TiDB team for help")
}

func (b *baseBuiltinFunc) vectorized() bool {
	return false
}

func (b *baseBuiltinFunc) supportReverseEval() bool {
	return false
}

func (b *baseBuiltinFunc) reverseEval(sc *stmtctx.StatementContext, res types.Datum, rType types.RoundingType) (types.Datum, error) {
	return types.Datum{}, errors.Errorf("baseBuiltinFunc.reverseEvalInt() should never be called, please contact the TiDB team for help")
}

func (b *baseBuiltinFunc) getRetTp() *types.FieldType {
	switch b.tp.EvalType() {
	case types.ETString:
		if b.tp.Flen >= mysql.MaxBlobWidth {
			b.tp.Tp = mysql.TypeLongBlob
		} else if b.tp.Flen >= 65536 {
			b.tp.Tp = mysql.TypeMediumBlob
		}
		if len(b.tp.Charset) <= 0 {
			b.tp.Charset, b.tp.Collate = charset.GetDefaultCharsetAndCollate()
		}
	}
	return b.tp
}

func (b *baseBuiltinFunc) equal(fun builtinFunc) bool {
	funArgs := fun.getArgs()
	if len(funArgs) != len(b.args) {
		return false
	}
	for i := range b.args {
		if !b.args[i].Equal(b.ctx, funArgs[i]) {
			return false
		}
	}
	return true
}

func (b *baseBuiltinFunc) getCtx() sctx.Context {
	return b.ctx
}

func (b *baseBuiltinFunc) cloneFrom(from *baseBuiltinFunc) {
	b.args = make([]Expression, 0, len(b.args))
	for _, arg := range from.args {
		b.args = append(b.args, arg.Clone())
	}
	b.ctx = from.ctx
	b.tp = from.tp
}

func (b *baseBuiltinFunc) Clone() builtinFunc {
	panic("you should not call this method.")
}

// builtinFunc stands for a particular function signature.
type builtinFunc interface {
	// evalInt evaluates int result of builtinFunc by given row.
	evalInt(row chunk.Row) (val int64, isNull bool, err error)
	// evalReal evaluates real representation of builtinFunc by given row.
	evalReal(row chunk.Row) (val float64, isNull bool, err error)
	// evalString evaluates string representation of builtinFunc by given row.
	evalString(row chunk.Row) (val string, isNull bool, err error)
	// evalDecimal evaluates decimal representation of builtinFunc by given row.
	evalDecimal(row chunk.Row) (val *types.MyDecimal, isNull bool, err error)
	// evalTime evaluates DATE/DATETIME/TIMESTAMP representation of builtinFunc by given row.
	evalTime(row chunk.Row) (val types.Time, isNull bool, err error)
	// evalDuration evaluates duration representation of builtinFunc by given row.
	evalDuration(row chunk.Row) (val types.Duration, isNull bool, err error)
	// evalJSON evaluates JSON representation of builtinFunc by given row.
	evalJSON(row chunk.Row) (val json.BinaryJSON, isNull bool, err error)
	// getArgs returns the arguments expressions.
	getArgs() []Expression
	// equal check if this function equals to another function.
	equal(builtinFunc) bool
	// getCtx returns this function's context.
	getCtx() sctx.Context
	// getRetTp returns the return type of the built-in function.
	getRetTp() *types.FieldType
	// Clone returns a copy of itself.
	Clone() builtinFunc
	// setPbCode sets pbCode for signature.
	setPbCode(tipb.ScalarFuncSig)
}

// baseFunctionClass will be contained in every struct that implement functionClass interface.
type baseFunctionClass struct {
	funcName string
	minArgs  int
	maxArgs  int
}

func (b *baseFunctionClass) verifyArgs(args []Expression) error {
	l := len(args)
	if l < b.minArgs || (b.maxArgs != -1 && l > b.maxArgs) {
		return ErrIncorrectParameterCount.GenWithStackByArgs(b.funcName)
	}
	return nil
}

// functionClass is the interface for a function which may contains multiple functions.
type functionClass interface {
	// getFunction gets a function signature by the types and the counts of given arguments.
	getFunction(ctx sctx.Context, args []Expression) (builtinFunc, error)
}

// funcs holds all registered builtin functions. When new function is added,
// check expression/function_traits.go to see if it should be appended to
// any set there.
var funcs = map[string]functionClass{
	// information functions
	ast.Database: &databaseFunctionClass{baseFunctionClass{ast.Database, 0, 0}},
	// This function is a synonym for DATABASE().
	// See http://dev.mysql.com/doc/refman/5.7/en/information-functions.html#function_schema
	ast.Schema: &databaseFunctionClass{baseFunctionClass{ast.Schema, 0, 0}},

	ast.EQ: &compareFunctionClass{baseFunctionClass{ast.EQ, 2, 2}, opcode.EQ},
	ast.LE: &compareFunctionClass{baseFunctionClass{ast.LE, 2, 2}, opcode.LE},
	ast.GT: &compareFunctionClass{baseFunctionClass{ast.GT, 2, 2}, opcode.GT},
}

// IsFunctionSupported check if given function name is a builtin sql function.
func IsFunctionSupported(name string) bool {
	_, ok := funcs[name]
	return ok
}

// GetBuiltinList returns a list of builtin functions
func GetBuiltinList() []string {
	res := make([]string, 0, len(funcs))
	notImplementedFunctions := []string{ast.RowFunc}
	for funcName := range funcs {
		skipFunc := false
		// Skip not implemented functions
		for _, notImplFunc := range notImplementedFunctions {
			if funcName == notImplFunc {
				skipFunc = true
			}
		}
		// Skip literal functions
		// (their names are not readable: 'tidb`.(dateliteral, for example)
		// See: https://github.com/pingcap/parser/pull/591
		if strings.HasPrefix(funcName, "'tidb`.(") {
			skipFunc = true
		}
		if skipFunc {
			continue
		}
		res = append(res, funcName)
	}
	sort.Strings(res)
	return res
}
