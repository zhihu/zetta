package expression

import (
	"math"

	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/opcode"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	tipb "github.com/pingcap/tipb/go-tipb"
	"github.com/zhihu/zetta/tablestore/mysql/sctx"
)

type compareFunctionClass struct {
	baseFunctionClass

	op opcode.Op
}

// getFunction sets compare built-in function signatures for various types.
func (c *compareFunctionClass) getFunction(ctx sctx.Context, rawArgs []Expression) (sig builtinFunc, err error) {
	if err = c.verifyArgs(rawArgs); err != nil {
		return nil, err
	}
	cmpType := GetAccurateCmpType(rawArgs[0], rawArgs[1])
	sig, err = c.generateCmpSigs(ctx, rawArgs, cmpType)
	//bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, rawArgs, types.ETInt, types.ETInt, types.ETInt)
	//sig = &builtinNullEQStringSig{bf}
	return sig, err
}

type builtinNullEQStringSig struct {
	baseBuiltinFunc
}

func (b *builtinNullEQStringSig) Clone() builtinFunc {
	newSig := &builtinNullEQStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinNullEQStringSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	arg0, isNull0, err := b.args[0].EvalString(b.ctx, row)
	if err != nil {
		return 0, true, err
	}
	arg1, isNull1, err := b.args[1].EvalString(b.ctx, row)
	if err != nil {
		return 0, true, err
	}
	var res int64
	switch {
	case isNull0 && isNull1:
		res = 1
	case isNull0 != isNull1:
		break
	case types.CompareString(arg0, arg1, b.collation) == 0:
		res = 1
	}
	return res, false, nil
}

// generateCmpSigs generates compare function signatures.
func (c *compareFunctionClass) generateCmpSigs(ctx sctx.Context, args []Expression, tp types.EvalType) (sig builtinFunc, err error) {
	bf, err := newBaseBuiltinFuncWithTp(ctx, c.funcName, args, types.ETInt, tp, tp)
	if err != nil {
		return nil, err
	}
	bf.tp.Flen = 1
	switch tp {
	case types.ETInt:
		switch c.op {
		case opcode.LT:
			sig = &builtinLTIntSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_LTInt)
		case opcode.LE:
			sig = &builtinLEIntSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_LEInt)
		case opcode.GT:
			sig = &builtinGTIntSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_GTInt)
		case opcode.EQ:
			sig = &builtinEQIntSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_EQInt)
		case opcode.GE:
			sig = &builtinGEIntSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_GEInt)
		case opcode.NE:
			sig = &builtinNEIntSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_NEInt)
		case opcode.NullEQ:
			sig = &builtinNullEQIntSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_NullEQInt)
		}
	case types.ETString:
		switch c.op {
		case opcode.LT:
			sig = &builtinLTStringSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_LTString)
		case opcode.LE:
			sig = &builtinLEStringSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_LEString)
		case opcode.GT:
			sig = &builtinGTStringSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_GTString)
		case opcode.GE:
			sig = &builtinGEStringSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_GEString)
		case opcode.EQ:
			sig = &builtinEQStringSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_EQString)
		case opcode.NE:
			sig = &builtinNEStringSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_NEString)
		case opcode.NullEQ:
			sig = &builtinNullEQStringSig{bf}
			sig.setPbCode(tipb.ScalarFuncSig_NullEQString)
		}
	}
	return
}

// getBaseCmpType gets the EvalType that the two args will be treated as when comparing.
func getBaseCmpType(lhs, rhs types.EvalType, lft, rft *types.FieldType) types.EvalType {
	if lft.Tp == mysql.TypeUnspecified || rft.Tp == mysql.TypeUnspecified {
		if lft.Tp == rft.Tp {
			return types.ETString
		}
		if lft.Tp == mysql.TypeUnspecified {
			lhs = rhs
		} else {
			rhs = lhs
		}
	}
	if lhs.IsStringKind() && rhs.IsStringKind() {
		return types.ETString
	} else if (lhs == types.ETInt || lft.Hybrid()) && (rhs == types.ETInt || rft.Hybrid()) {
		return types.ETInt
	} else if ((lhs == types.ETInt || lft.Hybrid()) || lhs == types.ETDecimal) &&
		((rhs == types.ETInt || rft.Hybrid()) || rhs == types.ETDecimal) {
		return types.ETDecimal
	}
	return types.ETReal
}

// GetAccurateCmpType uses a more complex logic to decide the EvalType of the two args when compare with each other than
// getBaseCmpType does.
func GetAccurateCmpType(lhs, rhs Expression) types.EvalType {
	lhsFieldType, rhsFieldType := lhs.GetType(), rhs.GetType()
	lhsEvalType, rhsEvalType := lhsFieldType.EvalType(), rhsFieldType.EvalType()
	cmpType := getBaseCmpType(lhsEvalType, rhsEvalType, lhsFieldType, rhsFieldType)
	if (lhsEvalType.IsStringKind() && rhsFieldType.Tp == mysql.TypeJSON) ||
		(lhsFieldType.Tp == mysql.TypeJSON && rhsEvalType.IsStringKind()) {
		cmpType = types.ETJson
	} else if cmpType == types.ETString && (types.IsTypeTime(lhsFieldType.Tp) || types.IsTypeTime(rhsFieldType.Tp)) {
		// date[time] <cmp> date[time]
		// string <cmp> date[time]
		// compare as time
		if lhsFieldType.Tp == rhsFieldType.Tp {
			cmpType = lhsFieldType.EvalType()
		} else {
			cmpType = types.ETDatetime
		}
	} else if lhsFieldType.Tp == mysql.TypeDuration && rhsFieldType.Tp == mysql.TypeDuration {
		// duration <cmp> duration
		// compare as duration
		cmpType = types.ETDuration
	} else if cmpType == types.ETReal || cmpType == types.ETString {
		_, isLHSConst := lhs.(*Constant)
		_, isRHSConst := rhs.(*Constant)
		if (lhsEvalType == types.ETDecimal && !isLHSConst && rhsEvalType.IsStringKind() && isRHSConst) ||
			(rhsEvalType == types.ETDecimal && !isRHSConst && lhsEvalType.IsStringKind() && isLHSConst) {
			/*
				<non-const decimal expression> <cmp> <const string expression>
				or
				<const string expression> <cmp> <non-const decimal expression>

				Do comparison as decimal rather than float, in order not to lose precision.
			)*/
			cmpType = types.ETDecimal
		}
	}
	return cmpType
}

type builtinLTIntSig struct {
	baseBuiltinFunc
}

func (b *builtinLTIntSig) Clone() builtinFunc {
	newSig := &builtinLTIntSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinLTIntSig) evalIntWithCtx(ctx sctx.Context, row chunk.Row) (val int64, isNull bool, err error) {
	return resOfLT(CompareInt(ctx, b.args[0], b.args[1], row, row))
}

func (b *builtinLTIntSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfLT(CompareInt(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinLTStringSig struct {
	baseBuiltinFunc
}

func (b *builtinLTStringSig) Clone() builtinFunc {
	newSig := &builtinLTStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinLTStringSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfLT(CompareStringWithCollationInfo(b.ctx, b.args[0], b.args[1], row, row, b.collation))
}

type builtinLEIntSig struct {
	baseBuiltinFunc
}

func (b *builtinLEIntSig) Clone() builtinFunc {
	newSig := &builtinLEIntSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinLEIntSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfLE(CompareInt(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinLEStringSig struct {
	baseBuiltinFunc
}

func (b *builtinLEStringSig) Clone() builtinFunc {
	newSig := &builtinLEStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinLEStringSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfLE(CompareStringWithCollationInfo(b.ctx, b.args[0], b.args[1], row, row, b.collation))
}

type builtinGTIntSig struct {
	baseBuiltinFunc
}

func (b *builtinGTIntSig) Clone() builtinFunc {
	newSig := &builtinGTIntSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinGTIntSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfGT(CompareInt(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinGTStringSig struct {
	baseBuiltinFunc
}

func (b *builtinGTStringSig) Clone() builtinFunc {
	newSig := &builtinGTStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinGTStringSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfGT(CompareStringWithCollationInfo(b.ctx, b.args[0], b.args[1], row, row, b.collation))
}

type builtinGEIntSig struct {
	baseBuiltinFunc
}

func (b *builtinGEIntSig) Clone() builtinFunc {
	newSig := &builtinGEIntSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinGEIntSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfGE(CompareInt(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinGEStringSig struct {
	baseBuiltinFunc
}

func (b *builtinGEStringSig) Clone() builtinFunc {
	newSig := &builtinGEStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinGEStringSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfGE(CompareStringWithCollationInfo(b.ctx, b.args[0], b.args[1], row, row, b.collation))
}

type builtinEQIntSig struct {
	baseBuiltinFunc
}

func (b *builtinEQIntSig) Clone() builtinFunc {
	newSig := &builtinEQIntSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinEQIntSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfEQ(CompareInt(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinEQStringSig struct {
	baseBuiltinFunc
}

func (b *builtinEQStringSig) Clone() builtinFunc {
	newSig := &builtinEQStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinEQStringSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfEQ(CompareStringWithCollationInfo(b.ctx, b.args[0], b.args[1], row, row, b.collation))
}

type builtinNEIntSig struct {
	baseBuiltinFunc
}

func (b *builtinNEIntSig) Clone() builtinFunc {
	newSig := &builtinNEIntSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinNEIntSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfNE(CompareInt(b.ctx, b.args[0], b.args[1], row, row))
}

type builtinNEStringSig struct {
	baseBuiltinFunc
}

func (b *builtinNEStringSig) Clone() builtinFunc {
	newSig := &builtinNEStringSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinNEStringSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	return resOfNE(CompareStringWithCollationInfo(b.ctx, b.args[0], b.args[1], row, row, b.collation))
}

type builtinNullEQIntSig struct {
	baseBuiltinFunc
}

func (b *builtinNullEQIntSig) Clone() builtinFunc {
	newSig := &builtinNullEQIntSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

func (b *builtinNullEQIntSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	arg0, isNull0, err := b.args[0].EvalInt(b.ctx, row)
	if err != nil {
		return 0, isNull0, err
	}
	arg1, isNull1, err := b.args[1].EvalInt(b.ctx, row)
	if err != nil {
		return 0, isNull1, err
	}
	isUnsigned0, isUnsigned1 := mysql.HasUnsignedFlag(b.args[0].GetType().Flag), mysql.HasUnsignedFlag(b.args[1].GetType().Flag)
	var res int64
	switch {
	case isNull0 && isNull1:
		res = 1
	case isNull0 != isNull1:
		break
	case isUnsigned0 && isUnsigned1 && types.CompareUint64(uint64(arg0), uint64(arg1)) == 0:
		res = 1
	case !isUnsigned0 && !isUnsigned1 && types.CompareInt64(arg0, arg1) == 0:
		res = 1
	case isUnsigned0 && !isUnsigned1:
		if arg1 < 0 || arg0 > math.MaxInt64 {
			break
		}
		if types.CompareInt64(arg0, arg1) == 0 {
			res = 1
		}
	case !isUnsigned0 && isUnsigned1:
		if arg0 < 0 || arg1 > math.MaxInt64 {
			break
		}
		if types.CompareInt64(arg0, arg1) == 0 {
			res = 1
		}
	}
	return res, false, nil
}

type builtinNullEQRealSig struct {
	baseBuiltinFunc
}

func (b *builtinNullEQRealSig) Clone() builtinFunc {
	newSig := &builtinNullEQRealSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

/*
func (b *builtinNullEQRealSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	arg0, isNull0, err := b.args[0].EvalReal(b.ctx, row)
	if err != nil {
		return 0, true, err
	}
	arg1, isNull1, err := b.args[1].EvalReal(b.ctx, row)
	if err != nil {
		return 0, true, err
	}
	var res int64
	switch {
	case isNull0 && isNull1:
		res = 1
	case isNull0 != isNull1:
		break
	case types.CompareFloat64(arg0, arg1) == 0:
		res = 1
	}
	return res, false, nil
}
*/

type builtinNullEQDecimalSig struct {
	baseBuiltinFunc
}

func (b *builtinNullEQDecimalSig) Clone() builtinFunc {
	newSig := &builtinNullEQDecimalSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

/*
func (b *builtinNullEQDecimalSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	arg0, isNull0, err := b.args[0].EvalDecimal(b.ctx, row)
	if err != nil {
		return 0, true, err
	}
	arg1, isNull1, err := b.args[1].EvalDecimal(b.ctx, row)
	if err != nil {
		return 0, true, err
	}
	var res int64
	switch {
	case isNull0 && isNull1:
		res = 1
	case isNull0 != isNull1:
		break
	case arg0.Compare(arg1) == 0:
		res = 1
	}
	return res, false, nil
}
*/

type builtinNullEQDurationSig struct {
	baseBuiltinFunc
}

func (b *builtinNullEQDurationSig) Clone() builtinFunc {
	newSig := &builtinNullEQDurationSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

/*
func (b *builtinNullEQDurationSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	arg0, isNull0, err := b.args[0].EvalDuration(b.ctx, row)
	if err != nil {
		return 0, true, err
	}
	arg1, isNull1, err := b.args[1].EvalDuration(b.ctx, row)
	if err != nil {
		return 0, true, err
	}
	var res int64
	switch {
	case isNull0 && isNull1:
		res = 1
	case isNull0 != isNull1:
		break
	case arg0.Compare(arg1) == 0:
		res = 1
	}
	return res, false, nil
}
*/

type builtinNullEQTimeSig struct {
	baseBuiltinFunc
}

func (b *builtinNullEQTimeSig) Clone() builtinFunc {
	newSig := &builtinNullEQTimeSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

/*
func (b *builtinNullEQTimeSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	arg0, isNull0, err := b.args[0].EvalTime(b.ctx, row)
	if err != nil {
		return 0, true, err
	}
	arg1, isNull1, err := b.args[1].EvalTime(b.ctx, row)
	if err != nil {
		return 0, true, err
	}
	var res int64
	switch {
	case isNull0 && isNull1:
		res = 1
	case isNull0 != isNull1:
		break
	case arg0.Compare(arg1) == 0:
		res = 1
	}
	return res, false, nil
}
*/

type builtinNullEQJSONSig struct {
	baseBuiltinFunc
}

func (b *builtinNullEQJSONSig) Clone() builtinFunc {
	newSig := &builtinNullEQJSONSig{}
	newSig.cloneFrom(&b.baseBuiltinFunc)
	return newSig
}

/*
func (b *builtinNullEQJSONSig) evalInt(row chunk.Row) (val int64, isNull bool, err error) {
	arg0, isNull0, err := b.args[0].EvalJSON(b.ctx, row)
	if err != nil {
		return 0, true, err
	}
	arg1, isNull1, err := b.args[1].EvalJSON(b.ctx, row)
	if err != nil {
		return 0, true, err
	}
	var res int64
	switch {
	case isNull0 && isNull1:
		res = 1
	case isNull0 != isNull1:
		break
	default:
		cmpRes := json.CompareBinary(arg0, arg1)
		if cmpRes == 0 {
			res = 1
		}
	}
	return res, false, nil
}
*/

func resOfLT(val int64, isNull bool, err error) (int64, bool, error) {
	if isNull || err != nil {
		return 0, isNull, err
	}
	if val < 0 {
		val = 1
	} else {
		val = 0
	}
	return val, false, nil
}

func resOfLE(val int64, isNull bool, err error) (int64, bool, error) {
	if isNull || err != nil {
		return 0, isNull, err
	}
	if val <= 0 {
		val = 1
	} else {
		val = 0
	}
	return val, false, nil
}

func resOfGT(val int64, isNull bool, err error) (int64, bool, error) {
	if isNull || err != nil {
		return 0, isNull, err
	}
	if val > 0 {
		val = 1
	} else {
		val = 0
	}
	return val, false, nil
}

func resOfGE(val int64, isNull bool, err error) (int64, bool, error) {
	if isNull || err != nil {
		return 0, isNull, err
	}
	if val >= 0 {
		val = 1
	} else {
		val = 0
	}
	return val, false, nil
}

func resOfEQ(val int64, isNull bool, err error) (int64, bool, error) {
	if isNull || err != nil {
		return 0, isNull, err
	}
	if val == 0 {
		val = 1
	} else {
		val = 0
	}
	return val, false, nil
}

func resOfNE(val int64, isNull bool, err error) (int64, bool, error) {
	if isNull || err != nil {
		return 0, isNull, err
	}
	if val != 0 {
		val = 1
	} else {
		val = 0
	}
	return val, false, nil
}

// compareNull compares null values based on the following rules.
// 1. NULL is considered to be equal to NULL
// 2. NULL is considered to be smaller than a non-NULL value.
// NOTE: (lhsIsNull == true) or (rhsIsNull == true) is required.
func compareNull(lhsIsNull, rhsIsNull bool) int64 {
	if lhsIsNull && rhsIsNull {
		return 0
	}
	if lhsIsNull {
		return -1
	}
	return 1
}

// CompareFunc defines the compare function prototype.
type CompareFunc = func(sctx sctx.Context, lhsArg, rhsArg Expression, lhsRow, rhsRow chunk.Row) (int64, bool, error)

// CompareInt compares two integers.
func CompareInt(sctx sctx.Context, lhsArg, rhsArg Expression, lhsRow, rhsRow chunk.Row) (int64, bool, error) {
	arg0, isNull0, err := lhsArg.EvalInt(sctx, lhsRow)
	if err != nil {
		return 0, true, err
	}

	arg1, isNull1, err := rhsArg.EvalInt(sctx, rhsRow)
	if err != nil {
		return 0, true, err
	}

	// compare null values.
	if isNull0 || isNull1 {
		return compareNull(isNull0, isNull1), true, nil
	}

	isUnsigned0, isUnsigned1 := mysql.HasUnsignedFlag(lhsArg.GetType().Flag), mysql.HasUnsignedFlag(rhsArg.GetType().Flag)
	var res int
	switch {
	case isUnsigned0 && isUnsigned1:
		res = types.CompareUint64(uint64(arg0), uint64(arg1))
	case isUnsigned0 && !isUnsigned1:
		if arg1 < 0 || uint64(arg0) > math.MaxInt64 {
			res = 1
		} else {
			res = types.CompareInt64(arg0, arg1)
		}
	case !isUnsigned0 && isUnsigned1:
		if arg0 < 0 || uint64(arg1) > math.MaxInt64 {
			res = -1
		} else {
			res = types.CompareInt64(arg0, arg1)
		}
	case !isUnsigned0 && !isUnsigned1:
		res = types.CompareInt64(arg0, arg1)
	}
	return int64(res), false, nil
}

func genCompareString(collation string) func(sctx sctx.Context, lhsArg Expression, rhsArg Expression, lhsRow chunk.Row, rhsRow chunk.Row) (int64, bool, error) {
	return func(sctx sctx.Context, lhsArg, rhsArg Expression, lhsRow, rhsRow chunk.Row) (int64, bool, error) {
		return CompareStringWithCollationInfo(sctx, lhsArg, rhsArg, lhsRow, rhsRow, collation)
	}
}

// CompareStringWithCollationInfo compares two strings with the specified collation information.
func CompareStringWithCollationInfo(sctx sctx.Context, lhsArg, rhsArg Expression, lhsRow, rhsRow chunk.Row, collation string) (int64, bool, error) {
	arg0, isNull0, err := lhsArg.EvalString(sctx, lhsRow)
	if err != nil {
		return 0, true, err
	}

	arg1, isNull1, err := rhsArg.EvalString(sctx, rhsRow)
	if err != nil {
		return 0, true, err
	}

	if isNull0 || isNull1 {
		return compareNull(isNull0, isNull1), true, nil
	}
	return int64(types.CompareString(arg0, arg1, collation)), false, nil
}

/*
// CompareReal compares two float-point values.
func CompareReal(sctx sctx.Context, lhsArg, rhsArg Expression, lhsRow, rhsRow chunk.Row) (int64, bool, error) {
	arg0, isNull0, err := lhsArg.EvalReal(sctx, lhsRow)
	if err != nil {
		return 0, true, err
	}

	arg1, isNull1, err := rhsArg.EvalReal(sctx, rhsRow)
	if err != nil {
		return 0, true, err
	}

	if isNull0 || isNull1 {
		return compareNull(isNull0, isNull1), true, nil
	}
	return int64(types.CompareFloat64(arg0, arg1)), false, nil
}

// CompareDecimal compares two decimals.
func CompareDecimal(sctx sctx.Context, lhsArg, rhsArg Expression, lhsRow, rhsRow chunk.Row) (int64, bool, error) {
	arg0, isNull0, err := lhsArg.EvalDecimal(sctx, lhsRow)
	if err != nil {
		return 0, true, err
	}

	arg1, isNull1, err := rhsArg.EvalDecimal(sctx, rhsRow)
	if err != nil {
		return 0, true, err
	}

	if isNull0 || isNull1 {
		return compareNull(isNull0, isNull1), true, nil
	}
	return int64(arg0.Compare(arg1)), false, nil
}

// CompareTime compares two datetime or timestamps.
func CompareTime(sctx sctx.Context, lhsArg, rhsArg Expression, lhsRow, rhsRow chunk.Row) (int64, bool, error) {
	arg0, isNull0, err := lhsArg.EvalTime(sctx, lhsRow)
	if err != nil {
		return 0, true, err
	}

	arg1, isNull1, err := rhsArg.EvalTime(sctx, rhsRow)
	if err != nil {
		return 0, true, err
	}

	if isNull0 || isNull1 {
		return compareNull(isNull0, isNull1), true, nil
	}
	return int64(arg0.Compare(arg1)), false, nil
}

// CompareDuration compares two durations.
func CompareDuration(sctx sctx.Context, lhsArg, rhsArg Expression, lhsRow, rhsRow chunk.Row) (int64, bool, error) {
	arg0, isNull0, err := lhsArg.EvalDuration(sctx, lhsRow)
	if err != nil {
		return 0, true, err
	}

	arg1, isNull1, err := rhsArg.EvalDuration(sctx, rhsRow)
	if err != nil {
		return 0, true, err
	}

	if isNull0 || isNull1 {
		return compareNull(isNull0, isNull1), true, nil
	}
	return int64(arg0.Compare(arg1)), false, nil
}

// CompareJSON compares two JSONs.
func CompareJSON(sctx sctx.Context, lhsArg, rhsArg Expression, lhsRow, rhsRow chunk.Row) (int64, bool, error) {
	arg0, isNull0, err := lhsArg.EvalJSON(sctx, lhsRow)
	if err != nil {
		return 0, true, err
	}

	arg1, isNull1, err := rhsArg.EvalJSON(sctx, rhsRow)
	if err != nil {
		return 0, true, err
	}

	if isNull0 || isNull1 {
		return compareNull(isNull0, isNull1), true, nil
	}
	return int64(json.CompareBinary(arg0, arg1)), false, nil
}
*/
