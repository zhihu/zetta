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
	"bytes"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/zhihu/zetta/pkg/codec"

	"time"
)

var (
	tablePrefix        = []byte{'t'}
	indexPrefix        = []byte{'i'}
	primaryKeyPrefix   = []byte{'p'}
	columnFamilyPrefix = []byte{'c'}
)

const (
	idLen     = 8
	prefixLen = idLen + 1 + idLen // TableID + prefix + ID
)

var (
	errInvalidKey         = terror.ClassXEval.New(codeInvalidKey, "invalid key")
	errInvalidRecordKey   = terror.ClassXEval.New(codeInvalidRecordKey, "invalid record key")
	errInvalidIndexKey    = terror.ClassXEval.New(codeInvalidIndexKey, "invalid index key")
	errInvalidColumnCount = terror.ClassXEval.New(codeInvalidColumnCount, "invalid column count")
)

const (
	codeInvalidRecordKey   = 4
	codeInvalidColumnCount = 5
	codeInvalidKey         = 6
	codeInvalidIndexKey    = 7
)

func EncodeKeyTablePrefix(tableID int64) []byte {
	b := make([]byte, 0, idLen)
	b = codec.EncodeInt(b, tableID)
	return b
}

func GenRecordKeyPrefix(tableID int64) kv.Key {
	b := make([]byte, 0, idLen+len(primaryKeyPrefix))
	b = codec.EncodeInt(b, tableID)
	b = append(b, primaryKeyPrefix...)
	return b
}

func GenIndexKeyPrefix(tableID int64) kv.Key {
	b := make([]byte, 0, idLen+len(indexPrefix))
	b = codec.EncodeInt(b, tableID)
	b = append(b, indexPrefix...)
	return b
}

func EncodePkPrefix(key kv.Key) kv.Key {
	return append(key, primaryKeyPrefix...)
}

// ExtractEncodedPrimaryKey extract primary key raw bytes from encoded key.
// Used for constructing index, avoid to encode primary key one more time.
func ExtractEncodedPrimaryKey(key kv.Key) []byte {
	keyLen := len(key)
	cfidx := keyLen - 9
	return key[9:cfidx]
}

func EncodePrimaryKey(sc *stmtctx.StatementContext, primaryKey []types.Datum) ([]byte, error) {
	b := make([]byte, 0)
	var err error
	b, err = codec.EncodeKey(sc, b, primaryKey...)
	if err != nil {
		return b, errors.Trace(err)
	}
	return b, nil
}

// EncodeRecordPrimaryKey Format: v(TableID),v(PrimaryKeyPrefix),m(PrimaryKey)
func EncodeRecordPrimaryKey(sc *stmtctx.StatementContext, tableID int64,
	primaryKey []types.Datum) ([]byte, error) {
	var err error
	//TODO: The preallocated length of b can be more accurate.
	b := make([]byte, 0, idLen+1+len(primaryKey)+1+idLen)
	b = codec.EncodeInt(b, tableID)
	b = append(b, primaryKeyPrefix...)
	b, err = codec.EncodeKey(sc, b, primaryKey...)
	if err != nil {
		return b, errors.Trace(err)
	}
	return b, nil
}

// EncodeRecordPrimaryKeyCFPrefix Format: v(TableID),v(PrimaryKeyPrefix),m(PrimaryKey),v
func EncodeRecordPrimaryKeyCFPrefix(sc *stmtctx.StatementContext, tableID int64,
	primaryKey []types.Datum, columnFamilyID int64) ([]byte, error) {
	var err error
	//TODO: The preallocated length of b can be more accurate.
	b := make([]byte, 0, idLen+1+len(primaryKey)+1+idLen)
	b = codec.EncodeInt(b, tableID)
	b = append(b, primaryKeyPrefix...)
	b, err = codec.EncodeKey(sc, b, primaryKey...)
	if err != nil {
		return b, errors.Trace(err)
	}
	b = append(b, columnFamilyPrefix...)
	b = codec.EncodeInt(b, columnFamilyID)
	return b, nil
}

//Only for EncodeRecordPrimaryKeyCFPrefix !!!!
func ExtractEncodedPrimaryKeyCFPrefix(key kv.Key) []byte {
	b := []byte(key)
	idx := bytes.LastIndex(b, columnFamilyPrefix)
	return b[:idx+1+8]
}

func EncodePkCF(key kv.Key, columnFamilyID int64) kv.Key {
	b := append(key, columnFamilyPrefix...)
	b = codec.EncodeInt(b, columnFamilyID)
	return b
}

func EncodePkCFColumn(key kv.Key, column []byte) kv.Key {
	b := make([]byte, len(key), len(key)+len(column))
	copy(b, []byte(key))
	b = append(b, column...)
	return b
}

func EncodePkCFWithColumn(key kv.Key, columnFamilyID int64, column []byte) kv.Key {
	b := make([]byte, len(key), len(key)+1+idLen+idLen)
	copy(b, []byte(key))
	b = append(b, columnFamilyPrefix...)
	b = codec.EncodeInt(b, columnFamilyID)
	b = append(b, column...)
	// fmt.Printf("tablecodec:%s\n", string(b))
	return b
}

func ExtractEncodedHighColumnName(key []byte, sc *stmtctx.StatementContext, tableID int64,
	primaryKey []types.Datum, columnFamilyID int64) (string, error) {
	body, err := EncodeRecordPrimaryKeyCFPrefix(sc, tableID, primaryKey, columnFamilyID)
	if err != nil {
		return "", err
	}
	b := []byte(key)
	return string(b[len(body):]), nil
}

func EncodeRecordKeyWideWithEncodedPK(tableID int64,
	encodedPK []byte, columnFamilyID int64) []byte {
	//TODO: The preallocated length of b can be more accurate.
	b := make([]byte, 0, idLen+1+len(encodedPK)+1+idLen)
	b = codec.EncodeInt(b, tableID)
	b = append(b, primaryKeyPrefix...)
	b = append(b, encodedPK...)
	b = append(b, columnFamilyPrefix...)
	b = codec.EncodeInt(b, columnFamilyID)
	return b
}

// Format: v(TableID),v(PrimaryKeyPrefix),m(PrimaryKey),v(ColumnFamilyPrefix),v(ColumnFamilyID)
func EncodeRecordKeyWide(sc *stmtctx.StatementContext, tableID int64,
	primaryKey []types.Datum, columnFamilyID int64) ([]byte, error) {
	var err error
	//TODO: The preallocated length of b can be more accurate.
	b := make([]byte, 0, idLen+1+len(primaryKey)+1+idLen)
	b = codec.EncodeInt(b, tableID)
	b = append(b, primaryKeyPrefix...)
	b, err = codec.EncodeKey(sc, b, primaryKey...)
	if err != nil {
		return b, errors.Trace(err)
	}
	b = append(b, columnFamilyPrefix...)
	b = codec.EncodeInt(b, columnFamilyID)
	return b, nil
}

// encodeReservedFlag encodes a flag of one byte for reserved use, 2^8 is enough.
func encodeReservedFlag(b []byte) []byte {
	return append([]byte{'0'}, b...)
}

// decodeReservedFlag return the byte flag and the original []byte without flag.
func decodeReservedFlag(b []byte) (byte, []byte) {
	flag := b[0]
	return flag, b[1:]
}

// Format: v(TableID),v(PrimaryKeyPrefix),m(PrimaryKey),v(ColumnFamilyPrefix),v(ColumnFamilyID),v(ColumnName)
func EncodeRecordKeyHigh(sc *stmtctx.StatementContext, tableID int64,
	primaryKey []types.Datum, columnFamilyID int64, columnName []byte) ([]byte, error) {
	var err error
	b := make([]byte, 0, 1+idLen+1+len(primaryKey)+1+idLen+idLen)
	b = codec.EncodeInt(b, tableID)
	b = append(b, primaryKeyPrefix...)
	b, err = codec.EncodeKey(sc, b, primaryKey...)
	if err != nil {
		return b, errors.Trace(err)
	}
	b = append(b, columnFamilyPrefix...)
	b = codec.EncodeInt(b, columnFamilyID)
	b = append(b, columnName...)
	return b, nil
}

// Format: key: v(TableID),v(IndexPrefix),v(IndexID),m(ColumnValue1),m(ColumnValue2)...
func EncodeIndexUnique(sc *stmtctx.StatementContext, tableID, idxID int64,
	values []types.Datum) ([]byte, error) {
	var err error
	b := make([]byte, 0, idLen+1+idLen+len(values))
	b = codec.EncodeInt(b, tableID)
	b = append(b, indexPrefix...)
	b = codec.EncodeInt(b, idxID)
	b, err = codec.EncodeKey(sc, b, values...)
	if err != nil {
		return b, errors.Trace(err)
	}
	return b, nil
}

// Format: v(TableID),v(IndexPrefix),v(IndexID),m(ColumnValue1)...m(ColumnValueN),v(PrimaryKeyPrefix),m(PrimaryKey)
func EncodeIndex(sc *stmtctx.StatementContext, tableID, idxID int64,
	values, primaryKey []types.Datum) ([]byte, error) {
	b, err := EncodeIndexUnique(sc, tableID, idxID, values)
	if err != nil {
		return b, errors.Trace(err)
	}
	b = append(b, primaryKeyPrefix...)
	b, err = codec.EncodeKey(sc, b, primaryKey...)
	if err != nil {
		return b, errors.Trace(err)
	}
	return b, nil
}

func EncodeIndexPrefix(sc *stmtctx.StatementContext, tableID, idxID int64,
	values []types.Datum) ([]byte, error) {
	b, err := EncodeIndexUnique(sc, tableID, idxID, values)
	if err != nil {
		return b, errors.Trace(err)
	}
	b = append(b, primaryKeyPrefix...)
	return b, nil
}

func DecodeColumnValue(data []byte, ft *types.FieldType, loc *time.Location) (types.Datum, []byte, error) {
	remain, d, err := codec.DecodeOne(data)
	if err != nil {
		return types.Datum{}, remain, errors.Trace(err)
	}
	colDatum, err := unflatten(d, ft, loc)
	if err != nil {
		return types.Datum{}, remain, errors.Trace(err)
	}
	return colDatum, remain, nil
}

// DecodeUniqIndexKey decodes the key and gets the tableID, indexID, column values.
func DecodeUniqIndexKey(key []byte, cols []*types.FieldType,
	loc *time.Location) (int64, int64, []types.Datum, error) {
	indexValues := make([]types.Datum, 0)
	if len(key) < prefixLen {
		return 0, 0, indexValues, errInvalidKey.GenWithStack("invalid key - %q", key)
	}
	b, tableID, err := codec.DecodeInt(key)
	if err != nil {
		return 0, 0, indexValues, errors.Trace(err)
	}
	if b[0] != indexPrefix[0] {
		return 0, 0, indexValues, errInvalidIndexKey.GenWithStack("invalid index key - %q", key)
	}
	b = b[1:]
	b, indexID, err := codec.DecodeInt(b)
	if err != nil {
		return 0, 0, indexValues, errors.Trace(err)
	}
	for _, c := range cols {
		d, remain, err := DecodeColumnValue(b, c, loc)
		if err != nil {
			return 0, 0, indexValues, errors.Trace(err)
		}
		indexValues = append(indexValues, d)
		b = remain
	}
	return tableID, indexID, indexValues, nil
}

// DecodeUniqIndexKey decodes the key and gets the tableID, indexID, column values, pk values.
func DecodeIndexKey(key []byte, cols []*types.FieldType, pks []*types.FieldType,
	loc *time.Location) (int64, int64, []types.Datum, []types.Datum, error) {
	indexValues := make([]types.Datum, 0)
	pkValues := make([]types.Datum, 0)
	if len(key) < prefixLen {
		return 0, 0, indexValues, pkValues, errInvalidKey.GenWithStack("invalid key - %q", key)
	}
	b, tableID, err := codec.DecodeInt(key)
	if err != nil {
		return 0, 0, indexValues, pkValues, errors.Trace(err)
	}
	if b[0] != indexPrefix[0] {
		return 0, 0, indexValues, pkValues, errInvalidIndexKey.GenWithStack("invalid index key - %q", key)
	}
	b = b[1:]
	b, indexID, err := codec.DecodeInt(b)
	if err != nil {
		return 0, 0, indexValues, pkValues, errors.Trace(err)
	}
	for _, c := range cols {
		d, remain, err := DecodeColumnValue(b, c, loc)
		if err != nil {
			return 0, 0, indexValues, pkValues, errors.Trace(err)
		}
		indexValues = append(indexValues, d)
		b = remain
	}
	if b[0] != primaryKeyPrefix[0] {
		return 0, 0, indexValues, pkValues, errInvalidIndexKey.GenWithStack("invalid index key - %q", key)
	}
	b = b[1:]
	for _, c := range pks {
		d, remain, err := DecodeColumnValue(b, c, loc)
		if err != nil {
			return 0, 0, indexValues, pkValues, errors.Trace(err)
		}
		pkValues = append(pkValues, d)
		b = remain
	}
	return tableID, indexID, indexValues, pkValues, nil
}

// DecodeRecordKey decodes the key and gets the tableID, primaryKey, columnFamilyID,columnID
// columnID may not exist, return -1.
func DecodeRecordKey(key []byte, cols []*types.FieldType,
	loc *time.Location) (int64, []types.Datum, int64, []byte, error) {
	primaryKey := make([]types.Datum, 0)
	if len(key) < prefixLen {
		return 0, primaryKey, 0, nil, errInvalidRecordKey.GenWithStack("invalid record key - %q", key)
	}
	b, tableID, err := codec.DecodeInt(key)
	if err != nil {
		return 0, primaryKey, 0, nil, errors.Trace(err)
	}
	if b[0] != primaryKeyPrefix[0] {
		return 0, primaryKey, 0, nil, errInvalidRecordKey.GenWithStack("invalid record key - %q", key)
	}
	b = b[1:]
	for _, c := range cols {
		d, remain, err := DecodeColumnValue(b, c, loc)
		if err != nil {
			return 0, primaryKey, 0, nil, errors.Trace(err)
		}
		primaryKey = append(primaryKey, d)
		b = remain
	}
	if len(b) < 1+idLen || b[0] != columnFamilyPrefix[0] {
		return 0, primaryKey, 0, nil, errInvalidRecordKey.GenWithStack("invalid record key - %q", key)
	}
	b = b[1:]
	b, columnFamilyID, err := codec.DecodeInt(b)
	if err != nil {
		return 0, primaryKey, 0, nil, errors.Trace(err)
	}
	if len(b) < 1 {
		return tableID, primaryKey, columnFamilyID, nil, nil
	}
	return tableID, primaryKey, columnFamilyID, b, nil
}

// UnflattenDatums converts raw datums to column datums.
func UnflattenDatums(datums []types.Datum, fts []*types.FieldType, loc *time.Location) ([]types.Datum, error) {
	for i, datum := range datums {
		ft := fts[i]
		uDatum, err := unflatten(datum, ft, loc)
		if err != nil {
			return datums, errors.Trace(err)
		}
		datums[i] = uDatum
	}
	return datums, nil
}

// unflatten converts a raw datum to a column datum.
func unflatten(datum types.Datum, ft *types.FieldType, loc *time.Location) (types.Datum, error) {
	if datum.IsNull() {
		return datum, nil
	}
	switch ft.Tp {
	case mysql.TypeFloat:
		datum.SetFloat32(float32(datum.GetFloat64()))
		return datum, nil
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeYear, mysql.TypeInt24,
		mysql.TypeLong, mysql.TypeLonglong, mysql.TypeDouble, mysql.TypeTinyBlob,
		mysql.TypeMediumBlob, mysql.TypeBlob, mysql.TypeLongBlob, mysql.TypeVarchar,
		mysql.TypeString:
		return datum, nil
	case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeTimestamp:
		t := types.NewTime(types.ZeroCoreTime, ft.Tp, int8(ft.Decimal))
		var err error
		err = t.FromPackedUint(datum.GetUint64())
		if err != nil {
			return datum, errors.Trace(err)
		}
		if ft.Tp == mysql.TypeTimestamp && !t.IsZero() {
			err = t.ConvertTimeZone(time.UTC, loc)
			if err != nil {
				return datum, errors.Trace(err)
			}
		}
		datum.SetUint64(0)
		datum.SetMysqlTime(t)
		return datum, nil
	case mysql.TypeDuration: //duration should read fsp from column meta data
		dur := types.Duration{Duration: time.Duration(datum.GetInt64()), Fsp: int8(ft.Decimal)}
		datum.SetMysqlDuration(dur)
		return datum, nil
	case mysql.TypeEnum:
		// ignore error deliberately, to read empty enum value.
		enum, err := types.ParseEnumValue(ft.Elems, datum.GetUint64())
		if err != nil {
			enum = types.Enum{}
		}
		datum.SetMysqlEnum(enum, ft.Collate)
		return datum, nil
	case mysql.TypeSet:
		set, err := types.ParseSetValue(ft.Elems, datum.GetUint64())
		if err != nil {
			return datum, errors.Trace(err)
		}
		datum.SetMysqlSet(set, ft.Collate)
		return datum, nil
	case mysql.TypeBit:
		val := datum.GetUint64()
		byteSize := (ft.Flen + 7) >> 3
		datum.SetUint64(0)
		datum.SetMysqlBit(types.NewBinaryLiteralFromUint(val, byteSize))
	}
	return datum, nil
}

// EncodeRow encode row data and column ids into a slice of byte.
// Row layout: colID1, value1, colID2, value2, .....
// valBuf and values pass by caller, for reducing EncodeRow allocates temporary bufs. If you pass valBuf and values as nil,
// EncodeRow will allocate it.
func EncodeRow(sc *stmtctx.StatementContext, row []types.Datum, colIDs []int64,
	valBuf []byte, values []types.Datum) ([]byte, error) {
	if len(row) != len(colIDs) {
		return nil, errors.Errorf("EncodeRow error: data and columnID count not match %d vs %d", len(row), len(colIDs))
	}
	valBuf = valBuf[:0]
	if values == nil {
		values = make([]types.Datum, len(row)*2)
	}
	for i, c := range row {
		id := colIDs[i]
		values[2*i].SetInt64(id)
		err := flatten(sc, c, &values[2*i+1])
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	if len(values) == 0 {
		// We could not set nil value into kv.
		return []byte{codec.NilFlag}, nil
	}
	return codec.EncodeValue(sc, valBuf, values...)
}

// EncodeRowFlexible encode row data and column name into a slice of byte.
// Row layout: colName1, value1, colName2, value2, .....
// valBuf and values pass by caller, for reducing EncodeRow allocates temporary bufs. If you pass valBuf and values as nil,
// EncodeRow will allocate it.
func EncodeRowFlexible(sc *stmtctx.StatementContext, row map[string]types.Datum,
	valBuf []byte, values []types.Datum) ([]byte, error) {
	valBuf = valBuf[:0]
	if values == nil {
		values = make([]types.Datum, len(row)*2)
	}
	i := 0
	for name, col := range row {
		values[2*i].SetBytes([]byte(name))
		values[2*i+1] = col
		i++
	}
	if len(values) == 0 {
		// We could not set nil value into kv.
		return []byte{codec.NilFlag}, nil
	}
	b, err := codec.EncodeValue(sc, valBuf, values...)
	return encodeReservedFlag(b), err
}

func DecodeRowFlexible(sc *stmtctx.StatementContext, b []byte) (map[string]types.Datum, byte, error) {
	var (
		data []byte
		err  error
		flag byte
	)
	flag, b = decodeReservedFlag(b)
	res := make(map[string]types.Datum)
	for len(data) > 0 {
		data, b, err = codec.CutOne(b)
		if err != nil {
			return nil, flag, errors.Trace(err)
		}
		_, cName, err := codec.DecodeOne(data)
		if err != nil {
			return nil, flag, errors.Trace(err)
		}
		// Get col value.
		data, b, err = codec.CutOne(b)
		if err != nil {
			return nil, flag, errors.Trace(err)
		}
		_, cValue, err := codec.DecodeOne(data)
		if err != nil {
			return nil, flag, errors.Trace(err)
		}
		res[string(cName.GetBytes())] = cValue
	}
	return res, flag, nil
}

// EncodeRowHigh encode the value in `High Layout`.
func EncodeRowHigh(sc *stmtctx.StatementContext, value types.Datum, valBuf []byte) ([]byte, error) {
	var flattenValue types.Datum
	err := flatten(sc, value, &flattenValue)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return codec.EncodeValue(sc, valBuf, flattenValue)
}

func DecodeRowHigh(sc *stmtctx.StatementContext, data []byte, ft *types.FieldType,
	loc *time.Location) (types.Datum, error) {
	_, v, err := codec.DecodeOne(data)
	if err != nil {
		return v, errors.Trace(err)
	}
	if ft != nil {
		v, err = unflatten(v, ft, loc)
		if err != nil {
			return v, errors.Trace(err)
		}
	}
	return v, nil
}

func flatten(sc *stmtctx.StatementContext, data types.Datum, ret *types.Datum) error {
	switch data.Kind() {
	case types.KindMysqlTime:
		// for mysql datetime, timestamp and date type
		t := data.GetMysqlTime()
		if t.Type() == mysql.TypeTimestamp && sc.TimeZone != time.UTC {
			err := t.ConvertTimeZone(sc.TimeZone, time.UTC)
			if err != nil {
				return errors.Trace(err)
			}
		}
		v, err := t.ToPackedUint()
		ret.SetUint64(v)
		return errors.Trace(err)
	case types.KindMysqlDuration:
		// for mysql time type
		ret.SetInt64(int64(data.GetMysqlDuration().Duration))
		return nil
	case types.KindMysqlEnum:
		ret.SetUint64(data.GetMysqlEnum().Value)
		return nil
	case types.KindMysqlSet:
		ret.SetUint64(data.GetMysqlSet().Value)
		return nil
	case types.KindBinaryLiteral, types.KindMysqlBit:
		// We don't need to handle errors here since the literal is ensured to be
		// able to store in uint64 in convertToMysqlBit.
		val, err := data.GetBinaryLiteral().ToInt(sc)
		if err != nil {
			return errors.Trace(err)
		}
		ret.SetUint64(val)
		return nil
	default:
		*ret = data
		return nil
	}
}

// DecodeRowWithMap decodes a byte slice into datums with a existing row map.
// Row layout: colID1, value1, colID2, value2, .....
func DecodeRowWide(b []byte, cols map[int64]*types.FieldType, loc *time.Location,
	row map[int64]types.Datum) (map[int64]types.Datum, error) {
	if row == nil {
		row = make(map[int64]types.Datum, len(cols))
	}
	if b == nil {
		return row, nil
	}
	if len(b) == 1 && b[0] == codec.NilFlag {
		return row, nil
	}
	cnt := 0
	var (
		data []byte
		err  error
	)
	for len(b) > 0 {
		// Get col id.
		data, b, err = codec.CutOne(b)
		if err != nil {
			return nil, errors.Trace(err)
		}
		_, cid, err := codec.DecodeOne(data)
		if err != nil {
			return nil, errors.Trace(err)
		}
		// Get col value.
		data, b, err = codec.CutOne(b)
		if err != nil {
			return nil, errors.Trace(err)
		}
		id := cid.GetInt64()
		ft, ok := cols[id]
		if ok {
			_, v, err := codec.DecodeOne(data)
			if err != nil {
				return nil, errors.Trace(err)
			}
			v, err = unflatten(v, ft, loc)
			if err != nil {
				return nil, errors.Trace(err)
			}
			row[id] = v
			cnt++
			if cnt == len(cols) {
				// Get enough data.
				break
			}
		}
	}
	return row, nil
}

// EncodeTablePrefix encodes table prefix with table ID.
func EncodeTablePrefix(tableID int64) kv.Key {
	b := EncodeKeyTablePrefix(tableID)
	return kv.Key(b)
}

// EncodeTableIndexPrefix encodes index prefix with tableID and idxID.
func EncodeTableIndexPrefix(tableID, idxID int64) kv.Key {
	key := make([]byte, 0, prefixLen)
	//key = append(key, tablePrefix...)
	key = appendTableIndexPrefix(key, tableID)
	key = codec.EncodeInt(key, idxID)
	return key
}

// appendTableIndexPrefix appends table index prefix  "t[tableID]_i".
func appendTableIndexPrefix(buf []byte, tableID int64) []byte {
	//buf = append(buf, tablePrefix...)
	buf = codec.EncodeInt(buf, tableID)
	buf = append(buf, indexPrefix...)
	return buf
}
