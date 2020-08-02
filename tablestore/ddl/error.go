package ddl

import (
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
)

var (
	// errWorkerClosed means we have already closed the DDL worker.
	errInvalidWorker = terror.ClassDDL.New(codeInvalidWorker, "invalid worker")
	// errNotOwner means we are not owner and can't handle DDL jobs.
	errNotOwner              = terror.ClassDDL.New(codeNotOwner, "not Owner")
	errCantDecodeIndex       = terror.ClassDDL.New(codeCantDecodeIndex, "cannot decode index value, because %s")
	errInvalidDDLJob         = terror.ClassDDL.New(codeInvalidDDLJob, "invalid DDL job")
	errCancelledDDLJob       = terror.ClassDDL.New(codeCancelledDDLJob, "cancelled DDL job")
	errInvalidJobFlag        = terror.ClassDDL.New(codeInvalidJobFlag, "invalid job flag")
	errRunMultiSchemaChanges = terror.ClassDDL.New(codeRunMultiSchemaChanges, "can't run multi schema change")
	errWaitReorgTimeout      = terror.ClassDDL.New(codeWaitReorgTimeout, "wait for reorganization timeout")
	errInvalidStoreVer       = terror.ClassDDL.New(codeInvalidStoreVer, "invalid storage current version")

	// We don't support dropping column with index covered now.
	errCantDropColWithIndex     = terror.ClassDDL.New(codeCantDropColWithIndex, "can't drop column with index")
	errUnsupportedAddColumn     = terror.ClassDDL.New(codeUnsupportedAddColumn, "unsupported add column")
	errUnsupportedModifyColumn  = terror.ClassDDL.New(codeUnsupportedModifyColumn, "unsupported modify column %s")
	errUnsupportedModifyCharset = terror.ClassDDL.New(codeUnsupportedModifyCharset, "unsupported modify %s")
	errUnsupportedPKHandle      = terror.ClassDDL.New(codeUnsupportedDropPKHandle,
		"unsupported drop integer primary key")
	errUnsupportedCharset = terror.ClassDDL.New(codeUnsupportedCharset, "unsupported charset %s collate %s")

	errUnsupportedShardRowIDBits = terror.ClassDDL.New(codeUnsupportedShardRowIDBits, "unsupported shard_row_id_bits for table with primary key as row id.")
	errBlobKeyWithoutLength      = terror.ClassDDL.New(codeBlobKeyWithoutLength, "index for BLOB/TEXT column must specify a key length")
	errIncorrectPrefixKey        = terror.ClassDDL.New(codeIncorrectPrefixKey, "Incorrect prefix key; the used key part isn't a string, the used length is longer than the key part, or the storage engine doesn't support unique prefix keys")
	errKeyColumnDoesNotExits     = terror.ClassDDL.New(codeKeyColumnDoesNotExits, mysql.MySQLErrName[mysql.ErrKeyColumnDoesNotExits])
	errUnknownTypeLength         = terror.ClassDDL.New(codeUnknownTypeLength, "Unknown length for type tp %d")
	errUnknownFractionLength     = terror.ClassDDL.New(codeUnknownFractionLength, "Unknown Length for type tp %d and fraction %d")
	errInvalidJobVersion         = terror.ClassDDL.New(codeInvalidJobVersion, "DDL job with version %d greater than current %d")
	errFileNotFound              = terror.ClassDDL.New(codeFileNotFound, "Can't find file: './%s/%s.frm'")
	errErrorOnRename             = terror.ClassDDL.New(codeErrorOnRename, "Error on rename of './%s/%s' to './%s/%s'")
	errInvalidUseOfNull          = terror.ClassDDL.New(codeInvalidUseOfNull, "Invalid use of NULL value")
	errTooManyFields             = terror.ClassDDL.New(codeTooManyFields, "Too many columns")
	errInvalidSplitRegionRanges  = terror.ClassDDL.New(codeInvalidRanges, "Failed to split region ranges")
	errReorgPanic                = terror.ClassDDL.New(codeReorgWorkerPanic, "reorg worker panic.")

	errOnlyOnRangeListPartition = terror.ClassDDL.New(codeOnlyOnRangeListPartition, mysql.MySQLErrName[mysql.ErrOnlyOnRangeListPartition])
	// errWrongKeyColumn is for table column cannot be indexed.
	errWrongKeyColumn = terror.ClassDDL.New(codeWrongKeyColumn, mysql.MySQLErrName[mysql.ErrWrongKeyColumn])
	// errUnsupportedOnGeneratedColumn is for unsupported actions on generated columns.
	errUnsupportedOnGeneratedColumn = terror.ClassDDL.New(codeUnsupportedOnGeneratedColumn, mysql.MySQLErrName[mysql.ErrUnsupportedOnGeneratedColumn])
	// errGeneratedColumnNonPrior forbids to refer generated column non prior to it.
	errGeneratedColumnNonPrior = terror.ClassDDL.New(codeGeneratedColumnNonPrior, mysql.MySQLErrName[mysql.ErrGeneratedColumnNonPrior])
	// errDependentByGeneratedColumn forbids to delete columns which are dependent by generated columns.
	errDependentByGeneratedColumn = terror.ClassDDL.New(codeDependentByGeneratedColumn, mysql.MySQLErrName[mysql.ErrDependentByGeneratedColumn])
	// errJSONUsedAsKey forbids to use JSON as key or index.
	errJSONUsedAsKey = terror.ClassDDL.New(codeJSONUsedAsKey, mysql.MySQLErrName[mysql.ErrJSONUsedAsKey])
	// errBlobCantHaveDefault forbids to give not null default value to TEXT/BLOB/JSON.
	errBlobCantHaveDefault = terror.ClassDDL.New(codeBlobCantHaveDefault, mysql.MySQLErrName[mysql.ErrBlobCantHaveDefault])
	errTooLongIndexComment = terror.ClassDDL.New(codeErrTooLongIndexComment, mysql.MySQLErrName[mysql.ErrTooLongIndexComment])
	// ErrInvalidDefaultValue returns for invalid default value for columns.
	ErrInvalidDefaultValue = terror.ClassDDL.New(codeInvalidDefaultValue, mysql.MySQLErrName[mysql.ErrInvalidDefault])
	// ErrGeneratedColumnRefAutoInc forbids to refer generated columns to auto-increment columns .
	ErrGeneratedColumnRefAutoInc = terror.ClassDDL.New(codeErrGeneratedColumnRefAutoInc, mysql.MySQLErrName[mysql.ErrGeneratedColumnRefAutoInc])
	// ErrUnsupportedAddPartition returns for does not support add partitions.
	ErrUnsupportedAddPartition = terror.ClassDDL.New(codeUnsupportedAddPartition, "unsupported add partitions")
	// ErrUnsupportedCoalescePartition returns for does not support coalesce partitions.
	ErrUnsupportedCoalescePartition = terror.ClassDDL.New(codeUnsupportedCoalescePartition, "unsupported coalesce partitions")
	// ErrGeneratedColumnFunctionIsNotAllowed returns for unsupported functions for generated columns.
	ErrGeneratedColumnFunctionIsNotAllowed = terror.ClassDDL.New(codeErrGeneratedColumnFunctionIsNotAllowed, "Expression of generated column '%s' contains a disallowed function.")
	// ErrUnsupportedPartitionByRangeColumns returns for does unsupported partition by range columns.
	ErrUnsupportedPartitionByRangeColumns = terror.ClassDDL.New(codeUnsupportedPartitionByRangeColumns,
		"unsupported partition by range columns")
	errUnsupportedCreatePartition = terror.ClassDDL.New(codeUnsupportedCreatePartition, "unsupported partition type, treat as normal table")

	// ErrDupKeyName returns for duplicated key name
	ErrDupKeyName = terror.ClassDDL.New(codeDupKeyName, "duplicate key name")
	// ErrInvalidDBState returns for invalid database state.
	ErrInvalidDBState = terror.ClassDDL.New(codeInvalidDBState, "invalid database state")
	// ErrInvalidTableState returns for invalid Table state.
	ErrInvalidTableState = terror.ClassDDL.New(codeInvalidTableState, "invalid table state")
	// ErrInvalidColumnState returns for invalid column state.
	ErrInvalidColumnState = terror.ClassDDL.New(codeInvalidColumnState, "invalid column state")
	// ErrInvalidIndexState returns for invalid index state.
	ErrInvalidIndexState = terror.ClassDDL.New(codeInvalidIndexState, "invalid index state")
	// ErrInvalidForeignKeyState returns for invalid foreign key state.
	ErrInvalidForeignKeyState = terror.ClassDDL.New(codeInvalidForeignKeyState, "invalid foreign key state")
	// ErrUnsupportedModifyPrimaryKey returns an error when add or drop the primary key.
	// It's exported for testing.
	ErrUnsupportedModifyPrimaryKey = terror.ClassDDL.New(codeUnsupportedModifyPrimaryKey, "unsupported %s primary key")

	// ErrColumnBadNull returns for a bad null value.
	ErrColumnBadNull = terror.ClassDDL.New(codeBadNull, "column cann't be null")
	// ErrBadField forbids to refer to unknown column.
	ErrBadField = terror.ClassDDL.New(codeBadField, "Unknown column '%s' in '%s'")
	// ErrCantRemoveAllFields returns for deleting all columns.
	ErrCantRemoveAllFields = terror.ClassDDL.New(codeCantRemoveAllFields, "can't delete all columns with ALTER TABLE")
	// ErrCantDropFieldOrKey returns for dropping a non-existent field or key.
	ErrCantDropFieldOrKey = terror.ClassDDL.New(codeCantDropFieldOrKey, "can't drop field; check that column/key exists")
	// ErrInvalidOnUpdate returns for invalid ON UPDATE clause.
	ErrInvalidOnUpdate = terror.ClassDDL.New(codeInvalidOnUpdate, mysql.MySQLErrName[mysql.ErrInvalidOnUpdate])
	// ErrTooLongIdent returns for too long name of database/table/column/index.
	ErrTooLongIdent = terror.ClassDDL.New(codeTooLongIdent, mysql.MySQLErrName[mysql.ErrTooLongIdent])
	// ErrWrongDBName returns for wrong database name.
	ErrWrongDBName = terror.ClassDDL.New(codeWrongDBName, mysql.MySQLErrName[mysql.ErrWrongDBName])
	// ErrWrongTableName returns for wrong table name.
	ErrWrongTableName = terror.ClassDDL.New(codeWrongTableName, mysql.MySQLErrName[mysql.ErrWrongTableName])
	// ErrWrongColumnName returns for wrong column name.
	ErrWrongColumnName = terror.ClassDDL.New(codeWrongColumnName, mysql.MySQLErrName[mysql.ErrWrongColumnName])
	// ErrTableMustHaveColumns returns for missing column when creating a table.
	ErrTableMustHaveColumns = terror.ClassDDL.New(codeTableMustHaveColumns, mysql.MySQLErrName[mysql.ErrTableMustHaveColumns])
	// ErrWrongNameForIndex returns for wrong index name.
	ErrWrongNameForIndex = terror.ClassDDL.New(codeWrongNameForIndex, mysql.MySQLErrName[mysql.ErrWrongNameForIndex])
	// ErrUnknownCharacterSet returns unknown character set.
	ErrUnknownCharacterSet = terror.ClassDDL.New(codeUnknownCharacterSet, "Unknown character set: '%s'")
	// ErrUnknownCollation returns unknown collation.
	ErrUnknownCollation = terror.ClassDDL.New(codeUnknownCollation, "Unknown collation: '%s'")
	// ErrCollationCharsetMismatch returns when collation not match the charset.
	ErrCollationCharsetMismatch = terror.ClassDDL.New(codeCollationCharsetMismatch, mysql.MySQLErrName[mysql.ErrCollationCharsetMismatch])
	// ErrConflictingDeclarations return conflict declarations.
	ErrConflictingDeclarations = terror.ClassDDL.New(codeConflictingDeclarations, "Conflicting declarations: 'CHARACTER SET %s' and 'CHARACTER SET %s'")
	// ErrPrimaryCantHaveNull returns All parts of a PRIMARY KEY must be NOT NULL; if you need NULL in a key, use UNIQUE instead
	ErrPrimaryCantHaveNull = terror.ClassDDL.New(codePrimaryCantHaveNull, mysql.MySQLErrName[mysql.ErrPrimaryCantHaveNull])

	// ErrNotAllowedTypeInPartition returns not allowed type error when creating table partiton with unsupport expression type.
	ErrNotAllowedTypeInPartition = terror.ClassDDL.New(codeErrFieldTypeNotAllowedAsPartitionField, mysql.MySQLErrName[mysql.ErrFieldTypeNotAllowedAsPartitionField])
	// ErrPartitionMgmtOnNonpartitioned returns it's not a partition table.
	ErrPartitionMgmtOnNonpartitioned = terror.ClassDDL.New(codePartitionMgmtOnNonpartitioned, "Partition management on a not partitioned table is not possible")
	// ErrDropPartitionNonExistent returns error in list of partition.
	ErrDropPartitionNonExistent = terror.ClassDDL.New(codeDropPartitionNonExistent, " Error in list of partitions to %s")
	// ErrSameNamePartition returns duplicate partition name.
	ErrSameNamePartition = terror.ClassDDL.New(codeSameNamePartition, "Duplicate partition name %s")
	// ErrRangeNotIncreasing returns values less than value must be strictly increasing for each partition.
	ErrRangeNotIncreasing = terror.ClassDDL.New(codeRangeNotIncreasing, "VALUES LESS THAN value must be strictly increasing for each partition")
	// ErrPartitionMaxvalue returns maxvalue can only be used in last partition definition.
	ErrPartitionMaxvalue = terror.ClassDDL.New(codePartitionMaxvalue, "MAXVALUE can only be used in last partition definition")
	//ErrDropLastPartition returns cannot remove all partitions, use drop table instead.
	ErrDropLastPartition = terror.ClassDDL.New(codeDropLastPartition, mysql.MySQLErrName[mysql.ErrDropLastPartition])
	//ErrTooManyPartitions returns too many partitions were defined.
	ErrTooManyPartitions = terror.ClassDDL.New(codeTooManyPartitions, mysql.MySQLErrName[mysql.ErrTooManyPartitions])
	//ErrPartitionFunctionIsNotAllowed returns this partition function is not allowed.
	ErrPartitionFunctionIsNotAllowed = terror.ClassDDL.New(codePartitionFunctionIsNotAllowed, mysql.MySQLErrName[mysql.ErrPartitionFunctionIsNotAllowed])
	// ErrPartitionFuncNotAllowed returns partition function returns the wrong type.
	ErrPartitionFuncNotAllowed = terror.ClassDDL.New(codeErrPartitionFuncNotAllowed, mysql.MySQLErrName[mysql.ErrPartitionFuncNotAllowed])
	// ErrUniqueKeyNeedAllFieldsInPf returns must include all columns in the table's partitioning function.
	ErrUniqueKeyNeedAllFieldsInPf = terror.ClassDDL.New(codeUniqueKeyNeedAllFieldsInPf, mysql.MySQLErrName[mysql.ErrUniqueKeyNeedAllFieldsInPf])
	errWrongExprInPartitionFunc   = terror.ClassDDL.New(codeWrongExprInPartitionFunc, mysql.MySQLErrName[mysql.ErrWrongExprInPartitionFunc])
	// ErrWarnDataTruncated returns data truncated error.
	ErrWarnDataTruncated = terror.ClassDDL.New(codeWarnDataTruncated, mysql.MySQLErrName[mysql.WarnDataTruncated])
	// ErrCoalesceOnlyOnHashPartition returns coalesce partition can only be used on hash/key partitions.
	ErrCoalesceOnlyOnHashPartition = terror.ClassDDL.New(codeCoalesceOnlyOnHashPartition, mysql.MySQLErrName[mysql.ErrCoalesceOnlyOnHashPartition])
	// ErrViewWrongList returns create view must include all columns in the select clause
	ErrViewWrongList = terror.ClassDDL.New(codeViewWrongList, mysql.MySQLErrName[mysql.ErrViewWrongList])
	// ErrAlterOperationNotSupported returns when alter operations is not supported.
	ErrAlterOperationNotSupported = terror.ClassDDL.New(codeNotSupportedAlterOperation, mysql.MySQLErrName[mysql.ErrAlterOperationNotSupportedReason])
	// ErrWrongObject returns for wrong object.
	ErrWrongObject = terror.ClassDDL.New(codeErrWrongObject, mysql.MySQLErrName[mysql.ErrWrongObject])
	// ErrTableCantHandleFt returns FULLTEXT keys are not supported by table type
	ErrTableCantHandleFt = terror.ClassDDL.New(codeErrTableCantHandleFt, mysql.MySQLErrName[mysql.ErrTableCantHandleFt])
	// ErrFieldNotFoundPart returns an error when 'partition by columns' are not found in table columns.
	ErrFieldNotFoundPart = terror.ClassDDL.New(codeFieldNotFoundPart, mysql.MySQLErrName[mysql.ErrFieldNotFoundPart])
	// ErrWrongTypeColumnValue returns 'Partition column values of incorrect type'
	ErrWrongTypeColumnValue = terror.ClassDDL.New(codeWrongTypeColumnValue, mysql.MySQLErrName[mysql.ErrWrongTypeColumnValue])
)

// DDL error codes.
const (
	codeInvalidWorker         terror.ErrCode = 1
	codeNotOwner                             = 2
	codeInvalidDDLJob                        = 3
	codeInvalidJobFlag                       = 5
	codeRunMultiSchemaChanges                = 6
	codeWaitReorgTimeout                     = 7
	codeInvalidStoreVer                      = 8
	codeUnknownTypeLength                    = 9
	codeUnknownFractionLength                = 10
	codeInvalidJobVersion                    = 11
	codeCancelledDDLJob                      = 12
	codeInvalidRanges                        = 13
	codeReorgWorkerPanic                     = 14
	codeCantDecodeIndex                      = 15

	codeInvalidDBState         = 100
	codeInvalidTableState      = 101
	codeInvalidColumnState     = 102
	codeInvalidIndexState      = 103
	codeInvalidForeignKeyState = 104

	codeCantDropColWithIndex               = 201
	codeUnsupportedAddColumn               = 202
	codeUnsupportedModifyColumn            = 203
	codeUnsupportedDropPKHandle            = 204
	codeUnsupportedCharset                 = 205
	codeUnsupportedModifyPrimaryKey        = 206
	codeUnsupportedShardRowIDBits          = 207
	codeUnsupportedAddPartition            = 208
	codeUnsupportedCoalescePartition       = 209
	codeUnsupportedModifyCharset           = 210
	codeUnsupportedPartitionByRangeColumns = 211
	codeUnsupportedCreatePartition         = 212

	codeFileNotFound                           = 1017
	codeErrorOnRename                          = 1025
	codeBadNull                                = mysql.ErrBadNull
	codeBadField                               = 1054
	codeTooLongIdent                           = 1059
	codeDupKeyName                             = 1061
	codeInvalidDefaultValue                    = mysql.ErrInvalidDefault
	codeTooLongKey                             = 1071
	codeKeyColumnDoesNotExits                  = mysql.ErrKeyColumnDoesNotExits
	codeIncorrectPrefixKey                     = 1089
	codeCantRemoveAllFields                    = 1090
	codeCantDropFieldOrKey                     = 1091
	codeBlobCantHaveDefault                    = 1101
	codeWrongDBName                            = 1102
	codeWrongTableName                         = 1103
	codeTooManyFields                          = 1117
	codeInvalidUseOfNull                       = 1138
	codeWrongColumnName                        = 1166
	codeWrongKeyColumn                         = 1167
	codeBlobKeyWithoutLength                   = 1170
	codeInvalidOnUpdate                        = 1294
	codeErrWrongObject                         = terror.ErrCode(mysql.ErrWrongObject)
	codeViewWrongList                          = 1353
	codeUnsupportedOnGeneratedColumn           = 3106
	codeGeneratedColumnNonPrior                = 3107
	codeDependentByGeneratedColumn             = 3108
	codeJSONUsedAsKey                          = 3152
	codeWrongNameForIndex                      = terror.ErrCode(mysql.ErrWrongNameForIndex)
	codeErrTooLongIndexComment                 = terror.ErrCode(mysql.ErrTooLongIndexComment)
	codeUnknownCharacterSet                    = terror.ErrCode(mysql.ErrUnknownCharacterSet)
	codeUnknownCollation                       = terror.ErrCode(mysql.ErrUnknownCollation)
	codeCollationCharsetMismatch               = terror.ErrCode(mysql.ErrCollationCharsetMismatch)
	codeConflictingDeclarations                = terror.ErrCode(mysql.ErrConflictingDeclarations)
	codeCantCreateTable                        = terror.ErrCode(mysql.ErrCantCreateTable)
	codeTableMustHaveColumns                   = terror.ErrCode(mysql.ErrTableMustHaveColumns)
	codePartitionsMustBeDefined                = terror.ErrCode(mysql.ErrPartitionsMustBeDefined)
	codePartitionMgmtOnNonpartitioned          = terror.ErrCode(mysql.ErrPartitionMgmtOnNonpartitioned)
	codeDropPartitionNonExistent               = terror.ErrCode(mysql.ErrDropPartitionNonExistent)
	codeSameNamePartition                      = terror.ErrCode(mysql.ErrSameNamePartition)
	codeRangeNotIncreasing                     = terror.ErrCode(mysql.ErrRangeNotIncreasing)
	codePartitionMaxvalue                      = terror.ErrCode(mysql.ErrPartitionMaxvalue)
	codeErrTooManyValues                       = terror.ErrCode(mysql.ErrTooManyValues)
	codeDropLastPartition                      = terror.ErrCode(mysql.ErrDropLastPartition)
	codeTooManyPartitions                      = terror.ErrCode(mysql.ErrTooManyPartitions)
	codeNoParts                                = terror.ErrCode(mysql.ErrNoParts)
	codePartitionFunctionIsNotAllowed          = terror.ErrCode(mysql.ErrPartitionFunctionIsNotAllowed)
	codeErrPartitionFuncNotAllowed             = terror.ErrCode(mysql.ErrPartitionFuncNotAllowed)
	codeErrFieldTypeNotAllowedAsPartitionField = terror.ErrCode(mysql.ErrFieldTypeNotAllowedAsPartitionField)
	codeUniqueKeyNeedAllFieldsInPf             = terror.ErrCode(mysql.ErrUniqueKeyNeedAllFieldsInPf)
	codePrimaryCantHaveNull                    = terror.ErrCode(mysql.ErrPrimaryCantHaveNull)
	codeWrongExprInPartitionFunc               = terror.ErrCode(mysql.ErrWrongExprInPartitionFunc)
	codeWarnDataTruncated                      = terror.ErrCode(mysql.WarnDataTruncated)
	codeErrTableCantHandleFt                   = terror.ErrCode(mysql.ErrTableCantHandleFt)
	codeCoalesceOnlyOnHashPartition            = terror.ErrCode(mysql.ErrCoalesceOnlyOnHashPartition)
	codeUnknownPartition                       = terror.ErrCode(mysql.ErrUnknownPartition)
	codeErrGeneratedColumnFunctionIsNotAllowed = terror.ErrCode(mysql.ErrGeneratedColumnFunctionIsNotAllowed)
	codeErrGeneratedColumnRefAutoInc           = terror.ErrCode(mysql.ErrGeneratedColumnRefAutoInc)
	codeNotSupportedAlterOperation             = terror.ErrCode(mysql.ErrAlterOperationNotSupportedReason)
	codeFieldNotFoundPart                      = terror.ErrCode(mysql.ErrFieldNotFoundPart)
	codePartitionColumnList                    = terror.ErrCode(mysql.ErrPartitionColumnList)
	codeOnlyOnRangeListPartition               = terror.ErrCode(mysql.ErrOnlyOnRangeListPartition)
	codePartitionRequiresValues                = terror.ErrCode(mysql.ErrPartitionRequiresValues)
	codePartitionWrongNoPart                   = terror.ErrCode(mysql.ErrPartitionWrongNoPart)
	codePartitionWrongNoSubpart                = terror.ErrCode(mysql.ErrPartitionWrongNoSubpart)
	codePartitionWrongValues                   = terror.ErrCode(mysql.ErrPartitionWrongValues)
	codeRowSinglePartitionField                = terror.ErrCode(mysql.ErrRowSinglePartitionField)
	codeSubpartition                           = terror.ErrCode(mysql.ErrSubpartition)
	codeSystemVersioningWrongPartitions        = terror.ErrCode(mysql.ErrSystemVersioningWrongPartitions)
	codeWrongPartitionTypeExpectedSystemTime   = terror.ErrCode(mysql.ErrWrongPartitionTypeExpectedSystemTime)
	codeWrongTypeColumnValue                   = terror.ErrCode(mysql.ErrWrongTypeColumnValue)
)

func init() {
	ddlMySQLErrCodes := map[terror.ErrCode]uint16{
		codeBadNull:                                mysql.ErrBadNull,
		codeCantRemoveAllFields:                    mysql.ErrCantRemoveAllFields,
		codeCantDropFieldOrKey:                     mysql.ErrCantDropFieldOrKey,
		codeInvalidOnUpdate:                        mysql.ErrInvalidOnUpdate,
		codeBlobKeyWithoutLength:                   mysql.ErrBlobKeyWithoutLength,
		codeIncorrectPrefixKey:                     mysql.ErrWrongSubKey,
		codeTooLongIdent:                           mysql.ErrTooLongIdent,
		codeTooLongKey:                             mysql.ErrTooLongKey,
		codeKeyColumnDoesNotExits:                  mysql.ErrKeyColumnDoesNotExits,
		codeDupKeyName:                             mysql.ErrDupKeyName,
		codeWrongDBName:                            mysql.ErrWrongDBName,
		codeWrongTableName:                         mysql.ErrWrongTableName,
		codeFileNotFound:                           mysql.ErrFileNotFound,
		codeErrorOnRename:                          mysql.ErrErrorOnRename,
		codeBadField:                               mysql.ErrBadField,
		codeInvalidUseOfNull:                       mysql.ErrInvalidUseOfNull,
		codeUnsupportedOnGeneratedColumn:           mysql.ErrUnsupportedOnGeneratedColumn,
		codeGeneratedColumnNonPrior:                mysql.ErrGeneratedColumnNonPrior,
		codeDependentByGeneratedColumn:             mysql.ErrDependentByGeneratedColumn,
		codeJSONUsedAsKey:                          mysql.ErrJSONUsedAsKey,
		codeBlobCantHaveDefault:                    mysql.ErrBlobCantHaveDefault,
		codeWrongColumnName:                        mysql.ErrWrongColumnName,
		codeWrongKeyColumn:                         mysql.ErrWrongKeyColumn,
		codeWrongNameForIndex:                      mysql.ErrWrongNameForIndex,
		codeTableMustHaveColumns:                   mysql.ErrTableMustHaveColumns,
		codeTooManyFields:                          mysql.ErrTooManyFields,
		codeErrTooLongIndexComment:                 mysql.ErrTooLongIndexComment,
		codeViewWrongList:                          mysql.ErrViewWrongList,
		codeUnknownCharacterSet:                    mysql.ErrUnknownCharacterSet,
		codeUnknownCollation:                       mysql.ErrUnknownCollation,
		codeCollationCharsetMismatch:               mysql.ErrCollationCharsetMismatch,
		codeConflictingDeclarations:                mysql.ErrConflictingDeclarations,
		codePartitionsMustBeDefined:                mysql.ErrPartitionsMustBeDefined,
		codePartitionMgmtOnNonpartitioned:          mysql.ErrPartitionMgmtOnNonpartitioned,
		codeDropPartitionNonExistent:               mysql.ErrDropPartitionNonExistent,
		codeSameNamePartition:                      mysql.ErrSameNamePartition,
		codeRangeNotIncreasing:                     mysql.ErrRangeNotIncreasing,
		codePartitionMaxvalue:                      mysql.ErrPartitionMaxvalue,
		codeErrTooManyValues:                       mysql.ErrTooManyValues,
		codeDropLastPartition:                      mysql.ErrDropLastPartition,
		codeTooManyPartitions:                      mysql.ErrTooManyPartitions,
		codeNoParts:                                mysql.ErrNoParts,
		codePartitionFunctionIsNotAllowed:          mysql.ErrPartitionFunctionIsNotAllowed,
		codeErrPartitionFuncNotAllowed:             mysql.ErrPartitionFuncNotAllowed,
		codeErrFieldTypeNotAllowedAsPartitionField: mysql.ErrFieldTypeNotAllowedAsPartitionField,
		codeUniqueKeyNeedAllFieldsInPf:             mysql.ErrUniqueKeyNeedAllFieldsInPf,
		codePrimaryCantHaveNull:                    mysql.ErrPrimaryCantHaveNull,
		codeWrongExprInPartitionFunc:               mysql.ErrWrongExprInPartitionFunc,
		codeWarnDataTruncated:                      mysql.WarnDataTruncated,
		codeErrTableCantHandleFt:                   mysql.ErrTableCantHandleFt,
		codeCoalesceOnlyOnHashPartition:            mysql.ErrCoalesceOnlyOnHashPartition,
		codeUnknownPartition:                       mysql.ErrUnknownPartition,
		codeNotSupportedAlterOperation:             mysql.ErrAlterOperationNotSupportedReason,
		codeErrWrongObject:                         mysql.ErrWrongObject,
		codeFieldNotFoundPart:                      mysql.ErrFieldNotFoundPart,
		codePartitionColumnList:                    mysql.ErrPartitionColumnList,
		codeInvalidDefaultValue:                    mysql.ErrInvalidDefault,
		codeErrGeneratedColumnRefAutoInc:           mysql.ErrGeneratedColumnRefAutoInc,
		codeOnlyOnRangeListPartition:               mysql.ErrOnlyOnRangeListPartition,
		codePartitionRequiresValues:                mysql.ErrPartitionRequiresValues,
		codePartitionWrongNoPart:                   mysql.ErrPartitionWrongNoPart,
		codePartitionWrongNoSubpart:                mysql.ErrPartitionWrongNoSubpart,
		codePartitionWrongValues:                   mysql.ErrPartitionWrongValues,
		codeRowSinglePartitionField:                mysql.ErrRowSinglePartitionField,
		codeSubpartition:                           mysql.ErrSubpartition,
		codeSystemVersioningWrongPartitions:        mysql.ErrSystemVersioningWrongPartitions,
		codeWrongPartitionTypeExpectedSystemTime:   mysql.ErrWrongPartitionTypeExpectedSystemTime,
		codeWrongTypeColumnValue:                   mysql.ErrWrongTypeColumnValue,
	}
	terror.ErrClassToMySQLCodes[terror.ClassDDL] = ddlMySQLErrCodes
}
