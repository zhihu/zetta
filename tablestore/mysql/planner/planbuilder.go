package planner

import (
	"context"
	"fmt"
	"math"
	"strings"
	"unicode"

	"github.com/pingcap/errors"

	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	parser_model "github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/opcode"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	driver "github.com/pingcap/tidb/types/parser_driver"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/zhihu/zetta/pkg/model"
	"github.com/zhihu/zetta/tablestore/infoschema"
	"github.com/zhihu/zetta/tablestore/mysql/expression"
	"github.com/zhihu/zetta/tablestore/mysql/expression/aggregation"
	"github.com/zhihu/zetta/tablestore/mysql/sctx"
	"github.com/zhihu/zetta/tablestore/table"
)

// clauseCode indicates in which clause the column is currently.
type clauseCode int

const (
	unknowClause clauseCode = iota
	fieldList
	havingClause
	onClause
	orderByClause
	whereClause
	groupByClause
	showStatement
	globalOrderByClause
)

var clauseMsg = map[clauseCode]string{
	unknowClause:        "",
	fieldList:           "field list",
	havingClause:        "having clause",
	onClause:            "on clause",
	orderByClause:       "order clause",
	whereClause:         "where clause",
	groupByClause:       "group statement",
	showStatement:       "show statement",
	globalOrderByClause: "global ORDER clause",
}

type PlanBuilder struct {
	ctx sctx.Context
	is  infoschema.InfoSchema

	// rewriterPool stores the expressionRewriter we have created to reuse it if it has been released.
	// rewriterCounter counts how many rewriter is being used.
	rewriterPool    []*expressionRewriter
	rewriterCounter int

	// colMapper stores the column that must be pre-resolved.
	colMapper map[*ast.ColumnNameExpr]int

	// When where conditions can use index, fill the idxValues below.
	// If len(idxValues) != 0, set to DataSource.
	idxValues []types.Datum

	scanSchema *expression.Schema

	outerSchemas []*expression.Schema
	outerNames   [][]*types.FieldName
}

func NewPlanBuilder(ctx sctx.Context, is infoschema.InfoSchema) *PlanBuilder {
	return &PlanBuilder{
		ctx: ctx,
		is:  is,
	}
}

func (pb *PlanBuilder) Build(ctx context.Context, node ast.Node) (Plan, error) {
	if p := TryFastPlan(pb.ctx, node); p != nil {
		return p, nil
	}
	switch x := node.(type) {
	case *ast.ExplainStmt:
		return pb.buildExplain(ctx, x)
	case *ast.ShowStmt:
		return pb.buildShow(ctx, x)
	case *ast.SelectStmt:
		return pb.buildSelect(ctx, x)
	case *ast.UseStmt, *ast.CommitStmt, *ast.RollbackStmt:
		return pb.buildSimple(x.(ast.StmtNode))
	case ast.DDLNode:
		return pb.buildDDL(ctx, x)
	case *ast.InsertStmt:
		return pb.buildInsert(ctx, x)
	case *ast.DeleteStmt:
		return pb.buildDelete(ctx, x)
	case *ast.SetStmt:
		return pb.buildSet(ctx, x)
	case *ast.SplitRegionStmt:
		return pb.buildSplitRegion(x)
	case *ast.UpdateStmt:
		return pb.buildUpdate(ctx, x)
	}
	return nil, ErrUnsupportedType.GenWithStack("Unsupported type %T", node)
}

func (b *PlanBuilder) buildSplitRegion(node *ast.SplitRegionStmt) (Plan, error) {
	return b.buildSplitIndexRegion(node)
}

func (b *PlanBuilder) buildSplitIndexRegion(node *ast.SplitRegionStmt) (Plan, error) {
	tbName := node.Table.Name.L
	dbName := node.Table.Schema.L
	if dbName == "" {
		dbName = b.ctx.GetSessionVars().CurrentDB
	}
	tb, err := b.is.GetTableByName(dbName, tbName)
	if err != nil {
		return nil, err
	}
	var indexMeta *model.IndexMeta
	tbMeta := tb.Meta()
	if node.IndexName.L != "" {
		indexMeta = tbMeta.FindIndexByName(node.IndexName.L)
	}

	p := &SplitRegion{
		IndexMeta: indexMeta,
		TableMeta: tbMeta,
	}

	if indexMeta == nil && len(node.SplitOpt.ValueLists) > 0 {
		indexValues := make([][]types.Datum, 0, len(node.SplitOpt.ValueLists))
		for i, valuesItem := range node.SplitOpt.ValueLists {
			if len(valuesItem) > len(tbMeta.PrimaryKey) {
				return nil, ErrWrongValueCountOnRow.GenWithStackByArgs(i + 1)
			}
			values, err := b.convertValue2ColumnType(valuesItem, tbMeta)
			if err != nil {
				return nil, err
			}
			indexValues = append(indexValues, values)
		}
		p.ValueLists = indexValues
		return p, nil
	}

	lowerValues, err := b.convertValue2ColumnType(node.SplitOpt.Lower, tbMeta)
	if err != nil {
		return nil, err
	}
	upperValues, err := b.convertValue2ColumnType(node.SplitOpt.Upper, tbMeta)
	if err != nil {
		return nil, err
	}
	p.Lower = lowerValues
	p.Upper = upperValues
	//TODO: Limit the max split num by config or vars.
	p.Num = int(node.SplitOpt.Num)
	return p, nil
}

//Convert expr to datums based on primary index.
func (b *PlanBuilder) convertValue2ColumnType(valueItems []ast.ExprNode, tbInfo *model.TableMeta) ([]types.Datum, error) {
	values := make([]types.Datum, 0, len(valueItems))
	primaryKeyTypes := tbInfo.GetPrimaryFieldTypes()
	if len(valueItems) != len(primaryKeyTypes) {
		fmt.Println("split region:", len(values), len(primaryKeyTypes))
		return nil, errors.New("Values length and primary keys length not equal")
	}
	for i, valueItem := range valueItems {
		d, err := b.convertValue(valueItem, primaryKeyTypes[i])
		if err != nil {
			return nil, err
		}
		values = append(values, d)
	}
	return values, nil
}

func (b *PlanBuilder) convertValue(valueItem ast.ExprNode, typ *types.FieldType) (d types.Datum, err error) {
	var expr expression.Expression
	switch x := valueItem.(type) {
	case *driver.ValueExpr:
		expr = &expression.Constant{
			Value:   x.Datum,
			RetType: &x.Type,
		}
	default:
		return d, errors.New("Only support constant value")
	}
	constant, ok := expr.(*expression.Constant)
	if !ok {
		return d, errors.New("Expect constant values")
	}
	value, err := constant.Eval(chunk.Row{})
	if err != nil {
		return d, err
	}
	d, err = value.ConvertTo(b.ctx.GetSessionVars().StmtCtx, typ)
	if err != nil {
		return d, err
	}
	return d, err
}

func (pb *PlanBuilder) buildSet(ctx context.Context, v *ast.SetStmt) (Plan, error) {
	p := &Set{}
	for _, vars := range v.Variables {
		assign := &expression.VarAssignment{
			Name:     vars.Name,
			IsGlobal: vars.IsGlobal,
			IsSystem: vars.IsSystem,
		}
		if _, ok := vars.Value.(*ast.DefaultExpr); !ok {
			if cn, ok2 := vars.Value.(*ast.ColumnNameExpr); ok2 && cn.Name.Table.L == "" {
				// Convert column name expression to string value expression.
				char, col := pb.ctx.GetSessionVars().GetCharsetInfo()
				vars.Value = ast.NewValueExpr(cn.Name.Name.O, char, col)
			}
			mockTablePlan := LogicalTableDual{}.Init(pb.ctx)
			var err error
			assign.Expr, _, err = pb.rewrite(ctx, vars.Value, mockTablePlan, nil, true)
			if err != nil {
				return nil, err
			}
		} else {
			assign.IsDefault = true
		}
		if vars.ExtendValue != nil {
			assign.ExtendValue = &expression.Constant{
				Value:   vars.ExtendValue.(*driver.ValueExpr).Datum,
				RetType: &vars.ExtendValue.(*driver.ValueExpr).Type,
			}
		}
		p.VarAssigns = append(p.VarAssigns, assign)
	}
	return p, nil
}

func (pb *PlanBuilder) buildDelete(ctx context.Context, del *ast.DeleteStmt) (Plan, error) {
	p, err := pb.buildResultSetNode(ctx, del.TableRefs.TableRefs)
	if err != nil {
		return nil, err
	}
	if del.Where != nil {
		p, err = pb.buildSelection(ctx, p, del.Where, nil)
		if err != nil {
			return nil, err
		}
	}
	if del.Limit != nil {
		p, err = pb.buildLimit(p, del.Limit)
		if err != nil {
			return nil, err
		}
	}
	tblName := p.OutputNames()[0].TblName.L
	dbName := p.OutputNames()[0].DBName.L
	tbl, err := pb.is.GetTableByName(dbName, tblName)
	if err != nil {
		return nil, err
	}
	delPlan := Delete{
		SelectionPlan: p,
		Table:         tbl,
	}.Init(pb.ctx)
	return delPlan, nil
}

//Only support bellow situations:
// pk cols with only one column family:
//1.INSERT INTO tb1 ((pkcols), cf1.q1, cf1.q2) VALUES((pkvalues), q1_value, q2_value)
// All default cols:
//2.INSERT INTO tb1 (default cols) VALUES(value1, value2...)
func (pb *PlanBuilder) validateInsert(tbMeta *model.TableMeta, insert *ast.InsertStmt) (allDefaultCF bool, err error) {
	// First make sure the length of columns and the length of values are equal.
	if len(insert.Lists) > 0 && len(insert.Columns) != len(insert.Lists[0]) {
		err = errors.New("columns and value list must have same length")
		return
	}
	//Check if insert.Columns only include all default cols.
	allDefaultCF = true
	defaultColumns := tbMeta.Columns
	for _, col := range insert.Columns {
		if col.Table.L != "" && col.Table.L != "default" {
			allDefaultCF = false
			break
		}
	}
	if allDefaultCF {
		if len(defaultColumns) != len(insert.Columns) {
			err = errors.New("provided default columns length not equal to defined")
			return
		}
		for i, col := range insert.Columns {
			if col.Name.L != defaultColumns[i].ColumnMeta.Name {
				err = errors.New("default column not defined")
				return
			}
		}
		return
	}

	//Check if insert.Columns only include pk cols and one other cf,qualifer cannot be null.
	pkCols := tbMeta.PrimaryKey
	if len(pkCols) >= len(insert.Columns) {
		err = errors.New("insert columns length should not be less than pkey columns length")
		return
	}
	for i, pkcol := range pkCols {
		if insert.Columns[i].Table.L != "" && insert.Columns[i].Table.L != "default" {
			err = errors.New("insert columns should include all pk columns")
			return
		}
		if pkcol != insert.Columns[i].Name.L {
			err = errors.New("insert column no defined or in not rigth ordinal")
			return
		}
	}

	noneDefaultCols := insert.Columns[len(pkCols):]
	validateCF := func(cf string) bool {
		for _, c := range tbMeta.ColumnFamilies {
			if c.Name == cf {
				return true
			}
		}
		return false
	}
	for _, col := range noneDefaultCols {
		if !validateCF(col.Table.L) {
			err = errors.New("invalid column family")
			return
		}
		if col.Name.L == "" {
			err = errors.New("qualifier cannot be null")
			return
		}
	}
	return
}

func (pb *PlanBuilder) buildInsert(ctx context.Context, insert *ast.InsertStmt) (Plan, error) {
	ts, ok := insert.Table.TableRefs.Left.(*ast.TableSource)
	if !ok {
		return nil, infoschema.ErrTableNotExists.GenWithStackByArgs()
	}
	tn, ok := ts.Source.(*ast.TableName)
	if !ok {
		return nil, infoschema.ErrTableNotExists.GenWithStackByArgs()
	}
	if tn.Schema.L == "" {
		tn.Schema.L = pb.ctx.GetSessionVars().CurrentDB
	}
	tbl, err := pb.is.GetTableByName(tn.Schema.L, tn.Name.L)
	if err != nil {
		return nil, err
	}

	var defaultCF bool
	//if defaultCF, err = pb.validateInsert(tbl.Meta(), insert); err != nil {
	//	return nil, err
	//}

	valueLists := make([][]expression.Expression, len(insert.Lists))
	// TODO: check the lists validity.
	for i, r := range insert.Lists {
		for _, c := range r {
			newExpr, _, err := pb.rewriteWithPreprocess(ctx, c, nil, nil, nil, true, nil)
			if err != nil {
				return nil, err
			}
			valueLists[i] = append(valueLists[i], newExpr)
		}
	}
	insertPlan := Insert{
		Table:     tbl,
		Columns:   insert.Columns,
		Lists:     valueLists,
		DefaultCF: defaultCF,
	}.Init(pb.ctx)
	return insertPlan, nil
}

func (pb *PlanBuilder) buildDDL(ctx context.Context, node ast.DDLNode) (Plan, error) {
	switch node.(type) {
	case *ast.CreateDatabaseStmt:
	}
	p := &DDL{Statement: node}
	return p, nil
}

func (b *PlanBuilder) buildExplain(ctx context.Context, explain *ast.ExplainStmt) (Plan, error) {
	if show, ok := explain.Stmt.(*ast.ShowStmt); ok {
		return b.buildShow(ctx, show)
	}
	return nil, errors.New("Unsupported explain statement")
}

func (pb *PlanBuilder) buildShow(ctx context.Context, show *ast.ShowStmt) (Plan, error) {
	p := LogicalShow{
		ShowContents: ShowContents{
			Tp:        show.Tp,
			DBName:    show.DBName,
			Table:     show.Table,
			Column:    show.Column,
			IndexName: parser_model.NewCIStr(show.IndexName.O),
		},
	}.Init(pb.ctx)
	switch show.Tp {
	case ast.ShowTables, ast.ShowTableStatus, ast.ShowColumns, ast.ShowCreateTable:
		if p.DBName == "" {
			currentDB := pb.ctx.GetSessionVars().CurrentDB
			if currentDB == "" {
				return nil, ErrNoDB
			}
			p.DBName = currentDB
		}
	}

	schema, names := pb.buildShowSchema(show)
	p.SetSchema(schema)
	p.SetOutputNames(names)

	var np LogicalPlan
	var err error
	np = p
	if show.Where != nil {
		np, err = pb.buildSelection(ctx, np, show.Where, nil)
		if err != nil {
			return nil, err
		}
	}
	if show.Pattern != nil {
		show.Pattern.Expr = &ast.ColumnNameExpr{
			Name: &ast.ColumnName{Name: p.OutputNames()[0].ColName},
		}
		np, err = pb.buildSelection(ctx, np, show.Pattern, nil)
		if err != nil {
			return nil, err
		}
	}
	return np, nil
}

func (pb *PlanBuilder) buildShowSchema(show *ast.ShowStmt) (schema *expression.Schema, outputNames []*types.FieldName) {
	var (
		names  []string
		ftypes []byte
	)
	switch show.Tp {
	case ast.ShowDatabases:
		names = []string{"Databases"}
		ftypes = []byte{mysql.TypeVarchar}
	case ast.ShowTables:
		name := "tables_in_" + pb.ctx.GetSessionVars().CurrentDB
		names = []string{name}
		ftypes = []byte{mysql.TypeVarchar}
	case ast.ShowCreateTable:
		names = []string{"Table", "Create Table"}
	case ast.ShowColumns:
		names = table.ColDescFieldNames(false)
	case ast.ShowVariables, ast.ShowStatus:
		names = []string{"Variable_name", "Value"}
	case ast.ShowCharset:
		names = []string{"Charset", "Description", "Default collation", "Maxlen"}
		ftypes = []byte{mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeLonglong}
	case ast.ShowCollation:
		names = []string{"Collation", "Charset", "Id", "Default", "Compiled", "Sortlen"}
		ftypes = []byte{mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeLonglong,
			mysql.TypeVarchar, mysql.TypeVarchar, mysql.TypeLonglong}
	}
	schema = expression.NewSchema(make([]*expression.Column, 0, len(names))...)
	outputNames = make([]*types.FieldName, 0, len(names))
	for i := range names {
		col := &expression.Column{}
		outputNames = append(outputNames, &types.FieldName{ColName: parser_model.NewCIStr(names[i])})
		tp := mysql.TypeVarchar
		if len(ftypes) != 0 {
			tp = ftypes[i]
		}
		fieldType := types.NewFieldType(tp)
		fieldType.Flen, fieldType.Decimal = mysql.GetDefaultFieldLengthAndDecimal(tp)
		fieldType.Charset, fieldType.Collate = types.DefaultCharsetForType(tp)
		col.RetType = fieldType
		col.OrigName = names[i]
		schema.Append(col)
	}
	return
}

func (pb *PlanBuilder) buildSelect(ctx context.Context, sel *ast.SelectStmt) (p LogicalPlan, err error) {
	if sel.From != nil {
		p, err = pb.buildResultSetNode(ctx, sel.From.TableRefs)
		if err != nil {
			return nil, err
		}
	} else {
		p = pb.buildTableDual()
	}
	sel.Fields.Fields, err = pb.unfoldWildStar(p, sel.Fields.Fields)
	if sel.Where != nil {
		p, err = pb.buildSelection(ctx, p, sel.Where, nil)
		if err != nil {
			return nil, err
		}
	}

	hasAgg := pb.detectSelectAgg(sel)
	if hasAgg {
		aggFuncs, _ := pb.extractAggFuncs(sel.Fields.Fields)
		p, err = pb.buildAggregation(ctx, p, aggFuncs)
		if err != nil {
			return nil, err
		}
	}

	p, err = pb.buildProjection(ctx, p, sel.Fields.Fields)

	if err != nil {
		return nil, err
	}

	if sel.OrderBy != nil {
		p, err = pb.buildSort(ctx, p, sel.OrderBy.Items)
		if err != nil {
			return nil, err
		}
	}

	if sel.Limit != nil {
		p, err = pb.buildLimit(p, sel.Limit)
		if err != nil {
			return nil, err
		}
	}
	return
}

func (b *PlanBuilder) buildAggregation(ctx context.Context, p LogicalPlan, aggFuncList []*ast.AggregateFuncExpr) (LogicalPlan, error) {
	plan4Agg := LogicalAggregation{AggFuncs: make([]*aggregation.AggFuncDesc, 0, len(aggFuncList))}.Init(b.ctx)
	schema4Agg := expression.NewSchema(make([]*expression.Column, 0, len(aggFuncList)+p.Schema().Len())...)
	names := make(types.NameSlice, 0, len(aggFuncList)+p.Schema().Len())

	for _, aggFunc := range aggFuncList {
		newArgList := make([]expression.Expression, 0, len(aggFunc.Args))
		for _, arg := range aggFunc.Args {
			newArg, np, err := b.rewrite(ctx, arg, p, nil, true)
			if err != nil {
				return nil, err
			}
			p = np
			newArgList = append(newArgList, newArg)
		}
		newFunc, err := aggregation.NewAggFuncDesc(b.ctx, aggFunc.F, newArgList, aggFunc.Distinct)
		if err != nil {
			return nil, err
		}
		plan4Agg.AggFuncs = append(plan4Agg.AggFuncs, newFunc)
		schema4Agg.Append(&expression.Column{
			UniqueID: b.ctx.GetSessionVars().AllocPlanColumnID(),
			RetType:  newFunc.RetTp,
		})
		names = append(names, types.EmptyName)
	}
	ds := p.(*DataSource)
	plan4Agg.Table = ds.table
	plan4Agg.names = names
	plan4Agg.SetChildren(p)
	plan4Agg.SetSchema(schema4Agg)
	return plan4Agg, nil
}

func (b *PlanBuilder) extractAggFuncs(fields []*ast.SelectField) ([]*ast.AggregateFuncExpr, map[*ast.AggregateFuncExpr]int) {
	extractor := &AggregateFuncExtractor{}
	for _, f := range fields {
		n, _ := f.Expr.Accept(extractor)
		f.Expr = n.(ast.ExprNode)
	}
	aggList := extractor.AggFuncs
	totalAggMapper := make(map[*ast.AggregateFuncExpr]int, len(aggList))

	for i, agg := range aggList {
		totalAggMapper[agg] = i
	}
	return aggList, totalAggMapper
}

func (pb *PlanBuilder) buildSort(ctx context.Context, p LogicalPlan, byItems []*ast.ByItem) (*LogicalSort, error) {
	sort := LogicalSort{}.Init(pb.ctx)
	exprs := make([]*ByItems, 0, len(byItems))
	for _, item := range byItems {
		it, np, err := pb.rewriteWithPreprocess(ctx, item.Expr, p, nil, nil, true, nil)
		if err != nil {
			return nil, err
		}
		p = np
		exprs = append(exprs, &ByItems{Expr: it, Desc: item.Desc})
	}
	sort.ByItems = exprs
	sort.SetChildren(p)
	return sort, nil
}

// getUintFromNode gets uint64 value from ast.Node.
// For ordinary statement, node should be uint64 constant value.
// For prepared statement, node is string. We should convert it to uint64.
func getUintFromNode(ctx sctx.Context, n ast.Node) (uVal uint64, isNull bool, isExpectedType bool) {
	var val interface{}
	switch v := n.(type) {
	case *driver.ValueExpr:
		val = v.GetValue()
	default:
		return 0, false, false
	}
	switch v := val.(type) {
	case uint64:
		return v, false, true
	case int64:
		if v >= 0 {
			return uint64(v), false, true
		}
	case string:
		sc := ctx.GetSessionVars().StmtCtx
		uVal, err := types.StrToUint(sc, v, true)
		if err != nil {
			return 0, false, false
		}
		return uVal, false, true
	}
	return 0, false, false
}

func extractLimitCountOffset(ctx sctx.Context, limit *ast.Limit) (count uint64,
	offset uint64, err error) {
	var isExpectedType bool
	if limit.Count != nil {
		count, _, isExpectedType = getUintFromNode(ctx, limit.Count)
		if !isExpectedType {
			return 0, 0, ErrWrongArguments.GenWithStackByArgs("LIMIT")
		}
	}
	if limit.Offset != nil {
		offset, _, isExpectedType = getUintFromNode(ctx, limit.Offset)
		if !isExpectedType {
			return 0, 0, ErrWrongArguments.GenWithStackByArgs("LIMIT")
		}
	}
	return count, offset, nil
}

func (pb *PlanBuilder) buildLimit(src LogicalPlan, limit *ast.Limit) (LogicalPlan, error) {
	var (
		offset, count uint64
		err           error
	)
	if count, offset, err = extractLimitCountOffset(pb.ctx, limit); err != nil {
		return nil, err
	}

	if count > math.MaxUint64-offset {
		count = math.MaxUint64 - offset
	}
	li := LogicalLimit{
		Offset: offset,
		Count:  count,
	}.Init(pb.ctx)
	li.SetChildren(src)
	li.PushLimitDown(count)
	return li, nil
}

func (b *PlanBuilder) unfoldWildStar(p LogicalPlan, selectFields []*ast.SelectField) (resultList []*ast.SelectField, err error) {
	for i, field := range selectFields {
		if field.WildCard == nil {
			resultList = append(resultList, field)
			continue
		}
		if field.WildCard.Table.L == "" && i > 0 {
			return nil, ErrInvalidWildCard
		}
		dbName := field.WildCard.Schema
		tblName := field.WildCard.Table
		//findTblNameInSchema := false
		for i, name := range p.OutputNames() {
			col := p.Schema().Columns[i]
			if col.IsHidden {
				continue
			}
			if (dbName.L == "" || dbName.L == name.DBName.L) &&
				(tblName.L == "" || tblName.L == name.TblName.L) {
				//findTblNameInSchema = true
				colName := &ast.ColumnNameExpr{
					Name: &ast.ColumnName{
						Schema: name.DBName,
						//Table:  name.TblName,
						Name: name.ColName,
					}}
				colName.SetType(col.GetType())
				field := &ast.SelectField{Expr: colName}
				field.SetText(name.ColName.O)
				resultList = append(resultList, field)
			}
		}
		// tblName means column family name here.
		if tblName.L != "" {
			colName := &ast.ColumnNameExpr{
				Name: &ast.ColumnName{
					Schema: dbName,
					Table:  tblName,
					Name:   parser_model.NewCIStr("_qualifier"),
				}}
			colName.SetType(types.NewFieldType(mysql.TypeVarchar))
			field := &ast.SelectField{Expr: colName}
			resultList = append(resultList, field)

			valueColName := &ast.ColumnNameExpr{
				Name: &ast.ColumnName{
					Schema: dbName,
					Table:  tblName,
					Name:   parser_model.NewCIStr("_value"),
				}}
			valueColName.SetType(types.NewFieldType(mysql.TypeBlob))
			valueField := &ast.SelectField{Expr: valueColName}
			resultList = append(resultList, valueField)
		}
	}
	return resultList, nil
}

func (pb *PlanBuilder) buildTableDual() *LogicalTableDual {
	return LogicalTableDual{RowCount: 1}.Init(pb.ctx)
}

func (pb *PlanBuilder) buildProjection(ctx context.Context, p LogicalPlan, fields []*ast.SelectField) (LogicalPlan, error) {
	logicalProj := LogicalProjection{
		selectFields: fields,
	}.Init(pb.ctx)
	schema := expression.NewSchema(make([]*expression.Column, 0, len(fields))...)
	newNames := make([]*types.FieldName, 0, len(fields))
	for _, field := range fields {
		newExpr, np, err := pb.rewriteWithPreprocess(ctx, field.Expr, p, nil, nil, true, nil)
		if err != nil {
			return nil, err
		}
		logicalProj.Exprs = append(logicalProj.Exprs, newExpr)
		p = np
		col, name, err := pb.buildProjectionField(ctx, p, field, newExpr)
		if err != nil {
			return nil, err
		}

		schema.Append(col)
		newNames = append(newNames, name)
	}
	logicalProj.SetSchema(schema)
	logicalProj.names = newNames
	logicalProj.SetChildren(p)
	// TODO: Add PruneColumns in optimizer.
	leftCols, _ := logicalProj.PruneColumns(schema.Columns)
	for k, col := range schema.Columns {
		if pb.scanSchema == nil {
			logicalProj.ColIndexMap[k] = k
		}
		//TODO: This is time-costly.
		for i := range leftCols {
			if leftCols[i].OrigName == col.OrigName {
				logicalProj.ColIndexMap[k] = i
			}
		}
	}
	return logicalProj, nil
}

func getInnerFromParenthesesAndUnaryPlus(expr ast.ExprNode) ast.ExprNode {
	if pexpr, ok := expr.(*ast.ParenthesesExpr); ok {
		return getInnerFromParenthesesAndUnaryPlus(pexpr.Expr)
	}
	if uexpr, ok := expr.(*ast.UnaryOperationExpr); ok && uexpr.Op == opcode.Plus {
		return getInnerFromParenthesesAndUnaryPlus(uexpr.V)
	}
	return expr
}

// buildProjectionFieldNameFromColumns builds the field name, table name and database name when field expression is a column reference.
func (b *PlanBuilder) buildProjectionFieldNameFromColumns(origField *ast.SelectField, colNameField *ast.ColumnNameExpr, name *types.FieldName) (colName, origColName, tblName, origTblName, dbName parser_model.CIStr) {
	origTblName, origColName, dbName = name.OrigTblName, name.OrigColName, name.DBName
	if origField.AsName.L == "" {
		colName = colNameField.Name.Name
	} else {
		colName = origField.AsName
	}
	if tblName.L == "" {
		tblName = name.TblName
	} else {
		tblName = colNameField.Name.Table
	}
	return
}

// buildProjectionFieldNameFromExpressions builds the field name when field expression is a normal expression.
func (b *PlanBuilder) buildProjectionFieldNameFromExpressions(ctx context.Context, field *ast.SelectField) (parser_model.CIStr, error) {
	innerExpr := getInnerFromParenthesesAndUnaryPlus(field.Expr)
	valueExpr, isValueExpr := innerExpr.(*driver.ValueExpr)

	// Non-literal: Output as inputed, except that comments need to be removed.
	if !isValueExpr {
		return parser_model.NewCIStr(parser.SpecFieldPattern.ReplaceAllStringFunc(field.Text(), parser.TrimComment)), nil
	}

	// Literal: Need special processing
	switch valueExpr.Kind() {
	case types.KindString:
		projName := valueExpr.GetString()
		projOffset := valueExpr.GetProjectionOffset()
		if projOffset >= 0 {
			projName = projName[:projOffset]
		}
		// See #3686, #3994:
		// For string literals, string content is used as column name. Non-graph initial characters are trimmed.
		fieldName := strings.TrimLeftFunc(projName, func(r rune) bool {
			return !unicode.IsOneOf(mysql.RangeGraph, r)
		})
		return parser_model.NewCIStr(fieldName), nil
	case types.KindNull:
		// See #4053, #3685
		return parser_model.NewCIStr("NULL"), nil
	case types.KindBinaryLiteral:
		// Don't rewrite BIT literal or HEX literals
		return parser_model.NewCIStr(field.Text()), nil
	case types.KindInt64:
		// See #9683
		// TRUE or FALSE can be a int64
		if mysql.HasIsBooleanFlag(valueExpr.Type.Flag) {
			if i := valueExpr.GetValue().(int64); i == 0 {
				return parser_model.NewCIStr("FALSE"), nil
			}
			return parser_model.NewCIStr("TRUE"), nil
		}
		fallthrough

	default:
		fieldName := field.Text()
		fieldName = strings.TrimLeft(fieldName, "\t\n +(")
		fieldName = strings.TrimRight(fieldName, "\t\n )")
		return parser_model.NewCIStr(fieldName), nil
	}
}

// buildProjectionField builds the field object according to SelectField in projection.
func (b *PlanBuilder) buildProjectionField(ctx context.Context, p LogicalPlan, field *ast.SelectField, expr expression.Expression) (*expression.Column, *types.FieldName, error) {
	var origTblName, tblName, colName, dbName parser_model.CIStr
	innerNode := getInnerFromParenthesesAndUnaryPlus(field.Expr)
	col, isCol := expr.(*expression.Column)
	// Correlated column won't affect the final output names. So we can put it in any of the three logic block.
	// Don't put it into the first block just for simplifying the codes.
	if colNameField, ok := innerNode.(*ast.ColumnNameExpr); ok && isCol {
		// Field is a column reference.
		idx := p.Schema().ColumnIndex(col)
		var name *types.FieldName
		// The column maybe the one from join's redundant part.
		// TODO: Fully support USING/NATURAL JOIN, refactor here.
		if idx != -1 {
			name = p.OutputNames()[idx]
		}
		colName, _, tblName, origTblName, dbName = b.buildProjectionFieldNameFromColumns(field, colNameField, name)
	} else if field.AsName.L != "" {
		// Field has alias.
		colName = field.AsName
	} else {
		// Other: field is an expression.
		var err error
		if colName, err = b.buildProjectionFieldNameFromExpressions(ctx, field); err != nil {
			return nil, nil, err
		}
	}
	name := &types.FieldName{
		TblName:     tblName,
		OrigTblName: origTblName,
		ColName:     colName,
		OrigColName: colName,
		DBName:      dbName,
	}
	if isCol {
		return col, name, nil
	}
	newCol := &expression.Column{
		UniqueID: b.ctx.GetSessionVars().AllocPlanColumnID(),
		RetType:  expr.GetType(),
		OrigName: colName.L,
	}
	return newCol, name, nil
}

func (b *PlanBuilder) buildResultSetNode(ctx context.Context, node ast.ResultSetNode) (p LogicalPlan, err error) {
	switch x := node.(type) {
	case *ast.Join:
		return b.buildJoin(ctx, x)
	case *ast.TableSource:
		switch v := x.Source.(type) {
		case *ast.SelectStmt:
			p, err = b.buildSelect(ctx, v)
		case *ast.TableName:
			p, err = b.buildDataSource(ctx, v, &x.AsName)
		default:
			err = ErrUnsupportedType.GenWithStackByArgs(v)
		}
		if err != nil {
			return nil, err
		}
		return p, nil
	case *ast.SelectStmt:
		return b.buildSelect(ctx, x)
	default:
		return nil, ErrUnsupportedType.GenWithStack("Unsupported ast.ResultSetNode(%T) for buildResultSetNode()", x)
	}
}

func (b *PlanBuilder) buildJoin(ctx context.Context, joinNode *ast.Join) (LogicalPlan, error) {
	// We will construct a "Join" node for some statements like "INSERT",
	// "DELETE", "UPDATE", "REPLACE". For this scenario "joinNode.Right" is nil
	// and we only build the left "ResultSetNode".
	// We do not handle the real join for now.
	if joinNode.Right == nil {
		return b.buildResultSetNode(ctx, joinNode.Left)
	}
	return b.buildResultSetNode(ctx, joinNode.Left)
}

// splitWhere split a where expression to a list of AND conditions.
func splitWhere(where ast.ExprNode) []ast.ExprNode {
	var conditions []ast.ExprNode
	switch x := where.(type) {
	case nil:
	case *ast.BinaryOperationExpr:
		if x.Op == opcode.LogicAnd {
			conditions = append(conditions, splitWhere(x.L)...)
			conditions = append(conditions, splitWhere(x.R)...)
		} else {
			conditions = append(conditions, x)
		}
	case *ast.ParenthesesExpr:
		conditions = append(conditions, splitWhere(x.Expr)...)
	default:
		conditions = append(conditions, where)
	}
	return conditions
}

func (b *PlanBuilder) buildSelection(ctx context.Context, p LogicalPlan, where ast.ExprNode, AggMapper map[*ast.AggregateFuncExpr]int) (LogicalPlan, error) {
	conditions := splitWhere(where)
	expressions := make([]expression.Expression, 0, len(conditions))
	selection := LogicalSelection{}.Init(b.ctx)
	for _, cond := range conditions {
		expr, np, err := b.rewrite(ctx, cond, p, AggMapper, false)
		if err != nil {
			return nil, err
		}
		p = np
		if expr == nil {
			continue
		}
		expressions = append(expressions, expr)
	}
	selection.SetChildren(p)
	leaves := selection.PushFilterDown(expressions)
	if len(leaves) == 0 {
		return p, nil
	}
	selection.Conditions = leaves
	return selection, nil
}

func (b *PlanBuilder) buildDataSource(ctx context.Context, tn *ast.TableName, asName *parser_model.CIStr) (LogicalPlan, error) {
	sessionVars := b.ctx.GetSessionVars()
	dbName := tn.Schema
	if dbName.L == "" {
		dbName = parser_model.NewCIStr(sessionVars.CurrentDB)
	}

	tbl, err := b.is.GetTableByName(dbName.L, tn.Name.L)
	if err != nil {
		return nil, err
	}

	tableInfo := tbl.Meta()

	tblName := *asName
	if tblName.L == "" {
		tblName = tn.Name
	}

	columns := tableInfo.Columns
	ds := DataSource{
		DBName:      dbName,
		TableAsName: asName,
		table:       tbl,
		tableInfo:   tableInfo,
		Columns:     columns,
		TblCols:     make([]*expression.Column, 0, len(columns)),
	}.Init(b.ctx)

	schema := expression.NewSchema(make([]*expression.Column, 0, len(columns))...)
	names := make([]*types.FieldName, 0, len(columns))
	for _, col := range ds.Columns {
		names = append(names, &types.FieldName{
			DBName:      dbName,
			TblName:     parser_model.NewCIStr(tableInfo.TableName),
			ColName:     parser_model.NewCIStr(col.ColumnMeta.Name),
			OrigTblName: parser_model.NewCIStr(tableInfo.TableName),
			OrigColName: parser_model.NewCIStr(col.ColumnMeta.Name),
		})
		newCol := &expression.Column{
			ID:       col.Id,
			RetType:  &col.FieldType,
			OrigName: col.ColumnMeta.Name,
		}

		schema.Append(newCol)
		ds.TblCols = append(ds.TblCols, newCol)
	}

	b.scanSchema = schema
	ds.SetSchema(schema)
	ds.names = names

	return ds, nil
}

func (b *PlanBuilder) buildSimple(node ast.StmtNode) (Plan, error) {
	p := &Simple{Statement: node}

	switch v := node.(type) {
	case *ast.UseStmt:
		if v.DBName == "" {
			return nil, ErrNoDB
		}
	}
	return p, nil
}

// detectSelectAgg detects an aggregate function or GROUP BY clause.
func (b *PlanBuilder) detectSelectAgg(sel *ast.SelectStmt) bool {
	if sel.GroupBy != nil {
		return true
	}
	for _, f := range sel.Fields.Fields {
		if ast.HasAggFlag(f.Expr) {
			return true
		}
	}
	if sel.Having != nil {
		if ast.HasAggFlag(sel.Having.Expr) {
			return true
		}
	}
	if sel.OrderBy != nil {
		for _, item := range sel.OrderBy.Items {
			if ast.HasAggFlag(item.Expr) {
				return true
			}
		}
	}
	return false
}

func (b *PlanBuilder) buildUpdateList(tbl table.Table, assignList []*ast.Assignment) (map[int64]types.Datum, error) {
	res := make(map[int64]types.Datum)
	colMap, _ := tbl.Meta().GetColumnMap()
	for _, assign := range assignList {
		switch x := assign.Expr.(type) {
		case *driver.ValueExpr:
			col := colMap[assign.Column.Name.L]
			if col == nil {
				return res, errors.New("cannot find column")
			}
			value, err := x.Datum.ConvertTo(&stmtctx.StatementContext{}, &col.FieldType)
			if err != nil {
				return res, err
			}
			fmt.Println("val map:", col.ID, value)
			res[col.ID] = value
		default:
			fmt.Println("assgin type:", x)
		}

	}
	return res, nil
}

func (b *PlanBuilder) buildUpdate(ctx context.Context, update *ast.UpdateStmt) (Plan, error) {
	p, err := b.buildResultSetNode(ctx, update.TableRefs.TableRefs)
	if err != nil {
		return nil, err
	}
	ds, ok := p.(*DataSource)
	if !ok {
		return nil, errors.New("first plan in buildUpdate not DataSource")
	}
	sel, err := b.buildSelection(ctx, p, update.Where, nil)
	if err != nil {
		return nil, err
	}
	tbl := ds.table
	valMap, err := b.buildUpdateList(tbl, update.List)
	if err != nil {
		return nil, err
	}

	up := LogicalUpdate{
		Table:      tbl,
		DataSource: sel,
		ColValues:  valMap,
	}.Init(b.ctx)
	return up, err
}

// resolveHavingAndOrderBy will process aggregate functions and resolve the columns that don't exist in select fields.
// If we found some columns that are not in select fields, we will append it to select fields and update the colMapper.
// When we rewrite the order by / having expression, we will find column in map at first.
func (b *PlanBuilder) resolveHavingAndOrderBy(sel *ast.SelectStmt, p LogicalPlan) (
	map[*ast.AggregateFuncExpr]int, map[*ast.AggregateFuncExpr]int, error) {
	extractor := &havingWindowAndOrderbyExprResolver{
		p:            p,
		selectFields: sel.Fields.Fields,
		aggMapper:    make(map[*ast.AggregateFuncExpr]int),
		colMapper:    b.colMapper,
		outerSchemas: b.outerSchemas,
		outerNames:   b.outerNames,
	}
	if sel.GroupBy != nil {
		extractor.gbyItems = sel.GroupBy.Items
	}
	// Extract agg funcs from having clause.
	if sel.Having != nil {
		extractor.curClause = havingClause
		n, ok := sel.Having.Expr.Accept(extractor)
		if !ok {
			return nil, nil, errors.Trace(extractor.err)
		}
		sel.Having.Expr = n.(ast.ExprNode)
	}
	havingAggMapper := extractor.aggMapper
	extractor.aggMapper = make(map[*ast.AggregateFuncExpr]int)
	extractor.orderBy = true
	extractor.inExpr = false
	// Extract agg funcs from order by clause.
	if sel.OrderBy != nil {
		extractor.curClause = orderByClause
		for _, item := range sel.OrderBy.Items {
			if ast.HasWindowFlag(item.Expr) {
				continue
			}
			n, ok := item.Expr.Accept(extractor)
			if !ok {
				return nil, nil, errors.Trace(extractor.err)
			}
			item.Expr = n.(ast.ExprNode)
		}
	}
	sel.Fields.Fields = extractor.selectFields
	return havingAggMapper, extractor.aggMapper, nil
}

// havingWindowAndOrderbyExprResolver visits Expr tree.
// It converts ColunmNameExpr to AggregateFuncExpr and collects AggregateFuncExpr.
type havingWindowAndOrderbyExprResolver struct {
	inAggFunc    bool
	inWindowFunc bool
	inWindowSpec bool
	inExpr       bool
	orderBy      bool
	err          error
	p            LogicalPlan
	selectFields []*ast.SelectField
	aggMapper    map[*ast.AggregateFuncExpr]int
	colMapper    map[*ast.ColumnNameExpr]int
	gbyItems     []*ast.ByItem
	outerSchemas []*expression.Schema
	outerNames   [][]*types.FieldName
	curClause    clauseCode
}

// Enter implements Visitor interface.
func (a *havingWindowAndOrderbyExprResolver) Enter(n ast.Node) (node ast.Node, skipChildren bool) {
	switch n.(type) {
	case *ast.AggregateFuncExpr:
		a.inAggFunc = true
	case *ast.WindowFuncExpr:
		a.inWindowFunc = true
	case *ast.WindowSpec:
		a.inWindowSpec = true
	case *driver.ParamMarkerExpr, *ast.ColumnNameExpr, *ast.ColumnName:
	case *ast.SubqueryExpr, *ast.ExistsSubqueryExpr:
		// Enter a new context, skip it.
		// For example: select sum(c) + c + exists(select c from t) from t;
		return n, true
	default:
		a.inExpr = true
	}
	return n, false
}

func (a *havingWindowAndOrderbyExprResolver) resolveFromPlan(v *ast.ColumnNameExpr, p LogicalPlan) (int, error) {
	idx, err := expression.FindFieldName(p.OutputNames(), v.Name)
	if err != nil {
		return -1, err
	}
	if idx < 0 {
		return -1, nil
	}
	col := p.Schema().Columns[idx]
	if col.IsHidden {
		return -1, ErrUnknownColumn.GenWithStackByArgs(v.Name, clauseMsg[a.curClause])
	}
	name := p.OutputNames()[idx]
	newColName := &ast.ColumnName{
		Schema: name.DBName,
		Table:  name.TblName,
		Name:   name.ColName,
	}
	for i, field := range a.selectFields {
		if c, ok := field.Expr.(*ast.ColumnNameExpr); ok && colMatch(c.Name, newColName) {
			return i, nil
		}
	}
	sf := &ast.SelectField{
		Expr:      &ast.ColumnNameExpr{Name: newColName},
		Auxiliary: true,
	}
	sf.Expr.SetType(col.GetType())
	a.selectFields = append(a.selectFields, sf)
	return len(a.selectFields) - 1, nil
}

// Leave implements Visitor interface.
func (a *havingWindowAndOrderbyExprResolver) Leave(n ast.Node) (node ast.Node, ok bool) {
	switch v := n.(type) {
	case *ast.AggregateFuncExpr:
		a.inAggFunc = false
		a.aggMapper[v] = len(a.selectFields)
		a.selectFields = append(a.selectFields, &ast.SelectField{
			Auxiliary: true,
			Expr:      v,
			AsName:    parser_model.NewCIStr(fmt.Sprintf("sel_agg_%d", len(a.selectFields))),
		})
	case *ast.WindowFuncExpr:
		a.inWindowFunc = false
		if a.curClause == havingClause {
			a.err = ErrWindowInvalidWindowFuncUse.GenWithStackByArgs(strings.ToLower(v.F))
			return node, false
		}
		if a.curClause == orderByClause {
			a.selectFields = append(a.selectFields, &ast.SelectField{
				Auxiliary: true,
				Expr:      v,
				AsName:    parser_model.NewCIStr(fmt.Sprintf("sel_window_%d", len(a.selectFields))),
			})
		}
	case *ast.WindowSpec:
		a.inWindowSpec = false
	case *ast.ColumnNameExpr:
		resolveFieldsFirst := true
		if a.inAggFunc || a.inWindowFunc || a.inWindowSpec || (a.orderBy && a.inExpr) || a.curClause == fieldList {
			resolveFieldsFirst = false
		}
		if !a.inAggFunc && !a.orderBy {
			for _, item := range a.gbyItems {
				if col, ok := item.Expr.(*ast.ColumnNameExpr); ok &&
					(colMatch(v.Name, col.Name) || colMatch(col.Name, v.Name)) {
					resolveFieldsFirst = false
					break
				}
			}
		}
		var index int
		if resolveFieldsFirst {
			index, a.err = resolveFromSelectFields(v, a.selectFields, false)
			if a.err != nil {
				return node, false
			}
			if index != -1 && a.curClause == havingClause && ast.HasWindowFlag(a.selectFields[index].Expr) {
				a.err = ErrWindowInvalidWindowFuncAliasUse.GenWithStackByArgs(v.Name.Name.O)
				return node, false
			}
			if index == -1 {
				if a.orderBy {
					index, a.err = a.resolveFromPlan(v, a.p)
				} else {
					index, a.err = resolveFromSelectFields(v, a.selectFields, true)
				}
			}
		} else {
			// We should ignore the err when resolving from schema. Because we could resolve successfully
			// when considering select fields.
			var err error
			index, err = a.resolveFromPlan(v, a.p)
			_ = err
			if index == -1 && a.curClause != fieldList {
				index, a.err = resolveFromSelectFields(v, a.selectFields, false)
				if index != -1 && a.curClause == havingClause && ast.HasWindowFlag(a.selectFields[index].Expr) {
					a.err = ErrWindowInvalidWindowFuncAliasUse.GenWithStackByArgs(v.Name.Name.O)
					return node, false
				}
			}
		}
		if a.err != nil {
			return node, false
		}
		if index == -1 {
			// If we can't find it any where, it may be a correlated columns.
			for _, names := range a.outerNames {
				idx, err1 := expression.FindFieldName(names, v.Name)
				if err1 != nil {
					a.err = err1
					return node, false
				}
				if idx >= 0 {
					return n, true
				}
			}
			a.err = ErrUnknownColumn.GenWithStackByArgs(v.Name.OrigColName(), clauseMsg[a.curClause])
			return node, false
		}
		if a.inAggFunc {
			return a.selectFields[index].Expr, true
		}
		a.colMapper[v] = index
	}
	return n, true
}

// colMatch means that if a match b, e.g. t.a can match test.t.a but test.t.a can't match t.a.
// Because column a want column from database test exactly.
func colMatch(a *ast.ColumnName, b *ast.ColumnName) bool {
	if a.Schema.L == "" || a.Schema.L == b.Schema.L {
		if a.Table.L == "" || a.Table.L == b.Table.L {
			return a.Name.L == b.Name.L
		}
	}
	return false
}

func resolveFromSelectFields(v *ast.ColumnNameExpr, fields []*ast.SelectField, ignoreAsName bool) (index int, err error) {
	var matchedExpr ast.ExprNode
	index = -1
	for i, field := range fields {
		if field.Auxiliary {
			continue
		}
		if matchField(field, v, ignoreAsName) {
			curCol, isCol := field.Expr.(*ast.ColumnNameExpr)
			if !isCol {
				return i, nil
			}
			if matchedExpr == nil {
				matchedExpr = curCol
				index = i
			} else if !colMatch(matchedExpr.(*ast.ColumnNameExpr).Name, curCol.Name) &&
				!colMatch(curCol.Name, matchedExpr.(*ast.ColumnNameExpr).Name) {
				return -1, ErrAmbiguous.GenWithStackByArgs(curCol.Name.Name.L, clauseMsg[fieldList])
			}
		}
	}
	return
}

func matchField(f *ast.SelectField, col *ast.ColumnNameExpr, ignoreAsName bool) bool {
	// if col specify a table name, resolve from table source directly.
	if col.Name.Table.L == "" {
		if f.AsName.L == "" || ignoreAsName {
			if curCol, isCol := f.Expr.(*ast.ColumnNameExpr); isCol {
				return curCol.Name.Name.L == col.Name.Name.L
			} else if _, isFunc := f.Expr.(*ast.FuncCallExpr); isFunc {
				// Fix issue 7331
				// If there are some function calls in SelectField, we check if
				// ColumnNameExpr in GroupByClause matches one of these function calls.
				// Example: select concat(k1,k2) from t group by `concat(k1,k2)`,
				// `concat(k1,k2)` matches with function call concat(k1, k2).
				return strings.ToLower(f.Text()) == col.Name.Name.L
			}
			// a expression without as name can't be matched.
			return false
		}
		return f.AsName.L == col.Name.Name.L
	}
	return false
}
