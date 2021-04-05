package executor

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/pd/v4/server/schedule/placement"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/util/mock"
	"github.com/zhihu/zetta/pkg/model"
	"github.com/zhihu/zetta/tablestore/mysql/expression"
)

/*
Transfer MySQL data model to TableStore data model.
*/

func toCreateDatabase(s *ast.CreateDatabaseStmt) *model.DatabaseMeta {
	dbMeta := &model.DatabaseMeta{}
	dbMeta.Database = s.Name
	return dbMeta
}

func toAddCol(dbName, tblName string, s *ast.ColumnDef) *model.ColumnMeta {
	cm := model.NewColumnMetaFromColumnDef(s)
	if s.Options != nil {
		for _, v := range s.Options {
			switch v.Tp {
			case ast.ColumnOptionNotNull:
				cm.Flag |= mysql.NotNullFlag
			case ast.ColumnOptionNull:
				cm.Flag &= ^mysql.NotNullFlag
			case ast.ColumnOptionDefaultValue:
				v, _ := expression.EvalAstExpr(mock.NewContext(), v.Expr)
				cm.DefaultValue, _ = v.ToString()
			}
		}
	}
	return cm
}

//func parseLabelsFromComment(comment string)

func toCreateTable(s *ast.CreateTableStmt) *model.TableMeta {
	tbMeta := &model.TableMeta{}
	tbMeta.TableName = s.Table.Name.L
	tbMeta.Columns = make([]*model.ColumnMeta, len(s.Cols))
	for i, col := range s.Cols {
		tbMeta.Columns[i] = model.NewColumnMetaFromColumnDef(col)
		tbMeta.Columns[i].ColumnType = model.FieldTypeToProtoType(col.Tp)
	}
	tbMeta.Indices = make([]*model.IndexMeta, 0)
	for _, cons := range s.Constraints {
		if cons.Tp == ast.ConstraintPrimaryKey {
			tbMeta.PrimaryKey = model.GetPrimaryKeysFromConstraints(cons)
			continue
		}
		tbMeta.Indices = append(tbMeta.Indices, model.NewIndexMetaFromConstraits(cons))
	}
	for i, k := range tbMeta.PrimaryKey {
		tbMeta.PrimaryKey[i] = strings.ToLower(k)
	}

	tbInfo, _ := ddl.BuildTableInfoFromAST(s)
	tbMeta.TableInfo = *tbInfo
	for i, col := range tbInfo.Columns {
		tbMeta.Columns[i].ColumnInfo = *col
	}

	var rule *placement.Rule
	for _, opt := range s.Options {
		if opt.Tp == ast.TableOptionEngine {
			if tbMeta.Attributes == nil {
				tbMeta.Attributes = make(map[string]string)
			}
			tbMeta.Attributes["AccessMode"] = opt.StrValue
		}
		if opt.Tp == ast.TableOptionComment {
			rule = buildRuleFromComment(opt.StrValue, tbMeta)
		}
	}

	if rule != nil {
		tbMeta.Rules = append(tbMeta.Rules, rule)
	}

	return tbMeta
}

func buildRuleFromComment(comment string, tbMeta *model.TableMeta) *placement.Rule {
	fmt.Println("comment:", comment)
	if comment == "" {
		return nil
	}
	labelConstraint := placement.LabelConstraint{}
	err := json.Unmarshal([]byte(comment), &labelConstraint)
	if err != nil {
		fmt.Println("json decode err:", err)
		return nil
	}
	fmt.Println("label constraint:", labelConstraint)
	rule := &placement.Rule{
		Index:    7,
		Override: true,
		Count:    3,
		Role:     placement.Voter,
	}
	rule.LabelConstraints = append(rule.LabelConstraints, labelConstraint)
	return rule
}

func toCreateIndex(s *ast.CreateIndexStmt) *model.IndexMeta {
	idxMeta := &model.IndexMeta{}
	tbName := s.Table.Name.L
	idxName := s.IndexName
	defineCols := make([]string, len(s.IndexPartSpecifications))
	for i, col := range s.IndexPartSpecifications {
		colName := col.Column.Name.L
		defineCols[i] = colName
	}
	idxMeta.TableName = tbName
	idxMeta.Name = idxName
	idxMeta.DefinedColumns = defineCols
	if s.KeyType == ast.IndexKeyTypeUnique {
		idxMeta.Unique = true
	}
	return idxMeta
}
