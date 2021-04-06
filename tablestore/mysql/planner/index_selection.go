package planner

import (
	parser_model "github.com/pingcap/parser/model"
	"github.com/pingcap/parser/opcode"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	tspb "github.com/zhihu/zetta-proto/pkg/tablestore"
	"github.com/zhihu/zetta/pkg/model"
	"github.com/zhihu/zetta/tablestore/mysql/expression"
)

type scoreIndexSelect interface {
	score(idxSel *indexSelection)
}

type baseIndexSelection struct {
	weight int
}

func newBaseIndexSelection(weight int) *baseIndexSelection {
	return &baseIndexSelection{
		weight: weight,
	}
}

func (s *baseIndexSelection) score(idxSel *indexSelection) {}

type colNumsSelection struct {
	*baseIndexSelection
}

func (s *colNumsSelection) score(idxSel *indexSelection) {
	idxSel.score = idxSel.score + s.weight*len(idxSel.cols)
}

type primarySelection struct {
	*baseIndexSelection
}

func (s *primarySelection) score(idxSel *indexSelection) {
	if idxSel.meta.IsPrimary {
		idxSel.score = idxSel.score + s.weight
	}
}

type leftPrefixSelection struct {
	*baseIndexSelection
}

func (s *leftPrefixSelection) score(idxSel *indexSelection) {
	hasLeftPrefixCol := false
	for _, col := range idxSel.cols {
		if col.Name.L == idxSel.meta.DefinedColumns[0] {
			hasLeftPrefixCol = true
			break
		}
	}
	if hasLeftPrefixCol {
		idxSel.score = idxSel.score + s.weight
	}
}

var indexSelectionStrategys = []scoreIndexSelect{
	&colNumsSelection{
		baseIndexSelection: newBaseIndexSelection(1),
	},
	&primarySelection{
		baseIndexSelection: newBaseIndexSelection(10),
	},
	&leftPrefixSelection{
		baseIndexSelection: newBaseIndexSelection(100),
	},
}

func scoreWithStrategys(idxSel *indexSelection) {
	for _, s := range indexSelectionStrategys {
		s.score(idxSel)
	}
}

type indexSelection struct {
	meta     *model.IndexMeta
	exprs    []expression.Expression
	cols     []*parser_model.ColumnInfo
	funcName []string
	//col offset in defined columns.
	offsets []int
	datums  []types.Datum
	score   int
}

func selectExprsByIndex(tbMeta *model.TableMeta, exprs []expression.Expression) (left []expression.Expression, idxSel *indexSelection) {
	idxSelMap := collectIndex(tbMeta, exprs)
	if len(idxSelMap) == 0 {
		return exprs, nil
	}
	scoreIndexSels(idxSelMap)
	idxSel = &indexSelection{}
	for _, sel := range idxSelMap {
		if idxSel.score < sel.score {
			idxSel = sel
		}
	}
	selectedCols := make([]bool, len(idxSel.meta.DefinedColumns))
	for _, offset := range idxSel.offsets {
		selectedCols[offset] = true
	}
	mostLeft := 0
	for i := range selectedCols {
		if selectedCols[i] {
			mostLeft = i
		} else {
			break
		}
		if idxSel.funcName[i] != opcode.EQ.String() {
			break
		}
	}
	for _, expr := range exprs {
		same := false
		for i := 0; i <= mostLeft; i++ {
			if idxSel.exprs[i] == expr {
				same = true
				break
			}
		}
		if !same {
			left = append(left, expr)
		}
	}
	return
}

func scoreIndexSels(idxSels map[string]*indexSelection) {
	for _, idxSel := range idxSels {
		scoreWithStrategys(idxSel)
	}
}

func collectIndex(tbMeta *model.TableMeta, exprs []expression.Expression) map[string]*indexSelection {
	idxSelMap := make(map[string]*indexSelection)
	for _, expr := range exprs {
		switch x := expr.(type) {
		case *expression.ScalarFunction:
			args := x.GetArgs()
			if len(args) != 2 {
				continue
			}
			if col, ok := args[0].(*expression.Column); ok {
				if value, ok := args[1].(*expression.Constant); ok {
					colInfo := col.ToInfo()
					dt, _ := value.Value.ConvertTo(&stmtctx.StatementContext{}, col.GetType())

					if offset := isColBelongsToPK(colInfo.Name.L, tbMeta.PrimaryKey); offset != -1 {
						if idxMeta, ok := idxSelMap["primary"]; ok {
							idxMeta.exprs = append(idxMeta.exprs, expr)
							idxMeta.cols = append(idxMeta.cols, colInfo)
							idxMeta.datums = append(idxMeta.datums, dt)
							idxMeta.offsets = append(idxMeta.offsets, offset)
							idxMeta.funcName = append(idxMeta.funcName, x.FuncName.L)
						} else {
							pkMeta := &model.IndexMeta{
								IsPrimary: true,
								IndexMeta: tspb.IndexMeta{
									Name:           "primary",
									DefinedColumns: tbMeta.PrimaryKey,
									Unique:         true,
								},
							}
							idxSel := &indexSelection{
								meta:     pkMeta,
								exprs:    []expression.Expression{expr},
								cols:     []*parser_model.ColumnInfo{colInfo},
								datums:   []types.Datum{dt},
								offsets:  []int{offset},
								funcName: []string{x.FuncName.L},
							}
							idxSelMap[colInfo.Name.L] = idxSel
						}
					}

					if offset, idxMeta := tbMeta.GetIndexByColumnName(colInfo.Name.L); idxMeta != nil {
						if idxSel, ok := idxSelMap[idxMeta.Name]; ok {
							idxSel.exprs = append(idxSel.exprs, expr)
							idxSel.cols = append(idxSel.cols, colInfo)
							idxSel.datums = append(idxSel.datums, dt)
							idxSel.offsets = append(idxSel.offsets, offset)
							idxSel.funcName = append(idxSel.funcName, x.FuncName.L)
						} else {
							idxSel := &indexSelection{
								meta:     idxMeta,
								exprs:    []expression.Expression{expr},
								cols:     []*parser_model.ColumnInfo{colInfo},
								datums:   []types.Datum{dt},
								offsets:  []int{offset},
								funcName: []string{x.FuncName.L},
							}
							idxSelMap[colInfo.Name.L] = idxSel
						}
					}
				}
			}
		}
	}
	return idxSelMap
}

func isColBelongsToPK(col string, pkeys []string) int {
	for i, pk := range pkeys {
		if col == pk {
			return i
		}
	}
	return -1
}
