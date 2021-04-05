package aggfuncs

import (
	"github.com/pingcap/tidb/util/chunk"
	"github.com/zhihu/zetta/tablestore/mysql/sctx"
)

type baseCount struct {
	baseAggFunc
}

type partialResult4Count = int64

func (e *baseCount) AllocPartialResult() PartialResult {
	return PartialResult(new(partialResult4Count))
}

func (e *baseCount) ResetPartialResult(pr PartialResult) {
	p := (*partialResult4Count)(pr)
	*p = 0
}

func (e *baseCount) AppendFinalResult2Chunk(sctx sctx.Context, pr PartialResult, chk *chunk.Chunk) error {
	p := (*partialResult4Count)(pr)
	chk.AppendInt64(e.ordinal, *p)
	return nil
}

type partialCount struct {
	baseCount
}

func (e *partialCount) UpdatePartialResult(sctx sctx.Context, rowsInGroup []chunk.Row, pr PartialResult) error {
	p := (*partialResult4Count)(pr)
	for _, row := range rowsInGroup {
		/*
			fmt.Println("get row  value:", row.GetInt64(0))
			input, isNull, err := e.args[0].EvalInt(sctx, row)
			if err != nil {
				return err
			}
			if isNull {
				continue
			}
			fmt.Println("input count:", input)
		*/
		*p += row.GetInt64(0)
	}
	//*p = *p + int64(len(rowsInGroup))
	return nil
}
