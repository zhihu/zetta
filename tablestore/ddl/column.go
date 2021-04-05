package ddl

import (
	"github.com/pingcap/errors"
	parser_model "github.com/pingcap/parser/model"
	"github.com/zhihu/zetta/pkg/meta"
	"github.com/zhihu/zetta/pkg/model"
)

func setColumnsState(cols []*model.ColumnMeta, state parser_model.SchemaState) {
	for _, c := range cols {
		c.State = state
	}
}

func addColumns(tbMeta *model.TableMeta, cols []*model.ColumnMeta) {
	for _, c := range cols {
		addColumn(tbMeta, c)
	}
}

func onAddColumn(d *ddlCtx, t *meta.Meta, job *model.Job) (int64, error) {
	var (
		ver int64
		err error
	)
	column := &model.ColumnMeta{}
	if err = job.DecodeArgs(column); err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	columns := []*model.ColumnMeta{column}
	tbMeta, err := getTableInfoAndCancelFaultJob(t, job, job.SchemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}
	//setColumnsState(columns, parser_model.StateNone)
	addColumns(tbMeta, columns)
	ver, err = updateSchemaVersion(t, job)
	if err != nil {
		return ver, errors.Trace(err)
	}
	originalState := columns[0].State
	switch columns[0].State {
	case parser_model.StateNone:
		job.SchemaState = model.StateDeleteOnly
		setColumnsState(columns, parser_model.StateDeleteOnly)
		ver, err = updateVersionAndTableInfo(t, job, tbMeta, originalState != columns[0].State)
	case parser_model.StateDeleteOnly:
		job.SchemaState = model.StatePublic
		setColumnsState(columns, parser_model.StatePublic)
		ver, err = updateVersionAndTableInfo(t, job, tbMeta, originalState != columns[0].State)
		if err != nil {
			return ver, err
		}
		job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tbMeta)
	case parser_model.StatePublic:
		job.FinishTableJob(model.JobStateDone, model.StatePublic, ver, tbMeta)
		return ver, nil
	default:
		err = ErrInvalidTableState.GenWithStack("invalid column state %v", originalState)
	}

	return ver, errors.Trace(err)
}

//func (w *worker) onModifyColumn(t *meta.Meta, job *model.Job) (ver int64, _ error) {
//}
