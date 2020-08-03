package ddl

import (
	"github.com/pingcap/errors"
	"github.com/zhihu/zetta/pkg/meta"
	"github.com/zhihu/zetta/pkg/model"
)

func setColumnsState(cols []*model.ColumnMeta, state model.SchemaState) {
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
	columns := []*model.ColumnMeta{}
	if err = job.DecodeArgs(columns); err != nil {
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}
	tbMeta, err := getTableInfoAndCancelFaultJob(t, job, job.SchemaID)
	if err != nil {
		return ver, errors.Trace(err)
	}
	setColumnsState(columns, model.StateNone)
	addColumns(tbMeta, columns)
	ver, err = updateSchemaVersion(t, job)
	if err != nil {
		return ver, errors.Trace(err)
	}
	originalState := columns[0].State
	switch columns[0].State {
	case model.StateNone:
		job.SchemaState = model.StateDeleteOnly
		setColumnsState(columns, model.StateDeleteOnly)
		ver, err = updateVersionAndTableInfo(t, job, tbMeta, originalState != columns[0].State)
	case model.StateDeleteOnly:
		job.SchemaState = model.StatePublic
		setColumnsState(columns, model.StatePublic)
		ver, err = updateVersionAndTableInfo(t, job, tbMeta, originalState != columns[0].State)
	default:
		err = ErrInvalidTableState.GenWithStack("invalid column state %v", originalState)
	}

	return ver, errors.Trace(err)
}
