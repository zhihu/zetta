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

package ddl

import (
	"github.com/pingcap/tidb/util/logutil"
	"github.com/zhihu/zetta/pkg/meta"
	"github.com/zhihu/zetta/pkg/model"
	"go.uber.org/zap"
)

func convertJob2RollbackJob(w *worker, d *ddlCtx, t *meta.Meta, job *model.Job) (ver int64, err error) {
	switch job.Type {
	// When some types of DDL job failed, we need to roll back, or dirty state would be remained.
	// Add more later.
	default:
		job.State = model.JobStateCancelled
		err = errCancelledDDLJob
	}

	if err != nil {
		if job.State != model.JobStateRollingback && job.State != model.JobStateCancelled {
			logutil.Logger(w.logCtx).Error("[ddl] run DDL job failed", zap.String("job", job.String()), zap.Error(err))
		} else {
			logutil.Logger(w.logCtx).Info("[ddl] the DDL job is cancelled normally", zap.String("job", job.String()), zap.Error(err))
		}

		job.Error = toTError(err)
		job.ErrorCount++
	}
	return
}
