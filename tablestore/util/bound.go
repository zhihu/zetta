package util

import (
	"github.com/pingcap/tidb/types"
)

type Bound struct {
	Less     bool
	Desc     bool
	Included bool
	Value    *types.Datum
}
