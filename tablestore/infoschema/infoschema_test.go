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

package infoschema_test

import (
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/util/testleak"
	tspb "github.com/zhihu/zetta-proto/pkg/tablestore"
	"github.com/zhihu/zetta/pkg/meta"
	"github.com/zhihu/zetta/pkg/model"
	"github.com/zhihu/zetta/tablestore/infoschema"
)

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	TestingT(t)
}

var _ = Suite(&testSuite{})

type testSuite struct {
}

func getMeta(store kv.Storage, c *C) *meta.Meta {
	startTs, err := store.CurrentVersion()
	c.Assert(err, IsNil)
	snapshot, err := store.GetSnapshot(kv.NewVersion(startTs.Ver))
	c.Assert(err, IsNil)
	return meta.NewSnapshotMeta(snapshot)
}

func (*testSuite) TestT(c *C) {
	defer testleak.AfterTest(c)()
	store, err := mockstore.NewMockTikvStore()
	c.Assert(err, IsNil)
	defer store.Close()

	dbMeta := &model.DatabaseMeta{
		DatabaseMeta: tspb.DatabaseMeta{
			Id:       1,
			Database: "d",
		},
	}
	err = kv.RunInNewTxn(store, true, func(txn kv.Transaction) error {
		err = meta.NewMeta(txn).CreateDatabase(dbMeta)
		return errors.Trace(err)
	})
	c.Assert(err, IsNil)

	tbMeta := &model.TableMeta{
		TableMeta: tspb.TableMeta{
			Id:        1,
			TableName: "t",
			Database:  "d",
		},
	}
	err = kv.RunInNewTxn(store, true, func(txn kv.Transaction) error {
		err = meta.NewMeta(txn).CreateTableOrView(1, tbMeta)
		return errors.Trace(err)
	})

	m := getMeta(store, c)
	h := infoschema.NewHandler()
	err = h.InitCurrentInfoSchema(m)
	c.Assert(err, IsNil)

	is := h.Get()
	dm, _ := is.GetDatabaseMetaByName("d")
	c.Assert(dm, DeepEquals, dbMeta)
	tm, _ := is.GetTableMetaByName("d", "t")
	c.Assert(tm, DeepEquals, tbMeta)
}
