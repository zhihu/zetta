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

package rpc

import (
	tspb "github.com/zhihu/zetta-proto/pkg/tablestore"
)

type MutationRequest interface {
	GetSession() string
	GetMutations() []*tspb.Mutation
}

type ReadRequest interface {
	GetSession() string
	GetTable() string
}

func ReadRequestFromProto(r *tspb.ReadRequest) *readRequest {
	return &readRequest{r}
}

type readRequest struct {
	*tspb.ReadRequest
}

type SparseReadRequest struct {
	*tspb.SparseReadRequest
}
