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
	"time"

	structpb "github.com/gogo/protobuf/types"
	tspb "github.com/zhihu/zetta-proto/pkg/tablestore"
)

type Response interface {
	CommitTimestamp() time.Time
}

func TimestampProto(t time.Time) *structpb.Timestamp {
	if t.IsZero() {
		return nil
	}
	ts, err := structpb.TimestampProto(t)
	if err != nil {
		return nil
	}
	return ts
}

func BuildMutationResponse(r Response) *tspb.MutationResponse {
	resp := &tspb.MutationResponse{
		CommitTimestamp: TimestampProto(r.CommitTimestamp()),
	}
	return resp
}

func BuildCommitResponse(r Response) *tspb.CommitResponse {
	resp := &tspb.CommitResponse{
		CommitTimestamp: TimestampProto(r.CommitTimestamp()),
	}
	return resp
}

type CommitTimeResp struct {
	CommitTs time.Time
}

func (r CommitTimeResp) CommitTimestamp() time.Time {
	return r.CommitTs
}
