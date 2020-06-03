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

package codec

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/util/testleak"
)

var _ = Suite(&testBytesSuite{})

type testBytesSuite struct {
}

func (s *testBytesSuite) TestBytesCodec(c *C) {
	defer testleak.AfterTest(c)()
	inputs := []struct {
		enc []byte
		dec []byte
	}{
		{[]byte{}, []byte{0, 0, 0, 0, 0, 0, 0, 0, 0}},
		{[]byte{0}, []byte{0, 0, 0, 0, 0, 0, 0, 0, 1}},
		{[]byte{1, 2, 3}, []byte{1, 2, 3, 0, 0, 0, 0, 0, 3}},
		{[]byte{1, 2, 3, 0}, []byte{1, 2, 3, 0, 0, 0, 0, 0, 4}},
		{[]byte{1, 2, 3, 4, 5, 6, 7}, []byte{1, 2, 3, 4, 5, 6, 7, 0, 7}},
		{[]byte{0, 0, 0, 0, 0, 0, 0, 0}, []byte{0, 0, 0, 0, 0, 0, 0, 0, 8}},
		{[]byte{1, 2, 3, 4, 5, 6, 7, 8}, []byte{1, 2, 3, 4, 5, 6, 7, 8, 8}},
		{[]byte{1, 2, 3, 4, 5, 6, 7, 8, 9}, []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 9, 0, 0, 0, 0, 0, 0, 0, 1}},
	}

	for _, input := range inputs {
		b := EncodeBytes(nil, input.enc)
		c.Assert(b, BytesEquals, input.dec)
		_, d, err := DecodeBytes(b, nil)
		c.Assert(err, IsNil)
		c.Assert(d, BytesEquals, input.enc)
	}

	// Test error decode.
	errInputs := [][]byte{
		{1, 2, 3, 4},
		{0, 0, 0, 0, 0, 0, 0, 7},
		{0, 0, 0, 0, 0, 0, 0, 0, 10},
		{1, 2, 0, 0, 0, 0, 0, 1, 3},
	}

	for _, input := range errInputs {
		_, _, err := DecodeBytes(input, nil)
		c.Assert(err, NotNil)
	}
}
