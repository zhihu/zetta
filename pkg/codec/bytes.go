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
	"encoding/binary"

	"github.com/pingcap/errors"
)

const (
	encGroupSize = 9
	encPad       = 0x0
)

/*
  This is the new algorithm.  Similarly to the legacy format the input
  is split up into N-1 bytes and a flag byte is used as the Nth byte
  in the output.
  - If the previous segment needed any padding the flag is set to the
    number of bytes used (0..N-2).  0 is possible in the first segment
    if the input is 0 bytes long.
  - If no padding was used and there is no more data left in the input
    the flag is set to N-1
  - If no padding was used and there is still data left in the input the
    flag is set to N.
  For N=9, the following input values encode to the specified
  outout (where 'X' indicates a byte of the original input):
  - 0 bytes  is encoded as 0 0 0 0 0 0 0 0 0
  - 1 byte   is encoded as X 0 0 0 0 0 0 0 1
  - 2 bytes  is encoded as X X 0 0 0 0 0 0 2
  - 7 bytes  is encoded as X X X X X X X 0 7
  - 8 bytes  is encoded as X X X X X X X X 8
  - 9 bytes  is encoded as X X X X X X X X 9 X 0 0 0 0 0 0 0 1
  - 10 bytes is encoded as X X X X X X X X 9 X X 0 0 0 0 0 0 2
*/
func EncodeBytes(b []byte, data []byte) []byte {
	start := 0
	copyLen := encGroupSize - 1
	left := len(data)

	for {
		if left < encGroupSize-1 {
			copyLen = left
		}
		b = append(b, data[start:start+copyLen]...)
		left = left - copyLen
		if left == 0 {
			padding := make([]byte, encGroupSize-1-copyLen)
			b = append(b, padding...)
			b = append(b, byte(copyLen))
			break
		}
		b = append(b, byte(encGroupSize))
		start = start + encGroupSize - 1
	}
	return b
}

func decodeBytes(b, buf []byte) ([]byte, []byte, error) {
	if buf == nil {
		buf = make([]byte, 0, len(b))
	}
	if len(b) < encGroupSize {
		return b, buf, errors.New("No enough bytes to decode")
	}
	buf = buf[:0]
	for {
		used := b[encGroupSize-1]
		if used > encGroupSize {
			return b, buf, errors.Errorf("Invalid flag, groupBytes: %q", b[:encGroupSize])
		}
		if used <= encGroupSize-1 {
			for _, pad := range b[used : encGroupSize-1] {
				if pad != encPad {
					return b, buf, errors.Errorf("Pad bytes not 0x0, groupBytes: %q", b[:encGroupSize])
				}
			}
			buf = append(buf, b[:used]...)
			break
		}
		buf = append(buf, b[:encGroupSize-1]...)
		b = b[encGroupSize:]
	}
	return b[encGroupSize:], buf, nil
}

func DecodeBytes(b, buf []byte) ([]byte, []byte, error) {
	return decodeBytes(b, buf)
}

// EncodeCompactBytes joins bytes with its length into a byte slice. It is more
// efficient in both space and time compare to EncodeBytes. Note that the encoded
// result is not memcomparable.
func EncodeCompactBytes(b []byte, data []byte) []byte {
	b = reallocBytes(b, binary.MaxVarintLen64+len(data))
	b = EncodeVarint(b, int64(len(data)))
	return append(b, data...)
}

// DecodeCompactBytes decodes bytes which is encoded by EncodeCompactBytes before.
func DecodeCompactBytes(b []byte) ([]byte, []byte, error) {
	b, n, err := DecodeVarint(b)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	if int64(len(b)) < n {
		return nil, nil, errors.Errorf("insufficient bytes to decode value, expected length: %v", n)
	}
	return b[n:], b[:n], nil
}

// reallocBytes is like realloc.
func reallocBytes(b []byte, n int) []byte {
	newSize := len(b) + n
	if cap(b) < newSize {
		bs := make([]byte, len(b), newSize)
		copy(bs, b)
		return bs
	}

	// slice b has capability to store n bytes
	return b
}
