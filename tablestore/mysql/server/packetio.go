package server

import (
	"bufio"
	"io"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
)

const defaultWriterSize = 16 * 1024

// packetIO is a helper to read and write data in packet format.
type packetIO struct {
	bufReadConn *bufferedReadConn
	bufWriter   *bufio.Writer
	sequence    uint8
	readTimeout time.Duration
}

func newPacketIO(bufReadConn *bufferedReadConn) *packetIO {
	p := &packetIO{sequence: 0}
	p.setBufferedReadConn(bufReadConn)
	return p
}

func (p *packetIO) setBufferedReadConn(bufReadConn *bufferedReadConn) {
	p.bufReadConn = bufReadConn
	p.bufWriter = bufio.NewWriterSize(bufReadConn, defaultWriterSize)
}

func (p *packetIO) setReadTimeout(timeout time.Duration) {
	p.readTimeout = timeout
}

func (p *packetIO) readOnePacket() ([]byte, error) {
	var header [4]byte
	if p.readTimeout > 0 {
		if err := p.bufReadConn.SetReadDeadline(time.Now().Add(p.readTimeout)); err != nil {
			return nil, err
		}
	}
	if _, err := io.ReadFull(p.bufReadConn, header[:]); err != nil {
		return nil, errors.Trace(err)
	}

	sequence := header[3]
	if sequence != p.sequence {
		return nil, errInvalidSequence.GenWithStack("invalid sequence %d != %d", sequence, p.sequence)
	}

	p.sequence++

	length := int(uint32(header[0]) | uint32(header[1])<<8 | uint32(header[2])<<16)

	data := make([]byte, length)
	if p.readTimeout > 0 {
		if err := p.bufReadConn.SetReadDeadline(time.Now().Add(p.readTimeout)); err != nil {
			return nil, err
		}
	}
	if _, err := io.ReadFull(p.bufReadConn, data); err != nil {
		return nil, errors.Trace(err)
	}
	return data, nil
}

func (p *packetIO) readPacket() ([]byte, error) {
	data, err := p.readOnePacket()
	if err != nil {
		return nil, errors.Trace(err)
	}

	if len(data) < mysql.MaxPayloadLen {
		return data, nil
	}

	// handle multi-packet
	for {
		buf, err := p.readOnePacket()
		if err != nil {
			return nil, errors.Trace(err)
		}

		data = append(data, buf...)

		if len(buf) < mysql.MaxPayloadLen {
			break
		}
	}

	return data, nil
}

// writePacket writes data that already have header
func (p *packetIO) writePacket(data []byte) error {
	length := len(data) - 4

	for length >= mysql.MaxPayloadLen {
		data[0] = 0xff
		data[1] = 0xff
		data[2] = 0xff

		data[3] = p.sequence

		if n, err := p.bufWriter.Write(data[:4+mysql.MaxPayloadLen]); err != nil {
			return errors.Trace(mysql.ErrBadConn)
		} else if n != (4 + mysql.MaxPayloadLen) {
			return errors.Trace(mysql.ErrBadConn)
		} else {
			p.sequence++
			length -= mysql.MaxPayloadLen
			data = data[mysql.MaxPayloadLen:]
		}
	}

	data[0] = byte(length)
	data[1] = byte(length >> 8)
	data[2] = byte(length >> 16)
	data[3] = p.sequence

	if n, err := p.bufWriter.Write(data); err != nil {
		terror.Log(errors.Trace(err))
		return errors.Trace(mysql.ErrBadConn)
	} else if n != len(data) {
		return errors.Trace(mysql.ErrBadConn)
	} else {
		p.sequence++
		return nil
	}
}

func (p *packetIO) flush() error {
	err := p.bufWriter.Flush()
	if err != nil {
		return errors.Trace(err)
	}
	return err
}
