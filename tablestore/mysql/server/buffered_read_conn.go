package server

import (
	"bufio"
	"net"
)

const defaultReaderSize = 16 * 1024

// bufferedReadConn is a net.Conn compatible structure that reads from bufio.Reader.
type bufferedReadConn struct {
	net.Conn
	rb *bufio.Reader
}

func (conn bufferedReadConn) Read(b []byte) (n int, err error) {
	return conn.rb.Read(b)
}

func newBufferedReadConn(conn net.Conn) *bufferedReadConn {
	return &bufferedReadConn{
		Conn: conn,
		rb:   bufio.NewReaderSize(conn, defaultReaderSize),
	}
}
