package clickhouse

import (
	"errors"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestConnect(t *testing.T) {
	var conn *Conn
	tr := getMockTransport("Ok.")

	conn = NewConn("host.local", tr)
	assert.Equal(t, "http://host.local/", conn.Host)

	conn = NewConn("http://host.local/", tr)
	assert.Equal(t, "http://host.local/", conn.Host)

	conn = NewConn("http:/host.local", tr)
	assert.Equal(t, "http://http:/host.local/", conn.Host)
}

func TestConn_Ping(t *testing.T) {
	tr := getMockTransport("Ok.")
	conn := NewConn("host.local", tr)
	assert.NoError(t, conn.Ping())
}

func TestConn_Ping2(t *testing.T) {
	tr := getMockTransport("")
	conn := NewConn("host.local", tr)
	assert.Error(t, conn.Ping())
}

func TestConn_Ping3(t *testing.T) {
	tr := badTransport{err: errors.New("Connection timeout")}
	conn := NewConn("host.local", tr)
	assert.Error(t, conn.Ping())
	assert.Equal(t, "Connection timeout", conn.Ping().Error())
}
