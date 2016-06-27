package clickhouse

import (
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
