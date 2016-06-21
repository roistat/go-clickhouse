package clickhouse

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestConnect(t *testing.T) {
	var conn *Conn

	conn = Connect("host.local")
	assert.Equal(t, "http://host.local/", conn.Host)

	conn = Connect("http://host.local/")
	assert.Equal(t, "http://host.local/", conn.Host)

	conn = Connect("http:/host.local")
	assert.Equal(t, "http://http:/host.local/", conn.Host)
}
