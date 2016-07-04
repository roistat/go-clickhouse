package clickhouse

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestPing(t *testing.T) {
	goodTr := getMockTransport("Ok.")
	badTr := getMockTransport("Code: 9999, Error: ...")

	conn1 := NewConn("host1", badTr)
	conn2 := NewConn("host2", goodTr)

	cl := NewCluster(conn1, conn2)
	assert.Equal(t, conn1, cl.conn[0])
	assert.Equal(t, conn2, cl.conn[1])

	assert.True(t, cl.IsDown())

	cl.OnPingError(func(c *Conn) {
		assert.Equal(t, conn1, c)
	})

	cl.Ping()

	assert.Equal(t, conn2.Host, cl.ActiveConn().Host)

	assert.False(t, cl.IsDown())

	cl.conn[0] = NewConn("host1", goodTr)
	cl.conn[1] = NewConn("host2", badTr)

	cl.OnPingError(func(c *Conn) {
		assert.Equal(t, conn2.Host, c.Host)
	})

	cl.Ping()

	assert.Equal(t, conn1.Host, cl.ActiveConn().Host)

	cl.conn[0] = NewConn("host1", badTr)
	cl.conn[1] = NewConn("host2", badTr)

	cl.OnPingError(func(c *Conn) {})
	cl.Ping()

	assert.Nil(t, cl.ActiveConn())

	assert.True(t, cl.IsDown())
}
