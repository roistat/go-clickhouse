package clickhouse

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestCheckServers(t *testing.T) {
	goodTr := getMockTransport("Ok.")
	badTr := getMockTransport("Code: 9999, Error: ...")

	conn1 := NewConn(getHost(), badTr)
	conn2 := NewConn(getHost(), goodTr)

	cl := NewCluster(conn1, conn2)
	assert.Equal(t, conn1, cl.conn[0])
	assert.Equal(t, conn2, cl.conn[1])

	cl.OnPingError(func(c *Conn) {
		assert.Equal(t, conn1, c)
	})

	cl.Ping()

	assert.Equal(t, conn2, cl.ActiveConn())

	conn1 = NewConn(getHost(), goodTr)
	conn2 = NewConn(getHost(), badTr)
	cl.OnPingError(func(c *Conn) {
		assert.Equal(t, conn2, c)
	})

	cl.Ping()

	assert.Equal(t, conn1, cl.ActiveConn())

}
