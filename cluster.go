package clickhouse

import (
	"math/rand"
	"sync"
)

type PingErrorEvent func(*Conn)

type Cluster struct {
	conn   []*Conn
	active []*Conn
	fail   PingErrorEvent
	mx     sync.Mutex
}

func NewCluster(conn ...*Conn) *Cluster {
	return &Cluster{
		conn: conn,
	}
}

func (c *Cluster) OnFail(f PingErrorEvent) {
	c.fail = f
}

func (c *Cluster) Active() *Conn {
	c.mx.Lock()
	defer c.mx.Unlock()
	l := len(c.active)
	if l < 1 {
		return nil
	}
	return c.active[rand.Intn(l)]
}

func (c *Cluster) CheckConnections() {
	var (
		err error
		res []*Conn
	)

	for _, conn := range c.conn {
		err = conn.Ping()
		if err == nil {
			res = append(res, conn)
		} else {
			c.fail(conn)
		}
	}

	c.mx.Lock()
	c.active = res
	c.mx.Unlock()
}
