package clickhouse

type PingErrorEvent func(*Conn)

type Cluster struct {
	conn   []*Conn
	active []*Conn
	fail   PingErrorEvent
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
	return c.active[0]
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

	c.active = res
}
