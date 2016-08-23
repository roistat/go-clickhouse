package clickhouse

import (
	"fmt"
	"strings"
)

const (
	successTestResponse = "Ok."
)

type Conn struct {
	Host      string
	transport Transport
}

func (c *Conn) Ping() (err error) {
	var res string
	res, err = c.transport.Exec(c, Query{Stmt: ""}, true)
	if err == nil {
		if !strings.Contains(res, successTestResponse) {
			err = fmt.Errorf("Clickhouse host response was '%s', expected '%s'.", res, successTestResponse)
		}
	}

	return err
}
