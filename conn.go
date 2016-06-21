package clickhouse

import (
	"errors"
	"fmt"
	"strings"
)

import ()

const (
	successTestResponse = "Ok."
)

type Conn struct {
	Host string
}

func Connect(host string) *Conn {
	host = "http://" + strings.Replace(host, "http://", "", 1)
	host = strings.TrimRight(host, "/") + "/"

	return &Conn{
		Host: host,
	}
}

func (c *Conn) Ping() (err error) {
	var res string
	res, err = req(c.Host, "ping")
	if err == nil {
		if !strings.Contains(res, successTestResponse) {
			err = errors.New(fmt.Sprintf("Clickhouse host response was '%s', expected '%s'.", res, successTestResponse))
		}
	}

	return err
}