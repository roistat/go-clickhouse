package clickhouse

import (
	"errors"
	"fmt"
	"strings"
)

const (
	successTestResponse = "Ok."
)

type Conn struct {
	Host   string
	client *queryClient
}

func NewConn(host string) *Conn {
	host = "http://" + strings.Replace(host, "http://", "", 1)
	host = strings.TrimRight(host, "/") + "/"

	return &Conn{
		Host: host,
		client: &queryClient{
			get:  getQueryInstance,
			post: postQueryInstance,
		},
	}
}

func (c *Conn) Ping() (err error) {
	var res string
	res, err = request("GET", c.Host, "ping")
	if err == nil {
		if !strings.Contains(res, successTestResponse) {
			err = errors.New(fmt.Sprintf("Clickhouse host response was '%s', expected '%s'.", res, successTestResponse))
		}
	}

	return err
}
