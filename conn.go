package clickhouse

import (
	"strings"
)

const (
	successTestResponse = "Ok."
)

type Conn struct {
	Host   string
	client *queryClient
}

func Connect(host string) *Conn {
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
