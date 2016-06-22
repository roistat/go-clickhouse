package clickhouse

import (
	"bytes"
	"net/http"
	"net/url"
	"strings"
)

type getQuery func(host string, query string) (string, error)

type queryClient struct {
	get getQuery
}

func req(h string, s string) (res string, err error) {
	resp, err := http.Get(h + s)
	if err == nil {
		defer resp.Body.Close()
		buf := new(bytes.Buffer)
		_, err = buf.ReadFrom(resp.Body)
		res = buf.String()
	}

	return res, err
}

func query(h string, q string) (res string, err error) {
	res, err = req(h, "?query="+url.QueryEscape(q))
	if err == nil {
		res = strings.Trim(res, "\n\r")
	}

	return res, err
}
