package clickhouse

import (
	"bytes"
	"net/http"
	"net/url"
	"strings"
)

const (
	MethodPost = "POST"
	MethodGet  = "GET"
)

type getQuery func(host string, query string) (string, error)
type postQuery func(host string, query string) (string, error)

type queryClient struct {
	get  getQuery
	post postQuery
}

func request(method, host, data string) (res string, err error) {
	var resp *http.Response
	switch method {
	case MethodPost:
		resp, err = http.Post(host, "text/plain", strings.NewReader(data))
	default:
	case MethodGet:
		resp, err = http.Get(host + data)
	}
	if err == nil {
		defer resp.Body.Close()
		buf := new(bytes.Buffer)
		_, err = buf.ReadFrom(resp.Body)
		res = buf.String()
	}

	return res, err
}

func getQueryInstance(host, stmt string) (res string, err error) {
	res, err = request(MethodGet, host, "?query="+url.QueryEscape(stmt))
	if err == nil {
		res = strings.Trim(res, "\n\r")
	}

	return res, err
}

func postQueryInstance(host, stmt string) (res string, err error) {
	res, err = request(MethodPost, host, stmt)
	if err == nil {
		res = strings.Trim(res, "\n\r")
	}

	return res, err
}
