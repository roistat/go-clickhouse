package clickhouse

import (
	"bytes"
	"net/http"
	"net/url"
	"strings"
	"errors"
)

type Statement struct {
	Text string
}

type QueryResult struct {
	err error
	text string
}

type Iter struct {
	data string
}

func (r *QueryResult) Error() error {
	return r.err
}

func (r *QueryResult) Iter() *QueryResult {
	return r
}

func (r *QueryResult) Scan(...interface{}) bool {
	var row string
	pos := strings.Index(r.text, "\n")
	if pos == -1 {
		row = r.text
		r.text = ""
	} else {
		row = r.text[:pos]
		r.text = r.text[pos+1:]
	}

	if len(row) == 0 {
		return false
	}
	//a := strings.Split(row, "\t")
	return true
}

func (c *Conn) Query(s string) *QueryResult {
	resp, err := query(c.Host, s)
	if err != nil {
		return &QueryResult{
			err: err,
		}
	}

	if strings.Contains(resp, "Code:") {
		return &QueryResult{
			err: errors.New(resp),
		}
	}

	return &QueryResult{
		text: resp,
	}
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
