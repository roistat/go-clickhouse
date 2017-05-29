package clickhouse

import (
	"bytes"
	"mime/multipart"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const (
	httpTransportBodyType = "text/plain"
)

type Transport interface {
	Exec(conn *Conn, q Query, readOnly bool) (res string, err error)
}

type HttpTransport struct {
	Timeout time.Duration
}

func (t HttpTransport) Exec(conn *Conn, q Query, readOnly bool) (res string, err error) {
	var resp *http.Response
	query := prepareHttp(q.Stmt, q.args)
	client := &http.Client{Timeout: t.Timeout}
	if readOnly {
		if len(query) > 0 {
			query = getDb(conn) + "query=" + query
		}
		resp, err = client.Get(conn.Host + query)
	} else {
		var req *http.Request
		req, err = prepareExecPostRequest(conn, q)
		if err != nil {
			return "", err
		}

		resp, err = client.Do(req)
	}
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	buf := new(bytes.Buffer)
	_, err = buf.ReadFrom(resp.Body)

	return buf.String(), err
}

func getDb(conn *Conn) string {
	if conn.db == "" {
		return "?"
	} else {
		return "?database=" + conn.db
	}
}

func prepareExecPostRequest(conn *Conn, q Query) (*http.Request, error) {
	query := prepareHttp(q.Stmt, q.args)
	var req *http.Request
	var err error = nil
	if len(q.externals) > 0 {
		if len(query) > 0 {
			query = getDb(conn) + "query=" + url.QueryEscape(query)
		}

		body := &bytes.Buffer{}
		writer := multipart.NewWriter(body)

		for _, ext := range q.externals {
			query = query + "&" + ext.Name + "_structure=" + url.QueryEscape(ext.Structure)
			part, err := writer.CreateFormFile(ext.Name, ext.Name)
			if err != nil {
				return nil, err
			}
			_, err = part.Write(ext.Data)
			if err != nil {
				return nil, err
			}
		}

		err = writer.Close()
		if err != nil {
			return nil, err
		}

		req, err = http.NewRequest("POST", conn.Host + query, body)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Content-Type", writer.FormDataContentType())
	} else {
		req, err = http.NewRequest("POST", conn.Host + getDb(conn), strings.NewReader(query))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Content-Type", httpTransportBodyType)
	}
	return req, err
}

func prepareHttp(stmt string, args []interface{}) string {
	var res []byte
	buf := []byte(stmt)
	res = make([]byte, 0)
	k := 0
	for _, ch := range buf {
		if ch == '?' {
			res = append(res, []byte(marshal(args[k]))...)
			k++
		} else {
			res = append(res, ch)
		}
	}

	return string(res)
}
