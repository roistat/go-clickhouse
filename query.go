package clickhouse

import (
	"log"
	"strings"
)

type Query struct {
	Stmt string
	args []interface{}
}

func (q Query) Iter(conn *Conn) *Iter {
	resp, err := conn.transport.Exec(conn, q, false)
	if err != nil {
		return &Iter{err: err}
	}

	if err != nil {
		return &Iter{err: err}
	}

	err = errorFromResponse(resp)
	if err != nil {
		return &Iter{err: err}
	} else {
		return &Iter{text: resp}
	}
}

func (q Query) Exec(conn *Conn) (err error) {
	resp, err := conn.transport.Exec(conn, q, false)
	if err == nil {
		if err == nil {
			err = errorFromResponse(resp)
		}
	}

	return err
}

type Iter struct {
	err  error
	text string
}

func (r *Iter) Error() error {
	return r.err
}

func (iter *Iter) Scan(vars ...interface{}) bool {
	row := iter.fetchNext()
	if len(row) == 0 {
		return false
	}
	a := strings.Split(row, "\t")
	if len(a) < len(vars) {
		return false
	}
	for i, v := range vars {
		err := unmarshal(v, a[i])
		if err != nil {
			log.Fatal(err)
			return false
		}
	}
	return true
}

func (r *Iter) fetchNext() string {
	var res string
	pos := strings.Index(r.text, "\n")
	if pos == -1 {
		res = r.text
		r.text = ""
	} else {
		res = r.text[:pos]
		r.text = r.text[pos+1:]
	}
	return res
}
