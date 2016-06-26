package clickhouse

import (
	"log"
	"strings"
)

type Query struct {
	Stmt string
	conn *Conn
	args []interface{}
}

func (q Query) Iter(conn *Conn) *Iter {
	prepared := prepare(q.Stmt, q.args)
	resp, err := conn.client.get(conn.Host, prepared)
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
	var resp string
	prepared := prepare(q.Stmt, q.args)
	resp, err = conn.client.post(conn.Host, prepared)
	if err == nil {
		err = errorFromResponse(resp)
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

func prepare(stmt string, args []interface{}) (res string) {
	res = stmt
	for _, arg := range args {
		res = strings.Replace(res, "?", marshal(arg), 1)
	}

	return stmt
}
