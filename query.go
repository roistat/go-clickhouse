package clickhouse

import (
	"errors"
	"log"
	"strings"
)

func (c *Conn) Query(s string) *Query {
	return &Query{
		conn: c,
		text: s,
	}
}

type Query struct {
	conn *Conn
	text string
	iter *Iter
}

func (q *Query) Iter() *Iter {
	if q.iter != nil {
		return q.iter
	}

	resp, err := q.conn.client.get(q.conn.Host, q.text)
	if err != nil {
		q.iter = &Iter{
			err: err,
		}
	}

	if strings.Contains(resp, "Code:") {
		q.iter = &Iter{
			err: errors.New(resp),
		}
	}

	q.iter = &Iter{
		text: resp,
	}

	return q.iter
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
	if len(a) != len(vars) {
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
