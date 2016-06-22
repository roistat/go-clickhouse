package clickhouse

import (
	"errors"
	"fmt"
	"strings"
)

func (c *Conn) Query(stmt string, args ...interface{}) *Query {
	return &Query{
		Stmt: stmt,
		args: args,
		conn: c,
	}
}

func (c *Conn) Insert(table string, cols []string, row []interface{}) error {
	return c.MultiInsert(table, cols, [][]interface{}{row})
}

func (c *Conn) MultiInsert(table string, cols []string, rows [][]interface{}) error {
	if len(rows) < 1 {
		return nil
	}
	batch := make([]string, len(rows))
	for i, row := range rows {
		prepared := make([]string, len(row))
		for k, val := range row {
			prepared[k] = marshal(val)
		}
		batch[i] = "(" + strings.Join(prepared, ",") + ")"
	}
	stmt := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s", table, strings.Join(cols, ","), strings.Join(batch, ","))
	return c.Query(stmt).Exec()
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

func (c *Conn) CreateDatabase(name string, ifNotExists bool) error {
	if ifNotExists {
		return c.Query("CREATE DATABASE IF NOT EXISTS " + name).Exec()
	} else {
		return c.Query("CREATE DATABASE " + name).Exec()
	}
}
