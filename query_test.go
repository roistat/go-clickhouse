package clickhouse

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func nextGetResponse(c *Conn, resp string, err error) {
	c.client.get = func(host string, query string) (string, error) {
		return resp, err
	}
}

func nextPostResponse(c *Conn, resp string, err error) {
	c.client.post = func(host string, query string) (string, error) {
		return resp, err
	}
}

func TestConn_Query(t *testing.T) {
	conn := Connect("host.local")

	nextGetResponse(conn, "1\t2", nil)
	iter := conn.Query("SELECT 1, 2 FROM table").Iter()
	var v1, v2 int64
	scan := iter.Scan(&v1, &v2)
	assert.True(t, scan)
}

func TestIter_ScanInt(t *testing.T) {
	conn := Connect("host.local")

	nextGetResponse(conn, "1\t2", nil)
	iter := conn.Query("SELECT 1, 2 FROM table").Iter()
	var v1, v2 int
	scan := iter.Scan(&v1, &v2)
	assert.True(t, scan)
	if scan {
		assert.Equal(t, 1, v1)
		assert.Equal(t, 2, v2)
	}
}

func TestIter_ScanInt64(t *testing.T) {
	conn := Connect("host.local")

	nextGetResponse(conn, "1\t2", nil)
	iter := conn.Query("SELECT 1, 2 FROM table").Iter()
	var v1, v2 int64
	scan := iter.Scan(&v1, &v2)
	assert.True(t, scan)
	if scan {
		assert.Equal(t, int64(1), v1)
		assert.Equal(t, int64(2), v2)
	}
}

func TestIter_ScanString(t *testing.T) {
	conn := Connect("host.local")

	nextGetResponse(conn, "test1\ttest2", nil)
	iter := conn.Query("SELECT 'test1', 'test2' FROM table").Iter()
	var v1, v2 string
	scan := iter.Scan(&v1, &v2)
	assert.True(t, scan)
	if scan {
		assert.Equal(t, "test1", v1)
		assert.Equal(t, "test2", v2)
	}
}

func TestIter_ScanStringMultiple(t *testing.T) {
	conn := Connect("host.local")

	nextGetResponse(conn, "test1\ttest2\ntest3\ttest4", nil)
	iter := conn.Query("SELECT 'test1', 'test2' FROM table").Iter()
	var v1, v2 string
	scan := iter.Scan(&v1, &v2)
	assert.True(t, scan)
	if scan {
		assert.Equal(t, "test1", v1)
		assert.Equal(t, "test2", v2)
	}

	scan = iter.Scan(&v1, &v2)
	assert.True(t, scan)
	if scan {
		assert.Equal(t, "test3", v1)
		assert.Equal(t, "test4", v2)
	}
}
