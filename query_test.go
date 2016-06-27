package clickhouse

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

type mockTransport struct {
	response string
}

func (m mockTransport) Exec(conn *Conn, q Query, readOnly bool) (r string, err error) {
	return m.response, nil
}

func TestIter_ScanInt(t *testing.T) {
	tr := getMockTransport("1\t2")
	conn := NewConn(getHost(), tr)

	iter := NewQuery("SELECT 1, 2").Iter(conn)
	var v1, v2 int
	scan := iter.Scan(&v1, &v2)
	assert.True(t, scan)
	if scan {
		assert.Equal(t, 1, v1)
		assert.Equal(t, 2, v2)
	}
}

func TestIter_ScanInt64(t *testing.T) {
	tr := getMockTransport("1\t2")
	conn := NewConn(getHost(), tr)

	iter := NewQuery("SELECT 1, 2").Iter(conn)
	var v1, v2 int64
	scan := iter.Scan(&v1, &v2)
	assert.True(t, scan)
	if scan {
		assert.Equal(t, int64(1), v1)
		assert.Equal(t, int64(2), v2)
	}
}

func TestIter_ScanString(t *testing.T) {
	tr := getMockTransport("test1\ttest2")
	conn := NewConn(getHost(), tr)

	iter := NewQuery("SELECT 'test1', 'test2'").Iter(conn)
	var v1, v2 string
	scan := iter.Scan(&v1, &v2)
	assert.True(t, scan)
	if scan {
		assert.Equal(t, "test1", v1)
		assert.Equal(t, "test2", v2)
	}
}

func TestIter_ScanStringMultiple(t *testing.T) {
	tr := getMockTransport("test1\ttest2\ntest3\ttest4")
	conn := NewConn(getHost(), tr)

	iter := NewQuery("SELECT 'test1', 'test2'").Iter(conn)
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

func getMockTransport(resp string) mockTransport {
	tr := mockTransport{}
	tr.response = resp
	return tr
}

func getHost() string {
	return "host.local"
}
