package clickhouse

import (
	"errors"
	"github.com/stretchr/testify/assert"
	"testing"
)

type mockTransport struct {
	response string
}

type badTransport struct {
	response string
	err      error
}

func (m mockTransport) Exec(conn *Conn, q Query, readOnly bool) (r string, err error) {
	return m.response, nil
}

func (m badTransport) Exec(conn *Conn, q Query, readOnly bool) (r string, err error) {
	return "", m.err
}

func TestQuery_Iter(t *testing.T) {
	tr := getMockTransport("Code: 62, ")
	conn := NewConn(getHost(), tr)
	iter := NewQuery("SELECT 1").Iter(conn)
	assert.Error(t, iter.Error())
	assert.Equal(t, 62, iter.Error().(*DbError).Code())
}

func TestQuery_Iter2(t *testing.T) {
	tr := badTransport{err: errors.New("No connection")}
	conn := NewConn(getHost(), tr)
	iter := NewQuery("SELECT 1").Iter(conn)
	assert.Error(t, iter.Error())
	assert.Equal(t, "No connection", iter.Error().Error())
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

func TestIter_ScanErrors(t *testing.T) {
	tr := getMockTransport("test1\ttest2\ntest3\ttest4")
	conn := NewConn(getHost(), tr)

	iter := NewQuery("SELECT 'test1', 'test2'").Iter(conn)
	var v1, v2, v3 string
	scan := iter.Scan(&v1, &v2, &v3)
	assert.False(t, scan)
	assert.NoError(t, iter.Error())

	var u1 Conn
	scan = iter.Scan(&u1)
	assert.False(t, scan)
	assert.Error(t, iter.Error())

	tr = getMockTransport("")
	conn = NewConn(getHost(), tr)

	iter = NewQuery("SELECT 'test1', 'test2'").Iter(conn)
	scan = iter.Scan(&u1)
	assert.False(t, scan)
	assert.NoError(t, iter.Error())
}

func TestQuery_Exec(t *testing.T) {
	tr := getMockTransport("")
	conn := NewConn(getHost(), tr)

	err := NewQuery("INSERT INTO table VALUES 1").Exec(conn)
	assert.NoError(t, err)

	tr = getMockTransport("Code: 69, ")
	conn = NewConn(getHost(), tr)

	err = NewQuery("INSERT INTO table VALUES 1").Exec(conn)
	assert.Error(t, err)
	assert.Equal(t, 69, err.(*DbError).Code())
}

func TestQuery_Exec2(t *testing.T) {
	err := NewQuery("SELECT 1").Exec(nil)
	assert.Error(t, err)
}

func TestQuery_Iter3(t *testing.T) {
	iter := NewQuery("INSERT 1").Iter(nil)
	assert.Error(t, iter.err)
}

func getMockTransport(resp string) mockTransport {
	tr := mockTransport{}
	tr.response = resp
	return tr
}

func getHost() string {
	return "host.local"
}
