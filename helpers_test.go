package clickhouse

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestNewHttpTransport(t *testing.T) {
	tr := NewHttpTransport()
	assert.IsType(t, HttpTransport{}, tr)
}

func TestNewQuery(t *testing.T) {
	stmt := "SELECT * FROM table WHERE ?"
	q := NewQuery(stmt, 1)
	assert.Equal(t, stmt, q.Stmt)
	assert.Equal(t, []interface{}{1}, q.args)
}

func TestBuildInsert(t *testing.T) {
	var (
		q   Query
		err error
	)

	q, err = BuildInsert("test", Columns{"col1", "col2"}, Row{"val1", "val2"})
	assert.Equal(t, "INSERT INTO test (col1,col2) VALUES (?,?)", q.Stmt)
	assert.Equal(t, []interface{}{"val1", "val2"}, q.args)
	assert.NoError(t, err)

	q, err = BuildInsert("test", Columns{"col1", "col2"}, Row{"val1"})
	assert.Equal(t, "", q.Stmt)
	assert.Error(t, err)
}

func TestBuildInsertArray(t *testing.T) {
	var (
		q   Query
		err error
	)

	q, err = BuildInsert("test", Columns{"col1", "col2"}, Row{"val1", Array{"val2", "val3"}})
	assert.Equal(t, "INSERT INTO test (col1,col2) VALUES (?,?)", q.Stmt)
	assert.Equal(t, []interface{}{"val1", Array{"val2", "val3"}}, q.args)
	assert.NoError(t, err)
}

func TestNewMultiInsert(t *testing.T) {
	var (
		q   Query
		err error
	)

	q, err = BuildMultiInsert("test", Columns{"col1", "col2"}, Rows{
		Row{"val1", "val2"},
		Row{"val3", "val4"},
	})
	assert.Equal(t, "INSERT INTO test (col1,col2) VALUES (?,?),(?,?)", q.Stmt)
	assert.Equal(t, []interface{}{"val1", "val2", "val3", "val4"}, q.args)
	assert.NoError(t, err)

	q, err = BuildMultiInsert("test", Columns{"col1", "col2"}, Rows{
		Row{"val1", "val2"},
		Row{"val3"},
	})
	assert.Equal(t, "", q.Stmt)
	assert.Error(t, err)
}

func BenchmarkNewInsert(b *testing.B) {
	for i := 0; i < b.N; i++ {
		BuildInsert("test", Columns{"col1", "col2"}, Row{"val1", "val2"})
	}
}

func getRows(n int, r Row) Rows {
	res := make(Rows, n)
	for i := 0; i < n; i++ {
		res[i] = r
	}
	return res
}

func BenchmarkNewMultiInsert100(b *testing.B) {
	columns := Columns{"col1", "col2", "col3", "col4"}
	rows := getRows(100, Row{"val1", "val2", "val3", "val4"})
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		BuildMultiInsert("test", columns, rows)
	}
}

func BenchmarkNewMultiInsert1000(b *testing.B) {
	columns := Columns{"col1", "col2", "col3", "col4"}
	rows := getRows(1000, Row{"val1", "val2", "val3", "val4"})
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		BuildMultiInsert("test", columns, rows)
	}
}
