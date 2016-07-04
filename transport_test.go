package clickhouse

import (
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
)

func TestPrepareHttp(t *testing.T) {
	p := prepareHttp("SELECT * FROM table WHERE key = ?", []interface{}{"test"})
	assert.Equal(t, "SELECT * FROM table WHERE key = 'test'", p)
}

func TestPrepareHttpArray(t *testing.T) {
	p := prepareHttp("INSERT INTO table (arr) VALUES (?)", Row{Array{"val1", "val2"}})
	assert.Equal(t, "INSERT INTO table (arr) VALUES (['val1','val2'])", p)
}

func BenchmarkPrepareHttp(b *testing.B) {
	params := strings.Repeat("(?,?,?,?,?,?,?,?)", 1000)
	args := make([]interface{}, 8000)
	for i := 0; i < 8000; i++ {
		args[i] = "test"
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		prepareHttp("INSERT INTO t VALUES "+params, args)
	}
}
