package clickhouse

import (
	"testing"
	"github.com/stretchr/testify/assert"
	"strings"
)

func TestPrepareHttp(t *testing.T) {
	p := prepareHttp("SELECT * FROM table WHERE key = ?", []interface{}{"test"})
	assert.Equal(t, "SELECT * FROM table WHERE key = 'test'", p)
}

func BenchmarkPrepareHttp(b *testing.B) {
	params := strings.Repeat("(?,?,?,?,?,?,?,?)", 1000)
	args := make([]interface{}, 1000)
	for i := 0; i < 1000; i++ {
		args = append(args, []interface{}{
			"test",
			"test",
			"test",
			"test",
			"test",
			"test",
			"test",
			"test",
		})
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		prepareHttp("INSERT INTO t VALUES "+params, args)
	}
}

func BenchmarkPrepareHttpNew(b *testing.B) {
	params := strings.Repeat("(?,?,?,?,?,?,?,?)", 1000)
	args := make([]interface{}, 8000)
	for i := 0; i < 8000; i++ {
		args[i] = "test"
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		prepareHttpNew("INSERT INTO t VALUES "+params, args)
	}
}
