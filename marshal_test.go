package clickhouse

import "testing"

func BenchmarkMarshalString(b *testing.B) {
	for i := 0; i < b.N; i++ {
		marshal("test")
	}
}