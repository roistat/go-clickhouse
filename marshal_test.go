package clickhouse

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func BenchmarkMarshalString(b *testing.B) {
	for i := 0; i < b.N; i++ {
		marshal("test")
	}
}

func TestUnmarshal(t *testing.T) {
	var (
		err            error
		valInt         int
		valInt8        int8
		valInt16       int16
		valInt32       int32
		valInt64       int64
		valString      string
		valUnsupported testing.T
	)

	err = unmarshal(&valInt, "10")
	assert.Equal(t, int(10), valInt)
	assert.NoError(t, err)

	err = unmarshal(&valInt8, "10")
	assert.Equal(t, int8(10), valInt8)
	assert.NoError(t, err)

	err = unmarshal(&valInt16, "10")
	assert.Equal(t, int16(10), valInt16)
	assert.NoError(t, err)

	err = unmarshal(&valInt32, "10")
	assert.Equal(t, int32(10), valInt32)
	assert.NoError(t, err)

	err = unmarshal(&valInt64, "10")
	assert.Equal(t, int64(10), valInt64)
	assert.NoError(t, err)

	err = unmarshal(&valString, "10")
	assert.Equal(t, "10", valString)
	assert.NoError(t, err)

	err = unmarshal(&valUnsupported, "10")
	assert.Error(t, err)
}

func TestMarshal(t *testing.T) {
	assert.Equal(t, "10", marshal(10))
	assert.Equal(t, "10", marshal(int8(10)))
	assert.Equal(t, "10", marshal(int16(10)))
	assert.Equal(t, "10", marshal(int32(10)))
	assert.Equal(t, "10", marshal(int64(10)))
	assert.Equal(t, "'10'", marshal("10"))
	assert.Equal(t, "[10,20,30]", marshal(Array{10, 20, 30}))
	assert.Equal(t, "['k10','20','30val']", marshal(Array{"k10", "20", "30val"}))
	assert.Equal(t, "['k10','20','30val']", marshal([]string{"k10", "20", "30val"}))
	assert.Equal(t, "''", marshal(t))
}
