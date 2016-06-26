package clickhouse

import (
	"testing"
	"github.com/stretchr/testify/assert"
)

func TestErrorFromResponse(t *testing.T) {
	assert.NoError(t, errorFromResponse(""))
	assert.NoError(t, errorFromResponse("Ok."))
	assert.Error(t, errorFromResponse("Code: 10, Exception: test"))
}