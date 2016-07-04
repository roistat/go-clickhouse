package clickhouse

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestErrorFromResponse(t *testing.T) {
	var err *DbError

	assert.NoError(t, errorFromResponse(""))
	assert.NoError(t, errorFromResponse("Ok."))

	err = errorFromResponse("Code: 140, 000000").(*DbError)

	assert.Error(t, err)
	assert.Equal(t, 140, err.Code())
	assert.Equal(t, "", err.Message())

	err = errorFromResponse("Code: 62, e.displayText() = DB::Exception: Syntax error: failed at end of query.\n" +
		"Expected identifier, e.what() = DB::Exception").(*DbError)

	assert.Error(t, err)
	assert.Equal(t, 62, err.Code())
	assert.Equal(t, "clickhouse error: [62] DB::Exception: Syntax error: failed at end of query.\nExpected identifier",
		err.Error())
	assert.Equal(t, "[error code=62 message=\"DB::Exception: Syntax error: failed at end of query.\\nExpected identifier\"]",
		err.String())
	assert.Equal(t, "DB::Exception: Syntax error: failed at end of query.\nExpected identifier", err.Message())

	resp := "Code: 3, e.displayText() = DB::Exception: Syntax error: failed at end of query.\nExpected identifier,"
	err = errorFromResponse(resp).(*DbError)

	assert.Error(t, err)
	assert.Equal(t, 3, err.Code())
	assert.Equal(t, resp, err.Response())
	assert.Equal(t, "DB::Exception: Syntax error: failed at end of query.\nExpected identifier,", err.Message())
}
