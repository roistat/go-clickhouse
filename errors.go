package clickhouse

import (
	"fmt"
	"strconv"
	"strings"
)

type DbError struct {
	code int
	msg  string
	resp string
}

func (e *DbError) Code() int {
	return e.code
}

func (e *DbError) Message() string {
	return e.msg
}

func (e *DbError) Response() string {
	return e.resp
}

func (e *DbError) Error() string {
	return fmt.Sprintf("clickhouse error: [%d] %s", e.code, e.msg)
}

func (e *DbError) String() string {
	return fmt.Sprintf("[error code=%d message=%q]", e.code, e.msg)
}

func errorFromResponse(resp string) error {
	if resp == "" {
		return nil
	}

	if strings.Index(resp, "Code:") == 0 {
		codeStr := resp[6:strings.Index(resp, ",")]
		code, _ := strconv.Atoi(codeStr)
		var msg string
		msgIndex := strings.Index(resp, "e.displayText() = ")
		if msgIndex >= 0 {
			msgIndex += 18
			msgEnd := strings.Index(resp, ", e.what()")
			if msgEnd >= 0 {
				msg = resp[msgIndex:msgEnd]
			} else {
				msg = resp[msgIndex:]
			}
		}
		return &DbError{code, msg, resp}
	}

	return nil
}
