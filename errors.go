package clickhouse

import (
	"fmt"
	"regexp"
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
	errorPattern, err := regexp.Compile(`Code:\s(\d+)[.,]?(.*)`)
	if err != nil {
		return err
	}
	if !errorPattern.MatchString(resp) {
		return nil
	}

	matches := errorPattern.FindStringSubmatch(resp)
	code, err := strconv.Atoi(matches[1])
	if err != nil {
		return err
	}
	msg := matches[2]
	msg = strings.ReplaceAll(msg, "e.displayText() = ", "")
	msg = strings.ReplaceAll(msg, ", e.what()", "")
	msg = strings.TrimSpace(msg)
	return &DbError{code, msg, resp}
}
