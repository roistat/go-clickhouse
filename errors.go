package clickhouse

import (
	"errors"
	"strings"
)

func errorFromResponse(resp string) error {
	if resp == "" {
		return nil
	}

	if strings.Contains(resp, "Code:") {
		return errors.New(resp)
	}

	return nil
}
