package clickhouse

import (
	"errors"
	"fmt"
	"strconv"
)

func unmarshal(value interface{}, data string) (err error) {
	var m interface{}
	switch v := value.(type) {
	case *int:
		*v, err = strconv.Atoi(data)
		return err
	case *int8:
		m, err = strconv.ParseInt(data, 10, 8)
		*v = m.(int8)
	case *int16:
		m, err = strconv.ParseInt(data, 10, 16)
		*v = m.(int16)
	case *int32:
		m, err = strconv.ParseInt(data, 10, 32)
		*v = m.(int32)
	case *int64:
		*v, err = strconv.ParseInt(data, 10, 64)
	case *string:
		*v = data
	default:
		return errors.New(fmt.Sprintf("Type %T is not supported for unmarshaling", v))
	}

	return err
}
