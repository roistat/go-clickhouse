package clickhouse

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

func escape(s string) string {
	s = strings.Replace(s, "'", "\\'", -1)
	return s
}

func unescape(s string) string {
	s = strings.Replace(s, "\\'", "'", -1)
	return s
}

func unmarshal(value interface{}, data string) (err error) {
	var m interface{}
	switch v := value.(type) {
	case *int:
		*v, err = strconv.Atoi(data)
		return err
	case *int8:
		m, err = strconv.ParseInt(data, 10, 8)
		*v = int8(m.(int64))
	case *int16:
		m, err = strconv.ParseInt(data, 10, 16)
		*v = int16(m.(int64))
	case *int32:
		m, err = strconv.ParseInt(data, 10, 32)
		*v = int32(m.(int64))
	case *int64:
		*v, err = strconv.ParseInt(data, 10, 64)
	case *float64:
		m, err = strconv.ParseFloat(data, 64)
		*v = m.(float64)
	case *string:
		*v = unescape(data)
	default:
		return errors.New(fmt.Sprintf("Type %T is not supported for unmarshaling", v))
	}

	return err
}

func marshal(value interface{}) string {
	switch value.(type) {
	case int:
		return strconv.Itoa(value.(int))
	case int8:
		return strconv.FormatInt(int64(value.(int8)), 10)
	case int16:
		return strconv.FormatInt(int64(value.(int16)), 10)
	case int32:
		return strconv.FormatInt(int64(value.(int32)), 10)
	case int64:
		return strconv.FormatInt(value.(int64), 10)
	case string:
		return "'"+escape(value.(string))+"'"
	case []string:
		var res []string
		for _, v := range value.([]string) {
			res = append(res, marshal(v))
		}
		return "[" + strings.Join(res, ",") + "]"
	case Array:
		var res []string
		for _, v := range value.(Array) {
			res = append(res, marshal(v))
		}
		return "[" + strings.Join(res, ",") + "]"
	}

	return "''"
}
