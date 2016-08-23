package clickhouse

import (
	"fmt"
	"strconv"
	"strings"
)

func escape(s string) string {
	s = strings.Replace(s, "\\", "\\\\", -1)
	s = strings.Replace(s, "'", "\\'", -1)
	return s
}

func unescape(s string) string {
	s = strings.Replace(s, "\\\\", "\\", -1)
	s = strings.Replace(s, "\\'", "'", -1)
	return s
}

func isArray(s string) bool {
	return strings.HasPrefix(s, "[") && strings.HasSuffix(s, "]")
}

func isEmptyArray(s string) bool {
	return s == "[]"
}

func splitStringToItems(s string) []string {
	return strings.Split(string(s[1:len(s)-1]), ",")
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
	case *float32:
		m, err = strconv.ParseFloat(data, 32)
		*v = float32(m.(float64))
	case *float64:
		m, err = strconv.ParseFloat(data, 64)
		*v = m.(float64)
	case *string:
		*v = unescape(data)
	case *[]int:
		if !isArray(data) {
			//noinspection GoPlaceholderCount
			return fmt.Errorf("Column data is not of type []int")
		}
		if isEmptyArray(data) {
			*v = []int{}
			return
		}

		items := splitStringToItems(data)
		res := make([]int, len(items))
		for i := 0; i < len(items); i++ {
			unmarshal(&res[i], items[i])
		}

		*v = res
	case *[]string:
		if !isArray(data) {
			//noinspection GoPlaceholderCount
			return fmt.Errorf("Column data is not of type []string")
		}
		if isEmptyArray(data) {
			*v = []string{}
			return
		}

		items := splitStringToItems(data)
		res := make([]string, len(items))
		for i := 0; i < len(items); i++ {
			var s string
			unmarshal(&s, items[i])
			res[i] = string(s[1 : len(s)-1])
		}

		*v = res
	case *Array:
		if !isArray(data) {
			//noinspection GoPlaceholderCount
			return fmt.Errorf("Column data is not of type Array")
		}
		if isEmptyArray(data) {
			*v = Array{}
			return
		}

		items := splitStringToItems(data)
		res := make(Array, len(items))

		var intval int
		err = unmarshal(&intval, items[0])
		if err == nil {
			for i := 0; i < len(items); i++ {
				unmarshal(&intval, items[i])
				res[i] = intval
			}

			*v = res
			return
		}

		var floatval float64
		err = unmarshal(&floatval, items[0])
		if err == nil {
			for i := 0; i < len(items); i++ {
				unmarshal(&floatval, items[i])
				res[i] = floatval
			}

			*v = res
			return
		}

		var stringval string
		err = unmarshal(&stringval, items[0])
		if err == nil {
			for i := 0; i < len(items); i++ {
				unmarshal(&stringval, items[i])
				res[i] = string(stringval[1 : len(stringval)-1])
			}

			*v = res
			return
		}
	default:
		return fmt.Errorf("Type %T is not supported for unmarshaling", v)
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
	case float32:
		return strconv.FormatFloat(float64(value.(float32)), 'f', -1, 32)
	case float64:
		return strconv.FormatFloat(value.(float64), 'f', -1, 64)
	case string:
		return "'" + escape(value.(string)) + "'"
	case []int:
		var res []string
		for _, v := range value.([]int) {
			res = append(res, marshal(v))
		}
		return "[" + strings.Join(res, ",") + "]"
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
