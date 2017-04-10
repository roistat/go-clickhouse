package clickhouse

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"
)

func escape(s string) string {
	s = strings.Replace(s, `\`, `\\`, -1)
	s = strings.Replace(s, `'`, `\'`, -1)
	return s
}

func unescape(s string) string {
	s = strings.Replace(s, `\\`, `\`, -1)
	s = strings.Replace(s, `\'`, `'`, -1)
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
	case *time.Time:
		*v, err = time.ParseInLocation("2006-01-02 15:04:05", data, time.UTC)
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
	if reflect.TypeOf(value).Kind() == reflect.Slice {
		var res []string
		v := reflect.ValueOf(value)
		for i := 0; i < v.Len(); i++ {
			res = append(res, marshal(v.Index(i).Interface()))
		}
		return "[" + strings.Join(res, ",") + "]"
	}
	if t := reflect.TypeOf(value); t.Kind() == reflect.Struct && strings.HasSuffix(t.String(), "Func") {
		return fmt.Sprintf("%s(%v)", value.(Func).Name, marshal(value.(Func).Args))
	}
	switch v := value.(type) {
	case string:
		return fmt.Sprintf("'%s'", escape(v))
	case int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64,
		float32, float64:
		return fmt.Sprintf("%v", v)
	//https://clickhouse.yandex/reference_en.html#Boolean values
	case bool:
		if value.(bool) {
			return "1"
		}
		return "0"
	//Convert time to Date type https://clickhouse.yandex/reference_en.html#Date
	case time.Time:
		return value.(time.Time).Format("2006-01-02")
	}

	return "''"
}
