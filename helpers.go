package buildsqlx

import (
	"errors"
	"reflect"
)

func interfaceToSlice(slice interface{}) ([]interface{}, error) {
	var err error
	s := reflect.ValueOf(slice)
	if s.Kind() != reflect.Slice {
		return nil, errors.New("interfaceToSlice() given a non-slice type")
	}

	ret := make([]interface{}, s.Len())
	for i := 0; i < s.Len(); i++ {
		ret[i] = s.Index(i).Interface()
	}

	return ret, err
}
