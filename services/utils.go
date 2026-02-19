package svc

import (
	"encoding/json"
	"errors"
)

func unmarshalJsonMono[JSON any](value []byte) any {
	var v JSON
	err := json.Unmarshal(value, &v)
	if err != nil {
		return err.Error()
	}
	return v
}

func errorsAsType[T any](err error) bool {
	var errValue T
	return errors.As(err, &errValue)
}
