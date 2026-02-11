package svc

import (
	"encoding/json"
)

func unmarshalJsonMono[JSON any](value []byte) any {
	var v JSON
	err := json.Unmarshal(value, &v)
	if err != nil {
		return err.Error()
	}
	return v
}
