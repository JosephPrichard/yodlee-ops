package svc

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGzipJSONEcho(t *testing.T) {
	type echoStruct struct {
		Name string   `json:"name"`
		Age  int      `json:"age"`
		Tags []string `json:"tags"`
	}

	original := echoStruct{
		Name: "joe",
		Age:  30,
		Tags: []string{"a", "b"},
	}

	encoded, ok := EncodeGzipJSON(nil, original)
	require.True(t, ok)
	assert.NotNil(t, encoded)

	var decoded echoStruct
	require.NoError(t, DecodeGzipJSON(bytes.NewReader(encoded), &decoded))

	assert.Equal(t, original, decoded)
}
