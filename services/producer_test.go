package svc

import (
	"errors"
	"testing"

	"yodleeops/infra"

	"github.com/stretchr/testify/assert"
)

func TestProduceDeleteErrors(t *testing.T) {
	// given
	deleteErrs := []DeleteResult{
		{
			Bucket: "Bucket",
			Keys:   []string{"key1", "key2"},
			Err:    errors.New("err1"),
		},
		{
			Bucket: "Bucket",
			Prefix: "prefix",
			Err:    errors.New("err1"),
		},
	}

	// when
	msgs := MakeDeleteErrorsMsgs(t.Context(), "p1", deleteErrs)

	// then
	wantDeleteResults := []DeleteErrorMsg{
		{
			Key:   "p1",
			Topic: infra.DeleteRecoveryTopic,
			Value: DeleteRetry{
				Kind:   "delete",
				Bucket: "Bucket",
				Keys:   []string{"key1", "key2"},
			},
		},
		{
			Key:   "p1",
			Topic: infra.DeleteRecoveryTopic,
			Value: DeleteRetry{
				Kind:   "list",
				Bucket: "Bucket",
				Prefix: "prefix",
			},
		},
	}
	assert.ElementsMatch(t, wantDeleteResults, msgs)
}
