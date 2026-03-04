package svc

import (
	"errors"
	"github.com/IBM/sarama"
	saramaMocks "github.com/IBM/sarama/mocks"
	"github.com/google/go-cmp/cmp/cmpopts"
	"testing"
	"yodleeops/testutil"
	"yodleeops/yodlee"

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
	wantDeleteResults := []JsonMessage{
		{
			Key:   "p1",
			Topic: infra.DeleteRetryTopic,
			Value: DeleteRetry{
				Kind:   "delete",
				Bucket: "Bucket",
				Keys:   []string{"key1", "key2"},
			},
		},
		{
			Key:   "p1",
			Topic: infra.DeleteRetryTopic,
			Value: DeleteRetry{
				Kind:   "list",
				Bucket: "Bucket",
				Prefix: "prefix",
			},
		},
	}
	assert.ElementsMatch(t, wantDeleteResults, msgs)
}

func TestProducePutResults(t *testing.T) {
	// given
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true

	mockProducer := saramaMocks.NewAsyncProducer(t, config)
	appCtx := Context{Context: t.Context(), App: &App{Producer: mockProducer}}

	putResults := []PutResult[OpsProviderAccountRefresh]{
		{
			Key: "p1/1/1/2025-06-12",
			Input: OpsProviderAccountRefresh{
				OpsFiMessage: OpsFiMessage{ProfileId: "p1", OriginTopic: infra.CnctRefreshTopic},
				Data: yodlee.DataExtractsProviderAccount{
					Id:          90,
					LastUpdated: "2025-06-12",
					RequestId:   "REQUEST",
				},
			},
		},
		{
			Err: errors.New("error"),
			Key: "p1/1/1/2025-06-13",
			Input: OpsProviderAccountRefresh{
				OpsFiMessage: OpsFiMessage{ProfileId: "p1", OriginTopic: infra.CnctRefreshTopic},
				Data: yodlee.DataExtractsProviderAccount{
					Id:          90,
					LastUpdated: "2025-06-13",
					RequestId:   "REQUEST",
				},
			},
		},
	}

	// when
	for range putResults {
		// expect one message to be produced per `PutResult`
		mockProducer.ExpectInputAndSucceed()
	}
	go func() {
		defer mockProducer.Close()
		ProducePutResults(appCtx, infra.CnctRefreshTopic, "p1", putResults, nil)
	}()

	// then
	msgs := drainMockProducerMessages(mockProducer)

	wantMsgs := []any{
		BroadcastInput[OpsProviderAccountRefresh, yodlee.DataExtractsProviderAccount]{
			OriginTopic: infra.CnctRefreshTopic,
			FiMessages: []OpsProviderAccountRefresh{
				{
					OpsFiMessage: OpsFiMessage{ProfileId: "p1", OriginTopic: infra.CnctRefreshTopic},
					Data: yodlee.DataExtractsProviderAccount{
						Id:          90,
						LastUpdated: "2025-06-12",
						RequestId:   "REQUEST",
					},
				},
			},
		},
		[]yodlee.DataExtractsProviderAccount{{LastUpdated: "2025-06-13", RequestId: "REQUEST", Id: 90}},
	}
	testutil.Equal(t, wantMsgs, msgs, cmpopts.IgnoreFields(OpsFiMessage{}, "Timestamp"))
}
