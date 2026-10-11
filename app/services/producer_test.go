package svc

import (
	"errors"
	"testing"
	"yodleeops/storage"
	"yodleeops/testutil"
	"yodleeops/yodlee"

	"github.com/IBM/sarama"
	saramaMocks "github.com/IBM/sarama/mocks"
	"github.com/google/go-cmp/cmp/cmpopts"

	"github.com/stretchr/testify/assert"
)

func setupProducerTest(t *testing.T) (Context, *saramaMocks.AsyncProducer) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true

	mockProducer := saramaMocks.NewAsyncProducer(t, config)
	return Context{Context: t.Context(), State: &State{Producer: mockProducer}}, mockProducer
}

func TestProduceDeleteErrors(t *testing.T) {
	ctx, mockProducer := setupProducerTest(t)

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

	for range deleteErrs {
		mockProducer.ExpectInputAndSucceed()
	}
	go func() {
		defer mockProducer.Close()
		ProduceDeleteErrors(ctx, "p1", deleteErrs)
	}()

	messages := getMockProducerMessages(mockProducer)

	// (echo assert)
	wantMessages := []any{
		DeleteResult{Bucket: "Bucket", Keys: []string{"key1", "key2"}},
		DeleteResult{Bucket: "Bucket", Prefix: "prefix"},
	}
	assert.ElementsMatch(t, wantMessages, messages)
}

func TestProducePutResults(t *testing.T) {
	ctx, mockProducer := setupProducerTest(t)

	putResults := []PutResult[OpsProviderAccountRefresh]{
		{
			Key: "p1/1/1/2025-06-12",
			Input: OpsProviderAccountRefresh{
				OpsFiMessage: OpsFiMessage{ProfileId: "p1", OriginTopic: storage.CnctRefreshTopic},
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
				OpsFiMessage: OpsFiMessage{ProfileId: "p1", OriginTopic: storage.CnctRefreshTopic},
				Data: yodlee.DataExtractsProviderAccount{
					Id:          90,
					LastUpdated: "2025-06-13",
					RequestId:   "REQUEST",
				},
			},
		},
	}

	for range putResults {
		mockProducer.ExpectInputAndSucceed()
	}
	go func() {
		defer mockProducer.Close()
		ProducePutResults(ctx, storage.CnctRefreshTopic, "p1", putResults, nil)
	}()

	messages := getMockProducerMessages(mockProducer)

	wantMessages := []any{
		BroadcastInput[OpsProviderAccountRefresh, yodlee.DataExtractsProviderAccount]{
			OriginTopic: storage.CnctRefreshTopic,
			FiMessages: []OpsProviderAccountRefresh{
				{
					OpsFiMessage: OpsFiMessage{ProfileId: "p1", OriginTopic: storage.CnctRefreshTopic},
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
	testutil.Equal(t, wantMessages, messages, cmpopts.IgnoreFields(OpsFiMessage{}, "Timestamp"))
}
