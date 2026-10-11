package svc

import (
	"testing"
	"yodleeops/storage"

	"yodleeops/storage/fakes"
	"yodleeops/testutil"
	"yodleeops/yodlee"

	"github.com/IBM/sarama"
	saramaMocks "github.com/IBM/sarama/mocks"

	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/stretchr/testify/assert"
)

func setupConsumersTest(t *testing.T) (*State, MockConsumers, *saramaMocks.AsyncProducer) {
	s3Client := testutil.NewVirtualS3Client(t)
	state := &State{S3Client: s3Client, FiMessageBroadcaster: &FiMessageBroadcaster{}}

	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true

	mockProducer := saramaMocks.NewAsyncProducer(t, config)
	state.Producer = mockProducer

	mockConsumer := MockConsumers{t: t, consumers: MakeConsumerList(state)}

	return state, mockConsumer, mockProducer
}

var (
	ProviderAccountRefresh = yodlee.DataExtractsProviderAccount{
		Id:          99,
		LastUpdated: "2025-06-13",
		RequestId:   "REQUEST",
	}

	AccountRefresh = yodlee.DataExtractsAccount{
		ProviderAccountId: 99,
		Id:                999,
		LastUpdated:       "2025-06-13",
		AccountName:       "Savings Data",
	}

	HoldingRefresh = yodlee.DataExtractsHolding{
		AccountId:   999,
		Id:          9999,
		LastUpdated: "2025-06-13",
		HoldingType: "Stock",
	}

	TransactionRefresh = yodlee.DataExtractsTransaction{
		AccountId:   999,
		Id:          9999,
		Date:        "2025-06-13T07:06:18Z",
		CheckNumber: "1299",
	}

	ProviderAccountResponse = yodlee.ProviderAccount{
		Id:          77,
		LastUpdated: "2025-06-13",
		RequestId:   "REQUEST",
	}

	AccountResponse = yodlee.Account{
		ProviderAccountId: 77,
		Id:                777,
		LastUpdated:       "2025-06-13",
		AccountName:       "Savings Data",
	}

	HoldingResponse = yodlee.Holding{
		AccountId:   777,
		Id:          7777,
		LastUpdated: "2025-06-13",
		HoldingType: "Stock",
	}

	TransactionResponse = yodlee.TransactionWithDateTime{
		AccountId:   777,
		Id:          7777,
		Date:        "2025-06-13T07:06:18Z",
		CheckNumber: "1299",
	}
)

var WantBroadcastMsgs = []any{
	BroadcastInput[OpsProviderAccountRefresh, yodlee.DataExtractsProviderAccount]{
		OriginTopic: storage.CnctRefreshTopic,
		FiMessages: []OpsProviderAccountRefresh{
			{
				OpsFiMessage: OpsFiMessage{ProfileId: "p1", OriginTopic: storage.CnctRefreshTopic},
				Data:         ProviderAccountRefresh,
			},
		},
	},
	BroadcastInput[OpsAccountRefresh, yodlee.DataExtractsAccount]{
		OriginTopic: storage.AcctRefreshTopic,
		FiMessages: []OpsAccountRefresh{
			{
				OpsFiMessage: OpsFiMessage{ProfileId: "p1", OriginTopic: storage.AcctRefreshTopic},
				Data:         AccountRefresh,
			},
		},
	},
	BroadcastInput[OpsHoldingRefresh, yodlee.DataExtractsHolding]{
		OriginTopic: storage.HoldRefreshTopic,
		FiMessages: []OpsHoldingRefresh{
			{
				OpsFiMessage: OpsFiMessage{ProfileId: "p1", OriginTopic: storage.HoldRefreshTopic},
				Data:         HoldingRefresh,
			},
		},
	},
	BroadcastInput[OpsTransactionRefresh, yodlee.DataExtractsTransaction]{
		OriginTopic: storage.TxnRefreshTopic,
		FiMessages: []OpsTransactionRefresh{
			{
				OpsFiMessage: OpsFiMessage{ProfileId: "p1", OriginTopic: storage.TxnRefreshTopic},
				Data:         TransactionRefresh,
			},
		},
	},
	BroadcastInput[OpsProviderAccount, yodlee.ProviderAccount]{
		OriginTopic: storage.CnctResponseTopic,
		FiMessages: []OpsProviderAccount{
			{
				OpsFiMessage: OpsFiMessage{ProfileId: "p1", OriginTopic: storage.CnctResponseTopic},
				Data:         ProviderAccountResponse,
			},
		},
	},
	BroadcastInput[OpsAccount, yodlee.Account]{
		OriginTopic: storage.AcctResponseTopic,
		FiMessages: []OpsAccount{
			{
				OpsFiMessage: OpsFiMessage{ProfileId: "p1", OriginTopic: storage.AcctResponseTopic},
				Data:         AccountResponse,
			},
		},
	},
	BroadcastInput[OpsHolding, yodlee.Holding]{
		OriginTopic: storage.HoldResponseTopic,
		FiMessages: []OpsHolding{
			{
				OpsFiMessage: OpsFiMessage{ProfileId: "p1", OriginTopic: storage.HoldResponseTopic},
				Data:         HoldingResponse,
			},
		},
	},
	BroadcastInput[OpsTransaction, yodlee.TransactionWithDateTime]{
		OriginTopic: storage.TxnResponseTopic,
		FiMessages: []OpsTransaction{
			{
				OpsFiMessage: OpsFiMessage{ProfileId: "p1", OriginTopic: storage.TxnResponseTopic},
				Data:         TransactionResponse,
			},
		},
	},
}

func wantIngestedKeys(aws storage.S3Client) []testutil.WantKey {
	return []testutil.WantKey{
		{Bucket: aws.CnctBucket, Key: "p1/1/10/2025-06-12"},
		{Bucket: aws.CnctBucket, Key: "p1/1/10/2025-06-13"},
		{Bucket: aws.CnctBucket, Key: "p1/1/20/2025-06-14"},
		//{Bucket: infra.CnctBucket, Key: "p1/1/30/2025-06-15"},
		{Bucket: aws.CnctBucket, Key: "p1/1/99/2025-06-13"},
		{Bucket: aws.CnctBucket, Key: "p1/1/77/2025-06-13"},

		// AcctBucket
		{Bucket: aws.AcctBucket, Key: "p1/1/10/100/2025-06-12"},
		{Bucket: aws.AcctBucket, Key: "p1/1/10/100/2025-06-13"},
		{Bucket: aws.AcctBucket, Key: "p2/1/20/200/2025-06-14"},
		{Bucket: aws.AcctBucket, Key: "p2/1/30/400/2025-06-15"},
		{Bucket: aws.AcctBucket, Key: "p1/1/99/999/2025-06-13"},
		{Bucket: aws.AcctBucket, Key: "p1/1/77/777/2025-06-13"},

		// HoldBucket
		{Bucket: aws.HoldBucket, Key: "p1/1/100/1000/2025-06-12"},
		{Bucket: aws.HoldBucket, Key: "p1/1/100/1000/2025-06-13"},
		{Bucket: aws.HoldBucket, Key: "p2/1/100/1000/2025-06-14"},
		{Bucket: aws.HoldBucket, Key: "p2/1/200/2000/2025-06-15"},
		{Bucket: aws.HoldBucket, Key: "p1/1/999/9999/2025-06-13"},
		{Bucket: aws.HoldBucket, Key: "p1/1/777/7777/2025-06-13"},

		// TxnBucket
		//{Bucket: aws.TxnBucket, Key: "p1/1/100/3000/2025-06-12T00:14:37Z"},
		//{Bucket: aws.TxnBucket, Key: "p1/1/100/3000/2025-06-12T02:48:09Z"},
		{Bucket: aws.TxnBucket, Key: "p2/1/100/3000/2025-06-13T02:48:09Z"},
		{Bucket: aws.TxnBucket, Key: "p2/1/200/2000/2025-06-14T07:06:18Z"},
		{Bucket: aws.TxnBucket, Key: "p1/1/999/9999/2025-06-13T07:06:18Z"},
		{Bucket: aws.TxnBucket, Key: "p1/1/777/7777/2025-06-13T07:06:18Z"},
	}
}

func TestConsumers(t *testing.T) {
	state, mockConsumers, mockProducer := setupConsumersTest(t)
	defer mockProducer.Close()

	tests := []struct {
		topic storage.Topic
		value any
	}{
		// Refreshes
		{
			topic: storage.CnctRefreshTopic,
			value: []yodlee.DataExtractsProviderAccount{ProviderAccountRefresh},
		},
		{
			topic: storage.AcctRefreshTopic,
			value: []yodlee.DataExtractsAccount{AccountRefresh},
		},
		{
			topic: storage.HoldRefreshTopic,
			value: []yodlee.DataExtractsHolding{HoldingRefresh},
		},
		{
			topic: storage.TxnRefreshTopic,
			value: []yodlee.DataExtractsTransaction{TransactionRefresh},
		},
		// Responses
		{
			topic: storage.CnctResponseTopic,
			value: yodlee.ProviderAccountResponse{ProviderAccount: []yodlee.ProviderAccount{ProviderAccountResponse}},
		},
		{
			topic: storage.AcctResponseTopic,
			value: yodlee.AccountResponse{Account: []yodlee.Account{AccountResponse}},
		},
		{
			topic: storage.HoldResponseTopic,
			value: yodlee.HoldingResponse{Holding: []yodlee.Holding{HoldingResponse}},
		},
		{
			topic: storage.TxnResponseTopic,
			value: yodlee.TransactionResponse{Transaction: []yodlee.TransactionWithDateTime{TransactionResponse}},
		},
		{
			topic: storage.DeleteRetryTopic,
			value: []DeleteRetry{
				{
					Kind:   ListKind,
					Bucket: state.S3Client.TxnBucket,
					Prefix: "p1/1/100/3000",
				},
				{
					Kind:   DeleteKind,
					Bucket: state.S3Client.CnctBucket,
					Keys:   []string{"p1/1/30/2025-06-15"},
				},
			},
		},
	}

	for _, test := range tests {
		// expect one message to be produced per `PutResult` except DeleteRetries
		if _, ok := test.value.([]DeleteRetry); !ok {
			mockProducer.ExpectInputAndSucceed()
		}
		mockConsumers.Consume(test.topic, "p1", test.value)
	}

	broadcastMessages := ConsumeMockProducer(t, mockProducer, len(WantBroadcastMsgs))

	testutil.Equal(t, WantBroadcastMsgs, broadcastMessages, cmpopts.IgnoreFields(OpsFiMessage{}, "Timestamp"))

	// removed keys are commented.
	assert.ElementsMatch(t, wantIngestedKeys(state.S3Client), testutil.GetAllKeys(t, state.S3Client))
}

var (
	ProviderAccountRefreshSlice = []yodlee.DataExtractsProviderAccount{ProviderAccountRefresh}
	AccountRefreshSlice         = []yodlee.DataExtractsAccount{AccountRefresh}
	HoldingRefreshSlice         = []yodlee.DataExtractsHolding{HoldingRefresh}
	TransactionRefreshSlice     = []yodlee.DataExtractsTransaction{TransactionRefresh}

	ProviderAccountResponseSlice = yodlee.ProviderAccountResponse{ProviderAccount: []yodlee.ProviderAccount{ProviderAccountResponse}}
	AccountResponseSlice         = yodlee.AccountResponse{Account: []yodlee.Account{AccountResponse}}
	HoldingResponseSlice         = yodlee.HoldingResponse{Holding: []yodlee.Holding{HoldingResponse}}
	TransactionResponseSlice     = yodlee.TransactionResponse{Transaction: []yodlee.TransactionWithDateTime{TransactionResponse}}
)

var WantPutRetryMessages = []any{
	ProviderAccountResponseSlice,
	AccountResponseSlice,
	HoldingResponseSlice,
	TransactionResponseSlice,
	ProviderAccountRefreshSlice,
	AccountRefreshSlice,
	HoldingRefreshSlice,
	TransactionRefreshSlice,
}

func TestConsumers_S3Errors(t *testing.T) {
	state, mockConsumers, mockProducer := setupConsumersTest(t)
	defer mockProducer.Close()

	key := "p1" // all messages for same profileId.

	tests := []struct {
		topic      storage.Topic
		failPutKey string
		value      any
	}{
		// Refreshes
		{
			topic:      storage.CnctRefreshTopic,
			failPutKey: "p1/1/99/2025-06-13",
			value:      ProviderAccountRefreshSlice,
		},
		{
			topic:      storage.AcctRefreshTopic,
			failPutKey: "p1/1/99/999/2025-06-13",
			value:      AccountRefreshSlice,
		},
		{
			topic:      storage.HoldRefreshTopic,
			failPutKey: "p1/1/999/9999/2025-06-13",
			value:      HoldingRefreshSlice,
		},
		{
			topic:      storage.TxnRefreshTopic,
			failPutKey: "p1/1/999/9999/2025-06-13T07:06:18Z",
			value:      TransactionRefreshSlice,
		},
		// Responses
		{
			topic:      storage.CnctResponseTopic,
			failPutKey: "p1/1/77/2025-06-13",
			value:      ProviderAccountResponseSlice,
		},
		{
			topic:      storage.AcctResponseTopic,
			failPutKey: "p1/1/77/777/2025-06-13",
			value:      AccountResponseSlice,
		},
		{
			topic:      storage.HoldResponseTopic,
			failPutKey: "p1/1/777/7777/2025-06-13",
			value:      HoldingResponseSlice,
		},
		{
			topic:      storage.TxnResponseTopic,
			failPutKey: "p1/1/777/7777/2025-06-13T07:06:18Z",
			value:      TransactionResponseSlice,
		},
	}

	for _, test := range tests {
		fakes.SetBadS3Client(&state.S3Client, fakes.BadS3Config{
			FailPutKey: test.failPutKey,
		})

		// expect one message to be produced per `PutResult`
		mockProducer.ExpectInputAndSucceed()

		mockConsumers.Consume(test.topic, key, test.value)
	}

	messages := ConsumeMockProducer(t, mockProducer, len(WantPutRetryMessages))

	assert.ElementsMatch(t, WantPutRetryMessages, messages)
}
