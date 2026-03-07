package svc

import (
	"encoding/json"
	"github.com/IBM/sarama"
	saramaMocks "github.com/IBM/sarama/mocks"
	"github.com/stretchr/testify/require"
	"testing"

	"yodleeops/infra"
	"yodleeops/infra/fakes"
	"yodleeops/testutil"
	"yodleeops/yodlee"

	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/stretchr/testify/assert"
)

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

func setupConsumersTest(t *testing.T) *State {
	awsClient := testutil.SetupITest(t)
	state := &State{AWS: awsClient}
	return state
}

type MockConsumers struct {
	t         *testing.T
	consumers map[infra.Topic]Consumer
}

func (p *MockConsumers) ConsumeClaim(topic infra.Topic, key string, value any) {
	v, err := json.Marshal(value)
	require.NoError(p.t, err)

	session := &fakeSession{}
	claim := &fakeClaim{
		msgChan: make(chan *sarama.ConsumerMessage, 1),
	}

	msg := &sarama.ConsumerMessage{
		Key:   []byte(key),
		Value: v,
	}
	claim.msgChan <- msg
	close(claim.msgChan)

	err = p.consumers[topic].Handler.ConsumeClaim(session, claim)
	require.NoError(p.t, err)
}

var WantBroadcastMsgs = []any{
	BroadcastInput[OpsProviderAccountRefresh, yodlee.DataExtractsProviderAccount]{
		OriginTopic: infra.CnctRefreshTopic,
		FiMessages: []OpsProviderAccountRefresh{
			{
				OpsFiMessage: OpsFiMessage{ProfileId: "p1", OriginTopic: infra.CnctRefreshTopic},
				Data:         ProviderAccountRefresh,
			},
		},
	},
	BroadcastInput[OpsAccountRefresh, yodlee.DataExtractsAccount]{
		OriginTopic: infra.AcctRefreshTopic,
		FiMessages: []OpsAccountRefresh{
			{
				OpsFiMessage: OpsFiMessage{ProfileId: "p1", OriginTopic: infra.AcctRefreshTopic},
				Data:         AccountRefresh,
			},
		},
	},
	BroadcastInput[OpsHoldingRefresh, yodlee.DataExtractsHolding]{
		OriginTopic: infra.HoldRefreshTopic,
		FiMessages: []OpsHoldingRefresh{
			{
				OpsFiMessage: OpsFiMessage{ProfileId: "p1", OriginTopic: infra.HoldRefreshTopic},
				Data:         HoldingRefresh,
			},
		},
	},
	BroadcastInput[OpsTransactionRefresh, yodlee.DataExtractsTransaction]{
		OriginTopic: infra.TxnRefreshTopic,
		FiMessages: []OpsTransactionRefresh{
			{
				OpsFiMessage: OpsFiMessage{ProfileId: "p1", OriginTopic: infra.TxnRefreshTopic},
				Data:         TransactionRefresh,
			},
		},
	},
	BroadcastInput[OpsProviderAccount, yodlee.ProviderAccount]{
		OriginTopic: infra.CnctResponseTopic,
		FiMessages: []OpsProviderAccount{
			{
				OpsFiMessage: OpsFiMessage{ProfileId: "p1", OriginTopic: infra.CnctResponseTopic},
				Data:         ProviderAccountResponse,
			},
		},
	},
	BroadcastInput[OpsAccount, yodlee.Account]{
		OriginTopic: infra.AcctResponseTopic,
		FiMessages: []OpsAccount{
			{
				OpsFiMessage: OpsFiMessage{ProfileId: "p1", OriginTopic: infra.AcctResponseTopic},
				Data:         AccountResponse,
			},
		},
	},
	BroadcastInput[OpsHolding, yodlee.Holding]{
		OriginTopic: infra.HoldResponseTopic,
		FiMessages: []OpsHolding{
			{
				OpsFiMessage: OpsFiMessage{ProfileId: "p1", OriginTopic: infra.HoldResponseTopic},
				Data:         HoldingResponse,
			},
		},
	},
	BroadcastInput[OpsTransaction, yodlee.TransactionWithDateTime]{
		OriginTopic: infra.TxnResponseTopic,
		FiMessages: []OpsTransaction{
			{
				OpsFiMessage: OpsFiMessage{ProfileId: "p1", OriginTopic: infra.TxnResponseTopic},
				Data:         TransactionResponse,
			},
		},
	},
}

var WantIngestedKeys = []testutil.WantKey{
	{Bucket: infra.CnctBucket, Key: "p1/1/10/2025-06-12"},
	{Bucket: infra.CnctBucket, Key: "p1/1/10/2025-06-13"},
	{Bucket: infra.CnctBucket, Key: "p1/1/20/2025-06-14"},
	//{Bucket: infra.CnctBucket, Key: "p1/1/30/2025-06-15"},
	{Bucket: infra.CnctBucket, Key: "p1/1/99/2025-06-13"},
	{Bucket: infra.CnctBucket, Key: "p1/1/77/2025-06-13"},

	// Accounts
	{Bucket: infra.AcctBucket, Key: "p1/1/10/100/2025-06-12"},
	{Bucket: infra.AcctBucket, Key: "p1/1/10/100/2025-06-13"},
	{Bucket: infra.AcctBucket, Key: "p2/1/20/200/2025-06-14"},
	{Bucket: infra.AcctBucket, Key: "p2/1/30/400/2025-06-15"},
	{Bucket: infra.AcctBucket, Key: "p1/1/99/999/2025-06-13"},
	{Bucket: infra.AcctBucket, Key: "p1/1/77/777/2025-06-13"},

	// Holdings
	{Bucket: infra.HoldBucket, Key: "p1/1/100/1000/2025-06-12"},
	{Bucket: infra.HoldBucket, Key: "p1/1/100/1000/2025-06-13"},
	{Bucket: infra.HoldBucket, Key: "p2/1/100/1000/2025-06-14"},
	{Bucket: infra.HoldBucket, Key: "p2/1/200/2000/2025-06-15"},
	{Bucket: infra.HoldBucket, Key: "p1/1/999/9999/2025-06-13"},
	{Bucket: infra.HoldBucket, Key: "p1/1/777/7777/2025-06-13"},

	// Transactions
	//{Bucket: infra.TxnBucket, Key: "p1/1/100/3000/2025-06-12T00:14:37Z"},
	//{Bucket: infra.TxnBucket, Key: "p1/1/100/3000/2025-06-12T02:48:09Z"},
	{Bucket: infra.TxnBucket, Key: "p2/1/100/3000/2025-06-13T02:48:09Z"},
	{Bucket: infra.TxnBucket, Key: "p2/1/200/2000/2025-06-14T07:06:18Z"},
	{Bucket: infra.TxnBucket, Key: "p1/1/999/9999/2025-06-13T07:06:18Z"},
	{Bucket: infra.TxnBucket, Key: "p1/1/777/7777/2025-06-13T07:06:18Z"},
}

func TestConsumers(t *testing.T) {
	// given
	state := setupConsumersTest(t)

	mockConsumer := MockConsumers{t: t, consumers: MakeConsumers(state)}

	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true

	mockProducer := saramaMocks.NewAsyncProducer(t, config)
	state.Producer = mockProducer

	// when
	tests := []struct {
		topic infra.Topic
		value any
	}{
		// Refreshes
		{
			topic: infra.CnctRefreshTopic,
			value: []yodlee.DataExtractsProviderAccount{ProviderAccountRefresh},
		},
		{
			topic: infra.AcctRefreshTopic,
			value: []yodlee.DataExtractsAccount{AccountRefresh},
		},
		{
			topic: infra.HoldRefreshTopic,
			value: []yodlee.DataExtractsHolding{HoldingRefresh},
		},
		{
			topic: infra.TxnRefreshTopic,
			value: []yodlee.DataExtractsTransaction{TransactionRefresh},
		},
		// Responses
		{
			topic: infra.CnctResponseTopic,
			value: yodlee.ProviderAccountResponse{ProviderAccount: []yodlee.ProviderAccount{ProviderAccountResponse}},
		},
		{
			topic: infra.AcctResponseTopic,
			value: yodlee.AccountResponse{Account: []yodlee.Account{AccountResponse}},
		},
		{
			topic: infra.HoldResponseTopic,
			value: yodlee.HoldingResponse{Holding: []yodlee.Holding{HoldingResponse}},
		},
		{
			topic: infra.TxnResponseTopic,
			value: yodlee.TransactionResponse{Transaction: []yodlee.TransactionWithDateTime{TransactionResponse}},
		},
		{
			topic: infra.DeleteRetryTopic,
			value: []DeleteRetry{
				{
					Kind:   ListKind,
					Bucket: infra.TxnBucket,
					Prefix: "p1/1/100/3000",
				},
				{
					Kind:   DeleteKind,
					Bucket: infra.CnctBucket,
					Keys:   []string{"p1/1/30/2025-06-15"},
				},
			},
		},
	}
	go func() {
		defer mockProducer.Close()

		for _, test := range tests {
			// expect one message to be produced per `PutResult` except DeleteRetries
			if _, ok := test.value.([]DeleteRetry); !ok {
				mockProducer.ExpectInputAndSucceed()
			}
			mockConsumer.ConsumeClaim(test.topic, "p1", test.value)
		}
	}()

	// then
	broadcastMsgs := drainMockProducerMessages(mockProducer)
	testutil.Equal(t, WantBroadcastMsgs, broadcastMsgs, cmpopts.IgnoreFields(OpsFiMessage{}, "Timestamp"))

	// removed keys are commented.
	assert.ElementsMatch(t, WantIngestedKeys, testutil.GetAllKeys(t, state.AWS))
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

var WantPutRetryMsgs = []any{
	ProviderAccountResponseSlice, AccountResponseSlice, HoldingResponseSlice, TransactionResponseSlice,
	ProviderAccountRefreshSlice, AccountRefreshSlice, HoldingRefreshSlice, TransactionRefreshSlice,
}

func TestConsumers_S3Errors(t *testing.T) {
	// given
	state := setupConsumersTest(t)

	mockConsumer := MockConsumers{t: t, consumers: MakeConsumers(state)}

	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true

	mockProducer := saramaMocks.NewAsyncProducer(t, config)
	state.Producer = mockProducer

	key := "p1" // all messages for same profileId.

	// when
	tests := []struct {
		topic      infra.Topic
		failPutKey string
		value      any
	}{
		// Refreshes
		{
			topic:      infra.CnctRefreshTopic,
			failPutKey: "p1/1/99/2025-06-13",
			value:      ProviderAccountRefreshSlice,
		},
		{
			topic:      infra.AcctRefreshTopic,
			failPutKey: "p1/1/99/999/2025-06-13",
			value:      AccountRefreshSlice,
		},
		{
			topic:      infra.HoldRefreshTopic,
			failPutKey: "p1/1/999/9999/2025-06-13",
			value:      HoldingRefreshSlice,
		},
		{
			topic:      infra.TxnRefreshTopic,
			failPutKey: "p1/1/999/9999/2025-06-13T07:06:18Z",
			value:      TransactionRefreshSlice,
		},
		// Responses
		{
			topic:      infra.CnctResponseTopic,
			failPutKey: "p1/1/77/2025-06-13",
			value:      ProviderAccountResponseSlice,
		},
		{
			topic:      infra.AcctResponseTopic,
			failPutKey: "p1/1/77/777/2025-06-13",
			value:      AccountResponseSlice,
		},
		{
			topic:      infra.HoldResponseTopic,
			failPutKey: "p1/1/777/7777/2025-06-13",
			value:      HoldingResponseSlice,
		},
		{
			topic:      infra.TxnResponseTopic,
			failPutKey: "p1/1/777/7777/2025-06-13T07:06:18Z",
			value:      TransactionResponseSlice,
		},
	}
	go func() {
		defer mockProducer.Close()

		for _, test := range tests {
			fakes.MakeBadS3Client(&state.AWS, fakes.BadS3Config{
				FailPutKey: test.failPutKey,
			})

			// expect one message to be produced per `PutResult`
			mockProducer.ExpectInputAndSucceed()

			mockConsumer.ConsumeClaim(test.topic, key, test.value)
		}
	}()

	// then
	msgs := drainMockProducerMessages(mockProducer)
	assert.ElementsMatch(t, WantPutRetryMsgs, msgs)
}
