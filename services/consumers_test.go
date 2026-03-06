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

func setupConsumersTest(t *testing.T) *State {
	awsClient := testutil.SetupITest(t)
	state := &State{AWS: awsClient}
	return state
}

type MockConsumer struct {
	t         *testing.T
	consumers map[infra.Topic]Consumer
}

func (p *MockConsumer) ConsumeClaim(topic infra.Topic, key string, value any) {
	v, err := json.Marshal(value)
	require.NoError(p.t, err)

	session := &fakeSession{}
	claim := &fakeClaim{
		msgCh: make(chan *sarama.ConsumerMessage, 1),
	}

	msg := &sarama.ConsumerMessage{
		Key:   []byte(key),
		Value: v,
	}
	claim.msgCh <- msg
	close(claim.msgCh)

	err = p.consumers[topic].Handler.ConsumeClaim(session, claim)
	require.NoError(p.t, err)
}

func TestConsumers(t *testing.T) {
	// given
	state := setupConsumersTest(t)

	mockConsumer := MockConsumer{t: t, consumers: MakeConsumers(state)}

	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true

	mockProducer := saramaMocks.NewAsyncProducer(t, config)
	state.Producer = mockProducer

	providerAccountRefresh := yodlee.DataExtractsProviderAccount{
		Id:          99,
		LastUpdated: "2025-06-13",
		RequestId:   "REQUEST",
	}
	accountRefresh := yodlee.DataExtractsAccount{
		ProviderAccountId: 99,
		Id:                999,
		LastUpdated:       "2025-06-13",
		AccountName:       "Savings Data",
	}
	holdingRefresh := yodlee.DataExtractsHolding{
		AccountId:   999,
		Id:          9999,
		LastUpdated: "2025-06-13",
		HoldingType: "Stock",
	}
	transactionRefresh := yodlee.DataExtractsTransaction{
		AccountId:   999,
		Id:          9999,
		Date:        "2025-06-13T07:06:18Z",
		CheckNumber: "1299",
	}
	providerAccountResponse := yodlee.ProviderAccount{
		Id:          77,
		LastUpdated: "2025-06-13",
		RequestId:   "REQUEST",
	}
	accountResponse := yodlee.Account{
		ProviderAccountId: 77,
		Id:                777,
		LastUpdated:       "2025-06-13",
		AccountName:       "Savings Data",
	}
	holdingResponse := yodlee.Holding{
		AccountId:   777,
		Id:          7777,
		LastUpdated: "2025-06-13",
		HoldingType: "Stock",
	}
	transactionResponse := yodlee.TransactionWithDateTime{
		AccountId:   777,
		Id:          7777,
		Date:        "2025-06-13T07:06:18Z",
		CheckNumber: "1299",
	}

	// when
	go func() {
		defer mockProducer.Close()

		for _, test := range []struct {
			topic infra.Topic
			value any
		}{
			// Refreshes
			{
				topic: infra.CnctRefreshTopic,
				value: []yodlee.DataExtractsProviderAccount{providerAccountRefresh},
			},
			{
				topic: infra.AcctRefreshTopic,
				value: []yodlee.DataExtractsAccount{accountRefresh},
			},
			{
				topic: infra.HoldRefreshTopic,
				value: []yodlee.DataExtractsHolding{holdingRefresh},
			},
			{
				topic: infra.TxnRefreshTopic,
				value: []yodlee.DataExtractsTransaction{transactionRefresh},
			},
			// Responses
			{
				topic: infra.CnctResponseTopic,
				value: yodlee.ProviderAccountResponse{ProviderAccount: []yodlee.ProviderAccount{providerAccountResponse}},
			},
			{
				topic: infra.AcctResponseTopic,
				value: yodlee.AccountResponse{Account: []yodlee.Account{accountResponse}},
			},
			{
				topic: infra.HoldResponseTopic,
				value: yodlee.HoldingResponse{Holding: []yodlee.Holding{holdingResponse}},
			},
			{
				topic: infra.TxnResponseTopic,
				value: yodlee.TransactionResponse{Transaction: []yodlee.TransactionWithDateTime{transactionResponse}},
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
		} {
			// expect one message to be produced per `PutResult` except DeleteRetries
			if _, ok := test.value.([]DeleteRetry); !ok {
				mockProducer.ExpectInputAndSucceed()
			}
			mockConsumer.ConsumeClaim(test.topic, "p1", test.value)
		}
	}()

	// then
	wantBroadcastMsgs := []any{
		BroadcastInput[OpsProviderAccountRefresh, yodlee.DataExtractsProviderAccount]{
			OriginTopic: infra.CnctRefreshTopic,
			FiMessages: []OpsProviderAccountRefresh{
				{
					OpsFiMessage: OpsFiMessage{ProfileId: "p1", OriginTopic: infra.CnctRefreshTopic},
					Data:         providerAccountRefresh,
				},
			},
		},
		BroadcastInput[OpsAccountRefresh, yodlee.DataExtractsAccount]{
			OriginTopic: infra.AcctRefreshTopic,
			FiMessages: []OpsAccountRefresh{
				{
					OpsFiMessage: OpsFiMessage{ProfileId: "p1", OriginTopic: infra.AcctRefreshTopic},
					Data:         accountRefresh,
				},
			},
		},
		BroadcastInput[OpsHoldingRefresh, yodlee.DataExtractsHolding]{
			OriginTopic: infra.HoldRefreshTopic,
			FiMessages: []OpsHoldingRefresh{
				{
					OpsFiMessage: OpsFiMessage{ProfileId: "p1", OriginTopic: infra.HoldRefreshTopic},
					Data:         holdingRefresh,
				},
			},
		},
		BroadcastInput[OpsTransactionRefresh, yodlee.DataExtractsTransaction]{
			OriginTopic: infra.TxnRefreshTopic,
			FiMessages: []OpsTransactionRefresh{
				{
					OpsFiMessage: OpsFiMessage{ProfileId: "p1", OriginTopic: infra.TxnRefreshTopic},
					Data:         transactionRefresh,
				},
			},
		},
		BroadcastInput[OpsProviderAccount, yodlee.ProviderAccount]{
			OriginTopic: infra.CnctResponseTopic,
			FiMessages: []OpsProviderAccount{
				{
					OpsFiMessage: OpsFiMessage{ProfileId: "p1", OriginTopic: infra.CnctResponseTopic},
					Data:         providerAccountResponse,
				},
			},
		},
		BroadcastInput[OpsAccount, yodlee.Account]{
			OriginTopic: infra.AcctResponseTopic,
			FiMessages: []OpsAccount{
				{
					OpsFiMessage: OpsFiMessage{ProfileId: "p1", OriginTopic: infra.AcctResponseTopic},
					Data:         accountResponse,
				},
			},
		},
		BroadcastInput[OpsHolding, yodlee.Holding]{
			OriginTopic: infra.HoldResponseTopic,
			FiMessages: []OpsHolding{
				{
					OpsFiMessage: OpsFiMessage{ProfileId: "p1", OriginTopic: infra.HoldResponseTopic},
					Data:         holdingResponse,
				},
			},
		},
		BroadcastInput[OpsTransaction, yodlee.TransactionWithDateTime]{
			OriginTopic: infra.TxnResponseTopic,
			FiMessages: []OpsTransaction{
				{
					OpsFiMessage: OpsFiMessage{ProfileId: "p1", OriginTopic: infra.TxnResponseTopic},
					Data:         transactionResponse,
				},
			},
		},
	}
	broadcastMsgs := drainMockProducerMessages(mockProducer)
	testutil.Equal(t, wantBroadcastMsgs, broadcastMsgs, cmpopts.IgnoreFields(OpsFiMessage{}, "Timestamp"))

	// removed keys are commented.
	wantKeys := []testutil.WantKey{
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

	assert.ElementsMatch(t, wantKeys, testutil.GetAllKeys(t, state.AWS))
}

func TestConsumers_S3Errors(t *testing.T) {
	// given
	state := setupConsumersTest(t)

	mockConsumer := MockConsumer{t: t, consumers: MakeConsumers(state)}

	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true

	mockProducer := saramaMocks.NewAsyncProducer(t, config)
	state.Producer = mockProducer

	key := "p1" // all messages for same profileId.

	providerAccountRefresh := []yodlee.DataExtractsProviderAccount{
		{
			Id:          99,
			LastUpdated: "2025-06-13",
			RequestId:   "REQUEST",
		},
	}
	accountRefresh := []yodlee.DataExtractsAccount{
		{
			ProviderAccountId: 99,
			Id:                999,
			LastUpdated:       "2025-06-13",
			AccountName:       "Savings Data",
		},
	}
	holdingRefresh := []yodlee.DataExtractsHolding{
		{
			AccountId:   999,
			Id:          9999,
			LastUpdated: "2025-06-13",
			HoldingType: "Stock",
		},
	}
	transactionRefresh := []yodlee.DataExtractsTransaction{
		{
			AccountId:   999,
			Id:          9999,
			Date:        "2025-06-13T07:06:18Z",
			CheckNumber: "1299",
		},
	}
	providerAccountResponse := yodlee.ProviderAccountResponse{
		ProviderAccount: []yodlee.ProviderAccount{
			{
				Id:          77,
				LastUpdated: "2025-06-13",
				RequestId:   "REQUEST",
			},
		},
	}
	accountResponse := yodlee.AccountResponse{
		Account: []yodlee.Account{
			{
				ProviderAccountId: 77,
				Id:                777,
				LastUpdated:       "2025-06-13",
				AccountName:       "Savings Data",
			},
		},
	}
	holdingResponse := yodlee.HoldingResponse{
		Holding: []yodlee.Holding{
			{
				AccountId:   777,
				Id:          7777,
				LastUpdated: "2025-06-13",
				HoldingType: "Stock",
			},
		},
	}
	transactionResponse := yodlee.TransactionResponse{
		Transaction: []yodlee.TransactionWithDateTime{
			{
				AccountId:   777,
				Id:          7777,
				Date:        "2025-06-13T07:06:18Z",
				CheckNumber: "1299",
			},
		},
	}

	// when
	go func() {
		defer mockProducer.Close()

		for _, test := range []struct {
			topic      infra.Topic
			failPutKey string
			value      any
		}{
			// Refreshes
			{
				topic:      infra.CnctRefreshTopic,
				failPutKey: "p1/1/99/2025-06-13",
				value:      providerAccountRefresh,
			},
			{
				topic:      infra.AcctRefreshTopic,
				failPutKey: "p1/1/99/999/2025-06-13",
				value:      accountRefresh,
			},
			{
				topic:      infra.HoldRefreshTopic,
				failPutKey: "p1/1/999/9999/2025-06-13",
				value:      holdingRefresh,
			},
			{
				topic:      infra.TxnRefreshTopic,
				failPutKey: "p1/1/999/9999/2025-06-13T07:06:18Z",
				value:      transactionRefresh,
			},
			// Responses
			{
				topic:      infra.CnctResponseTopic,
				failPutKey: "p1/1/77/2025-06-13",
				value:      providerAccountResponse,
			},
			{
				topic:      infra.AcctResponseTopic,
				failPutKey: "p1/1/77/777/2025-06-13",
				value:      accountResponse,
			},
			{
				topic:      infra.HoldResponseTopic,
				failPutKey: "p1/1/777/7777/2025-06-13",
				value:      holdingResponse,
			},
			{
				topic:      infra.TxnResponseTopic,
				failPutKey: "p1/1/777/7777/2025-06-13T07:06:18Z",
				value:      transactionResponse,
			},
		} {
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

	wantPutRetryMsgs := []any{
		providerAccountResponse, accountResponse, holdingResponse, transactionResponse, providerAccountRefresh, accountRefresh, holdingRefresh, transactionRefresh,
	}
	assert.ElementsMatch(t, wantPutRetryMsgs, msgs)
}
