package svc

import (
	"github.com/IBM/sarama"
	saramaMocks "github.com/IBM/sarama/mocks"
	"testing"

	"yodleeops/infra"
	"yodleeops/infra/fakes"
	"yodleeops/testutil"
	"yodleeops/yodlee"

	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/stretchr/testify/assert"
)

func setupConsumersTest(t *testing.T) *App {
	awsClient := testutil.SetupAwsITest(t)

	app := &App{AWS: awsClient}

	testutil.SeedS3Buckets(t, app.AWS)
	return app
}

func mockConsumeFiMessage(ctx Context, key string, value any) {
	switch v := value.(type) {
	case []yodlee.DataExtractsProviderAccount:
		ConsumeCnctRefreshMessage(ctx, key, v)
	case []yodlee.DataExtractsAccount:
		ConsumeAcctRefreshMessage(ctx, key, v)
	case []yodlee.DataExtractsHolding:
		ConsumeHoldRefreshMessage(ctx, key, v)
	case []yodlee.DataExtractsTransaction:
		ConsumeTxnRefreshMessage(ctx, key, v)
	case yodlee.ProviderAccountResponse:
		ConsumeCnctResponseMessage(ctx, key, v)
	case yodlee.AccountResponse:
		ConsumeAcctResponseMessage(ctx, key, v)
	case yodlee.HoldingResponse:
		ConsumeHoldResponseMessage(ctx, key, v)
	case yodlee.TransactionResponse:
		ConsumeTxnResponseMessage(ctx, key, v)
	case []DeleteRetry:
		ConsumeDeleteRetryMessage(ctx, key, v)
	}
}

func TestFiMessageConsumers(t *testing.T) {
	// given
	app := setupConsumersTest(t)

	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true

	mockProducer := saramaMocks.NewAsyncProducer(t, config)
	app.Producer = mockProducer

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
			value any
		}{
			// Refreshes
			{value: []yodlee.DataExtractsProviderAccount{providerAccountRefresh}},
			{value: []yodlee.DataExtractsAccount{accountRefresh}},
			{value: []yodlee.DataExtractsHolding{holdingRefresh}},
			{value: []yodlee.DataExtractsTransaction{transactionRefresh}},

			// Responses
			{value: yodlee.ProviderAccountResponse{ProviderAccount: []yodlee.ProviderAccount{providerAccountResponse}}},
			{value: yodlee.AccountResponse{Account: []yodlee.Account{accountResponse}}},
			{value: yodlee.HoldingResponse{Holding: []yodlee.Holding{holdingResponse}}},
			{value: yodlee.TransactionResponse{Transaction: []yodlee.TransactionWithDateTime{transactionResponse}}},
			{
				value: []DeleteRetry{
					{
						Kind:   ListKind,
						Bucket: app.AWS.Buckets.Transactions,
						Prefix: "p1/1/100/3000",
					},
					{
						Kind:   DeleteKind,
						Bucket: app.AWS.Buckets.Connections,
						Keys:   []string{"p1/1/30/2025-06-15"},
					},
				},
			},
		} {
			appCtx := Context{Context: t.Context(), App: app}

			// expect one message to be produced per `PutResult` except DeleteRetries
			if _, ok := test.value.([]DeleteRetry); !ok {
				mockProducer.ExpectInputAndSucceed()
			}

			mockConsumeFiMessage(appCtx, "p1", test.value)
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
		{Bucket: app.AWS.Buckets.Connections, Key: "p1/1/10/2025-06-12"},
		{Bucket: app.AWS.Buckets.Connections, Key: "p1/1/10/2025-06-13"},
		{Bucket: app.AWS.Buckets.Connections, Key: "p1/1/20/2025-06-14"},
		//{Bucket: App.S3.Buckets.Connections, Key: "p1/1/30/2025-06-15"},
		{Bucket: app.AWS.Buckets.Connections, Key: "p1/1/99/2025-06-13"},
		{Bucket: app.AWS.Buckets.Connections, Key: "p1/1/77/2025-06-13"},

		// Accounts
		{Bucket: app.AWS.Buckets.Accounts, Key: "p1/1/10/100/2025-06-12"},
		{Bucket: app.AWS.Buckets.Accounts, Key: "p1/1/10/100/2025-06-13"},
		{Bucket: app.AWS.Buckets.Accounts, Key: "p2/1/20/200/2025-06-14"},
		{Bucket: app.AWS.Buckets.Accounts, Key: "p2/1/30/400/2025-06-15"},
		{Bucket: app.AWS.Buckets.Accounts, Key: "p1/1/99/999/2025-06-13"},
		{Bucket: app.AWS.Buckets.Accounts, Key: "p1/1/77/777/2025-06-13"},

		// Holdings
		{Bucket: app.AWS.Buckets.Holdings, Key: "p1/1/100/1000/2025-06-12"},
		{Bucket: app.AWS.Buckets.Holdings, Key: "p1/1/100/1000/2025-06-13"},
		{Bucket: app.AWS.Buckets.Holdings, Key: "p2/1/100/1000/2025-06-14"},
		{Bucket: app.AWS.Buckets.Holdings, Key: "p2/1/200/2000/2025-06-15"},
		{Bucket: app.AWS.Buckets.Holdings, Key: "p1/1/999/9999/2025-06-13"},
		{Bucket: app.AWS.Buckets.Holdings, Key: "p1/1/777/7777/2025-06-13"},

		// Transactions
		//{Bucket: App.S3.Buckets.Transactions, Key: "p1/1/100/3000/2025-06-12T00:14:37Z"},
		//{Bucket: App.S3.Buckets.Transactions, Key: "p1/1/100/3000/2025-06-12T02:48:09Z"},
		{Bucket: app.AWS.Buckets.Transactions, Key: "p2/1/100/3000/2025-06-13T02:48:09Z"},
		{Bucket: app.AWS.Buckets.Transactions, Key: "p2/1/200/2000/2025-06-14T07:06:18Z"},
		{Bucket: app.AWS.Buckets.Transactions, Key: "p1/1/999/9999/2025-06-13T07:06:18Z"},
		{Bucket: app.AWS.Buckets.Transactions, Key: "p1/1/777/7777/2025-06-13T07:06:18Z"},
	}

	assert.ElementsMatch(t, wantKeys, testutil.GetAllKeys(t, app.AWS))
}

func TestFiMessageConsumers_S3Errors(t *testing.T) {
	// given
	app := setupConsumersTest(t)

	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true

	mockProducer := saramaMocks.NewAsyncProducer(t, config)
	app.Producer = mockProducer

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
			failPutKey string
			value      any
		}{
			// Refreshes
			{
				failPutKey: "p1/1/99/2025-06-13",
				value:      providerAccountRefresh,
			},
			{
				failPutKey: "p1/1/99/999/2025-06-13",
				value:      accountRefresh,
			},
			{
				failPutKey: "p1/1/999/9999/2025-06-13",
				value:      holdingRefresh,
			},
			{
				failPutKey: "p1/1/999/9999/2025-06-13T07:06:18Z",
				value:      transactionRefresh,
			},
			// Responses
			{
				failPutKey: "p1/1/77/2025-06-13",
				value:      providerAccountResponse,
			},
			{
				failPutKey: "p1/1/77/777/2025-06-13",
				value:      accountResponse,
			},
			{
				failPutKey: "p1/1/777/7777/2025-06-13",
				value:      holdingResponse,
			},
			{
				failPutKey: "p1/1/777/7777/2025-06-13T07:06:18Z",
				value:      transactionResponse,
			},
		} {
			fakes.MakeBadS3Client(&app.AWS, fakes.BadS3Config{
				FailPutKey: test.failPutKey,
			})
			appCtx := Context{Context: t.Context(), App: app}

			// expect one message to be produced per `PutResult`
			mockProducer.ExpectInputAndSucceed()

			mockConsumeFiMessage(appCtx, key, test.value)
		}
	}()

	// then
	msgs := drainMockProducerMessages(mockProducer)

	wantPutRetryMsgs := []any{
		providerAccountResponse, accountResponse, holdingResponse, transactionResponse, providerAccountRefresh, accountRefresh, holdingRefresh, transactionRefresh,
	}
	assert.ElementsMatch(t, wantPutRetryMsgs, msgs)
}
