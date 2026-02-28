package svc

import (
	"encoding/json"
	"fmt"
	"github.com/google/go-cmp/cmp/cmpopts"
	"testing"
	"yodleeops/infra"
	"yodleeops/infra/fakes"

	"yodleeops/testutil"
	"yodleeops/yodlee"

	"github.com/stretchr/testify/assert"
)

func setupConsumersTest(t *testing.T) *App {
	awsClient := testutil.SetupAwsITest(t)

	app := &App{AWS: awsClient}

	testutil.SeedS3Buckets(t, app.AWS)
	return app
}

func handleFiMessage(ctx Context, key string, value any) {
	switch v := value.(type) {
	case []yodlee.DataExtractsProviderAccount:
		HandleCnctRefreshMessage(ctx, key, v)
	case []yodlee.DataExtractsAccount:
		HandleAcctRefreshMessage(ctx, key, v)
	case []yodlee.DataExtractsHolding:
		HandleHoldRefreshMessage(ctx, key, v)
	case []yodlee.DataExtractsTransaction:
		HandleTxnRefreshMessage(ctx, key, v)
	case yodlee.ProviderAccountResponse:
		HandleCnctResponseMessage(ctx, key, v)
	case yodlee.AccountResponse:
		HandleAcctResponseMessage(ctx, key, v)
	case yodlee.HoldingResponse:
		HandleHoldResponseMessage(ctx, key, v)
	case yodlee.TransactionResponse:
		HandleTxnResponseMessage(ctx, key, v)
	case []DeleteRetry:
		HandleDeleteRecoveryMessage(ctx, key, v)
	}
}

func decode[JSON any](value []byte) any {
	var v JSON
	err := json.Unmarshal(value, &v)
	if err != nil {
		return err.Error()
	}
	return v
}

func decodeBroadcast(kafkaMsg fakes.KafkaMessage) any {
	type broadcastOutput struct {
		OriginTopic infra.Topic     `json:"origintopic"`
		FiMessages  json.RawMessage `json:"messages"`
	}
	var brd broadcastOutput
	if err := json.Unmarshal(kafkaMsg.Value, &brd); err != nil {
		return fmt.Errorf("unmarshal broadcast message: %w", err)
	}
	switch brd.OriginTopic {
	case infra.CnctResponseTopic:
		return decode[[]OpsProviderAccount](brd.FiMessages)
	case infra.AcctResponseTopic:
		return decode[[]OpsAccount](brd.FiMessages)
	case infra.HoldResponseTopic:
		return decode[[]OpsHolding](brd.FiMessages)
	case infra.TxnResponseTopic:
		return decode[[]OpsTransaction](brd.FiMessages)
	case infra.CnctRefreshTopic:
		return decode[[]OpsProviderAccountRefresh](brd.FiMessages)
	case infra.AcctRefreshTopic:
		return decode[[]OpsAccountRefresh](brd.FiMessages)
	case infra.HoldRefreshTopic:
		return decode[[]OpsHoldingRefresh](brd.FiMessages)
	case infra.TxnRefreshTopic:
		return decode[[]OpsTransactionRefresh](brd.FiMessages)
	default:
		return fmt.Sprintf("unexpected broadcast origin topic: %s", kafkaMsg.Topic)
	}
}

func decodeFiMessage(kafkaMsg fakes.KafkaMessage) any {
	switch kafkaMsg.Topic {
	case infra.CnctResponseTopic:
		return decode[yodlee.ProviderAccountResponse](kafkaMsg.Value)
	case infra.AcctResponseTopic:
		return decode[yodlee.AccountResponse](kafkaMsg.Value)
	case infra.HoldResponseTopic:
		return decode[yodlee.HoldingResponse](kafkaMsg.Value)
	case infra.TxnResponseTopic:
		return decode[yodlee.TransactionResponse](kafkaMsg.Value)
	case infra.CnctRefreshTopic:
		return decode[[]yodlee.DataExtractsProviderAccount](kafkaMsg.Value)
	case infra.AcctRefreshTopic:
		return decode[[]yodlee.DataExtractsAccount](kafkaMsg.Value)
	case infra.HoldRefreshTopic:
		return decode[[]yodlee.DataExtractsHolding](kafkaMsg.Value)
	case infra.TxnRefreshTopic:
		return decode[[]yodlee.DataExtractsTransaction](kafkaMsg.Value)
	case infra.BroadcastTopic:
		return decodeBroadcast(kafkaMsg)
	default:
		return fmt.Sprintf("unexpected topic: %s", kafkaMsg.Topic)
	}
}

func decodeFakedFiMessages(producerStub *fakes.FakeProducer) []any {
	var msgs []any
	for _, kafkaMsg := range producerStub.Messages {
		msgs = append(msgs, decodeFiMessage(kafkaMsg))
	}
	return msgs
}

func TestFiMessageConsumers(t *testing.T) {
	// given
	app := setupConsumersTest(t)
	appCtx := Context{Context: t.Context(), App: app}

	producerStub := &fakes.FakeProducer{}
	app.KafkaClient = infra.KafkaClient{Producer: producerStub}

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
					Bucket: app.Buckets.Transactions,
					Prefix: "p1/1/100/3000",
				},
				{
					Kind:   DeleteKind,
					Bucket: app.Buckets.Connections,
					Keys:   []string{"p1/1/30/2025-06-15"},
				},
			},
		},
	} {
		handleFiMessage(appCtx, "p1", test.value)
	}

	// then
	wantBroadcastMsgs := []any{
		[]OpsProviderAccountRefresh{
			{OpsFiMessage: OpsFiMessage{ProfileId: "p1", OriginTopic: infra.CnctRefreshTopic}, Data: providerAccountRefresh},
		},
		[]OpsAccountRefresh{
			{OpsFiMessage: OpsFiMessage{ProfileId: "p1", OriginTopic: infra.AcctRefreshTopic}, Data: accountRefresh},
		},
		[]OpsHoldingRefresh{
			{OpsFiMessage: OpsFiMessage{ProfileId: "p1", OriginTopic: infra.HoldRefreshTopic}, Data: holdingRefresh},
		},
		[]OpsTransactionRefresh{
			{OpsFiMessage: OpsFiMessage{ProfileId: "p1", OriginTopic: infra.TxnRefreshTopic}, Data: transactionRefresh},
		},
		[]OpsProviderAccount{
			{OpsFiMessage: OpsFiMessage{ProfileId: "p1", OriginTopic: infra.CnctResponseTopic}, Data: providerAccountResponse},
		},
		[]OpsAccount{
			{OpsFiMessage: OpsFiMessage{ProfileId: "p1", OriginTopic: infra.AcctResponseTopic}, Data: accountResponse},
		},
		[]OpsHolding{
			{OpsFiMessage: OpsFiMessage{ProfileId: "p1", OriginTopic: infra.HoldResponseTopic}, Data: holdingResponse},
		},
		[]OpsTransaction{
			{OpsFiMessage: OpsFiMessage{ProfileId: "p1", OriginTopic: infra.TxnResponseTopic}, Data: transactionResponse},
		},
	}
	broadcastMsgs := decodeFakedFiMessages(producerStub)
	testutil.Equal(t, wantBroadcastMsgs, broadcastMsgs, cmpopts.IgnoreFields(OpsFiMessage{}, "Timestamp"))

	// removed keys are commented.
	wantKeys := []testutil.WantKey{
		{Bucket: app.Buckets.Connections, Key: "p1/1/10/2025-06-12"},
		{Bucket: app.Buckets.Connections, Key: "p1/1/10/2025-06-13"},
		{Bucket: app.Buckets.Connections, Key: "p1/1/20/2025-06-14"},
		//{Bucket: app.Buckets.Connections, Key: "p1/1/30/2025-06-15"},
		{Bucket: app.Buckets.Connections, Key: "p1/1/99/2025-06-13"},
		{Bucket: app.Buckets.Connections, Key: "p1/1/77/2025-06-13"},

		// Accounts
		{Bucket: app.Buckets.Accounts, Key: "p1/1/10/100/2025-06-12"},
		{Bucket: app.Buckets.Accounts, Key: "p1/1/10/100/2025-06-13"},
		{Bucket: app.Buckets.Accounts, Key: "p2/1/20/200/2025-06-14"},
		{Bucket: app.Buckets.Accounts, Key: "p2/1/30/400/2025-06-15"},
		{Bucket: app.Buckets.Accounts, Key: "p1/1/99/999/2025-06-13"},
		{Bucket: app.Buckets.Accounts, Key: "p1/1/77/777/2025-06-13"},

		// Holdings
		{Bucket: app.Buckets.Holdings, Key: "p1/1/100/1000/2025-06-12"},
		{Bucket: app.Buckets.Holdings, Key: "p1/1/100/1000/2025-06-13"},
		{Bucket: app.Buckets.Holdings, Key: "p2/1/100/1000/2025-06-14"},
		{Bucket: app.Buckets.Holdings, Key: "p2/1/200/2000/2025-06-15"},
		{Bucket: app.Buckets.Holdings, Key: "p1/1/999/9999/2025-06-13"},
		{Bucket: app.Buckets.Holdings, Key: "p1/1/777/7777/2025-06-13"},

		// Transactions
		//{Bucket: app.Buckets.Transactions, Key: "p1/1/100/3000/2025-06-12T00:14:37Z"},
		//{Bucket: app.Buckets.Transactions, Key: "p1/1/100/3000/2025-06-12T02:48:09Z"},
		{Bucket: app.Buckets.Transactions, Key: "p2/1/100/3000/2025-06-13T02:48:09Z"},
		{Bucket: app.Buckets.Transactions, Key: "p2/1/200/2000/2025-06-14T07:06:18Z"},
		{Bucket: app.Buckets.Transactions, Key: "p1/1/999/9999/2025-06-13T07:06:18Z"},
		{Bucket: app.Buckets.Transactions, Key: "p1/1/777/7777/2025-06-13T07:06:18Z"},
	}

	assert.ElementsMatch(t, wantKeys, testutil.GetAllKeys(t, app.AWS))
}

func TestFiMessageConsumers_S3Errors(t *testing.T) {
	// given
	app := setupConsumersTest(t)
	appCtx := Context{Context: t.Context(), App: app}

	producerStub := &fakes.FakeProducer{}
	app.KafkaClient = infra.KafkaClient{Producer: producerStub}

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
		if test.failPutKey != "" {
			fakes.MakeBadS3Client(&app.AWS, fakes.BadS3Config{
				FailPutKey: test.failPutKey,
			})
		}

		handleFiMessage(appCtx, key, test.value)
	}

	// then
	msgs := decodeFakedFiMessages(producerStub)

	wantMsgs := []any{
		providerAccountResponse, accountResponse, holdingResponse, transactionResponse,
		providerAccountRefresh, accountRefresh, holdingRefresh, transactionRefresh,
	}
	wantKafkaMsgs := []fakes.KafkaMessage{
		{Topic: infra.CnctRefreshTopic, Key: "p1"},
		{Topic: infra.AcctRefreshTopic, Key: "p1"},
		{Topic: infra.HoldRefreshTopic, Key: "p1"},
		{Topic: infra.TxnRefreshTopic, Key: "p1"},
		{Topic: infra.CnctResponseTopic, Key: "p1"},
		{Topic: infra.AcctResponseTopic, Key: "p1"},
		{Topic: infra.HoldResponseTopic, Key: "p1"},
		{Topic: infra.TxnResponseTopic, Key: "p1"},
	}

	assert.ElementsMatch(t, wantMsgs, msgs)
	testutil.Equal(t, wantKafkaMsgs, producerStub.Messages, cmpopts.IgnoreFields(fakes.KafkaMessage{}, "Value"))
}
