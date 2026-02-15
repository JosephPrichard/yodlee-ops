package svc

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/google/go-cmp/cmp/cmpopts"
	"testing"
	"yodleeops/infra"
	infrastub "yodleeops/infra/stubs"
	"yodleeops/internal/yodlee"
	"yodleeops/testutil"

	"github.com/stretchr/testify/assert"
)

func setupConsumersTest(t *testing.T) *App {
	awsClient := testutil.SetupAwsITest(t)

	app := &App{AwsClient: awsClient}

	testutil.SeedS3Buckets(t, app.AwsClient)
	return app
}

func handleFiMessage(ctx context.Context, app *App, key string, value any) {
	switch v := value.(type) {
	case []yodlee.DataExtractsProviderAccount:
		app.HandleCnctRefreshMessage(ctx, key, v)
	case []yodlee.DataExtractsAccount:
		app.HandleAcctRefreshMessage(ctx, key, v)
	case []yodlee.DataExtractsHolding:
		app.HandleHoldRefreshMessage(ctx, key, v)
	case []yodlee.DataExtractsTransaction:
		app.HandleTxnRefreshMessage(ctx, key, v)
	case yodlee.ProviderAccountResponse:
		app.HandleCnctResponseMessage(ctx, key, v)
	case yodlee.AccountResponse:
		app.HandleAcctResponseMessage(ctx, key, v)
	case yodlee.HoldingResponse:
		app.HandleHoldResponseMessage(ctx, key, v)
	case yodlee.TransactionResponse:
		app.HandleTxnResponseMessage(ctx, key, v)
	case []DeleteRetry:
		app.HandleDeleteRecoveryMessage(ctx, key, v)
	}
}

func stubbedFiMessages(producerStub *infrastub.Producer) []any {
	var msgs []any
	for _, kafkaMsg := range producerStub.Messages {
		var result any
		switch kafkaMsg.Topic {
		case infra.CnctResponseTopic:
			result = unmarshalJsonMono[yodlee.ProviderAccountResponse](kafkaMsg.Value)
		case infra.AcctResponseTopic:
			result = unmarshalJsonMono[yodlee.AccountResponse](kafkaMsg.Value)
		case infra.HoldResponseTopic:
			result = unmarshalJsonMono[yodlee.HoldingResponse](kafkaMsg.Value)
		case infra.TxnResponseTopic:
			result = unmarshalJsonMono[yodlee.TransactionResponse](kafkaMsg.Value)
		case infra.CnctRefreshTopic:
			result = unmarshalJsonMono[[]yodlee.DataExtractsProviderAccount](kafkaMsg.Value)
		case infra.AcctRefreshTopic:
			result = unmarshalJsonMono[[]yodlee.DataExtractsAccount](kafkaMsg.Value)
		case infra.HoldRefreshTopic:
			result = unmarshalJsonMono[[]yodlee.DataExtractsHolding](kafkaMsg.Value)
		case infra.TxnRefreshTopic:
			result = unmarshalJsonMono[[]yodlee.DataExtractsTransaction](kafkaMsg.Value)
		case infra.DeleteRecoveryTopic:
			result = fmt.Errorf("did not expect message on delete recovery topic: %s", kafkaMsg.Value)
		case infra.BroadcastTopic:
			type broadcastOutput struct {
				OriginTopic string          `json:"origintopic"`
				FiMessages  json.RawMessage `json:"messages"`
			}
			var brd broadcastOutput
			if err := json.Unmarshal(kafkaMsg.Value, &brd); err != nil {
				result = fmt.Errorf("failed to unmarshal broadcast message: %w", err)
				break
			}
			switch brd.OriginTopic {
			case infra.CnctResponseTopic:
				fmt.Printf("unmarshaling broadcast message: %+v", string(brd.FiMessages))
				result = unmarshalJsonMono[[]OpsProviderAccount](brd.FiMessages)
			case infra.AcctResponseTopic:
				result = unmarshalJsonMono[[]OpsAccount](brd.FiMessages)
			case infra.HoldResponseTopic:
				result = unmarshalJsonMono[[]OpsHolding](brd.FiMessages)
			case infra.TxnResponseTopic:
				result = unmarshalJsonMono[[]OpsTransaction](brd.FiMessages)
			case infra.CnctRefreshTopic:
				result = unmarshalJsonMono[[]OpsProviderAccountRefresh](brd.FiMessages)
			case infra.AcctRefreshTopic:
				result = unmarshalJsonMono[[]OpsAccountRefresh](brd.FiMessages)
			case infra.HoldRefreshTopic:
				result = unmarshalJsonMono[[]OpsHoldingRefresh](brd.FiMessages)
			case infra.TxnRefreshTopic:
				result = unmarshalJsonMono[[]OpsTransactionRefresh](brd.FiMessages)
			default:
				result = fmt.Sprintf("unexpected broadcast origin topic: %s", kafkaMsg.Topic)
			}
		default:
			result = fmt.Sprintf("unexpected topic: %s", kafkaMsg.Topic)
		}

		msgs = append(msgs, result)
	}
	return msgs
}

func TestFiMessageConsumers(t *testing.T) {
	// given
	app := setupConsumersTest(t)

	producerStub := &infrastub.Producer{}
	app.KafkaClient = &infra.KafkaClient{Producer: producerStub}

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
					Bucket: app.TxnBucket,
					Prefix: "p1/1/100/3000",
				},
				{
					Kind:   DeleteKind,
					Bucket: app.CnctBucket,
					Keys:   []string{"p1/1/30/2025-06-15"},
				},
			},
		},
	} {
		ctx := t.Context()
		handleFiMessage(ctx, app, "p1", test.value)
	}

	// then
	wantBroadcastMsgs := []any{
		[]OpsProviderAccountRefresh{{ProfileId: "p1", Data: providerAccountRefresh}},
		[]OpsAccountRefresh{{ProfileId: "p1", Data: accountRefresh}},
		[]OpsHoldingRefresh{{ProfileId: "p1", Data: holdingRefresh}},
		[]OpsTransactionRefresh{{ProfileId: "p1", Data: transactionRefresh}},
		[]OpsProviderAccount{{ProfileId: "p1", Data: providerAccountResponse}},
		[]OpsAccount{{ProfileId: "p1", Data: accountResponse}},
		[]OpsHolding{{ProfileId: "p1", Data: holdingResponse}},
		[]OpsTransaction{{ProfileId: "p1", Data: transactionResponse}},
	}
	assert.Equal(t, wantBroadcastMsgs, stubbedFiMessages(producerStub))

	// removed keys are commented.
	wantKeys := []testutil.WantKey{
		{Bucket: app.CnctBucket, Key: "p1/1/10/2025-06-12"},
		{Bucket: app.CnctBucket, Key: "p1/1/10/2025-06-13"},
		{Bucket: app.CnctBucket, Key: "p1/1/20/2025-06-14"},
		//{Bucket: App.CnctBucket, Key: "p1/1/30/2025-06-15"},
		{Bucket: app.CnctBucket, Key: "p1/1/99/2025-06-13"},
		{Bucket: app.CnctBucket, Key: "p1/1/77/2025-06-13"},

		// Accounts
		{Bucket: app.AcctBucket, Key: "p1/1/10/100/2025-06-12"},
		{Bucket: app.AcctBucket, Key: "p1/1/10/100/2025-06-13"},
		{Bucket: app.AcctBucket, Key: "p2/1/20/200/2025-06-14"},
		{Bucket: app.AcctBucket, Key: "p2/1/30/400/2025-06-15"},
		{Bucket: app.AcctBucket, Key: "p1/1/99/999/2025-06-13"},
		{Bucket: app.AcctBucket, Key: "p1/1/77/777/2025-06-13"},

		// Holdings
		{Bucket: app.HoldBucket, Key: "p1/1/100/1000/2025-06-12"},
		{Bucket: app.HoldBucket, Key: "p1/1/100/1000/2025-06-13"},
		{Bucket: app.HoldBucket, Key: "p2/1/100/1000/2025-06-14"},
		{Bucket: app.HoldBucket, Key: "p2/1/200/2000/2025-06-15"},
		{Bucket: app.HoldBucket, Key: "p1/1/999/9999/2025-06-13"},
		{Bucket: app.HoldBucket, Key: "p1/1/777/7777/2025-06-13"},

		// Transactions
		//{Bucket: App.TxnBucket, Key: "p1/1/100/3000/2025-06-12T00:14:37Z"},
		//{Bucket: App.TxnBucket, Key: "p1/1/100/3000/2025-06-12T02:48:09Z"},
		{Bucket: app.TxnBucket, Key: "p2/1/100/3000/2025-06-13T02:48:09Z"},
		{Bucket: app.TxnBucket, Key: "p2/1/200/2000/2025-06-14T07:06:18Z"},
		{Bucket: app.TxnBucket, Key: "p1/1/999/9999/2025-06-13T07:06:18Z"},
		{Bucket: app.TxnBucket, Key: "p1/1/777/7777/2025-06-13T07:06:18Z"},
	}

	assert.ElementsMatch(t, wantKeys, testutil.GetAllKeys(t, app.AwsClient))
}

func TestFiMessageConsumers_S3Errors(t *testing.T) {
	// given
	app := setupConsumersTest(t)

	producerStub := &infrastub.Producer{}
	app.KafkaClient = &infra.KafkaClient{Producer: producerStub}

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
		ctx := t.Context()

		if test.failPutKey != "" {
			app.AwsClient.S3Client = infrastub.MakeBadS3Client(app.AwsClient.S3Client, infrastub.BadS3ClientCfg{
				FailPutKey: test.failPutKey,
			})
		}

		handleFiMessage(ctx, app, key, test.value)
	}

	// then
	msgs := stubbedFiMessages(producerStub)

	wantMsgs := []any{
		providerAccountResponse, accountResponse, holdingResponse, transactionResponse,
		providerAccountRefresh, accountRefresh, holdingRefresh, transactionRefresh,
	}
	wantKafkaMsgs := []infrastub.KafkaMessage{
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
	testutil.Equal(t, wantKafkaMsgs, producerStub.Messages, cmpopts.IgnoreFields(infrastub.KafkaMessage{}, "Value"))
}
