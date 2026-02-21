package svc

import (
	"context"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/stretchr/testify/assert"
	"testing"
	"yodleeops/internal/infra"
	"yodleeops/internal/infra/stubs"
	"yodleeops/internal/testutil"
	"yodleeops/internal/yodlee"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/stretchr/testify/require"
)

var fiMessageOpts = []cmp.Option{cmpopts.IgnoreFields(OpsFiMessage{}, "Timestamp")}

func setupIngestionTest(t *testing.T) *App {
	awsClient := testutil.SetupAwsITest(t)

	app := &App{AwsClient: awsClient}
	app.AwsClient.PageLength = aws.Int32(1) // testing ListObjectsV2 pagination.

	testutil.SeedS3Buckets(t, app.AwsClient)
	return app
}

func TestIngestCnctResponses(t *testing.T) {
	// given
	ctx := context.WithValue(t.Context(), "trace", t.Name())

	app := setupIngestionTest(t)
	appCtx := AppContext{Context: ctx, App: app}

	// when
	putResults := IngestCnctResponses(appCtx, "p1", yodlee.ProviderAccountResponse{
		ProviderAccount: []yodlee.ProviderAccount{
			{
				Id:          1,
				LastUpdated: "2025-06-12",
				RequestId:   "REQUEST",
			},
			{
				Id:          100,
				LastUpdated: "2025-06-13",
				RequestId:   "REQUEST",
			},
		},
	})
	assert.Equal(t, 2, len(putResults))

	// then
	wantObjects := []testutil.WantObject[OpsProviderAccount]{
		{
			Bucket: app.CnctBucket,
			Key:    "p1/1/1/2025-06-12",
			Value: OpsProviderAccount{
				OpsFiMessage: OpsFiMessage{ProfileId: "p1", OriginTopic: infra.CnctResponseTopic},
				Data:         yodlee.ProviderAccount{Id: 1, LastUpdated: "2025-06-12", RequestId: "REQUEST"},
			},
		},
		{
			Bucket: app.CnctBucket,
			Key:    "p1/1/100/2025-06-13",
			Value: OpsProviderAccount{
				OpsFiMessage: OpsFiMessage{ProfileId: "p1", OriginTopic: infra.CnctResponseTopic},
				Data:         yodlee.ProviderAccount{Id: 100, LastUpdated: "2025-06-13", RequestId: "REQUEST"},
			},
		},
	}
	testutil.AssertObjects(t, &app.AwsClient, wantObjects, fiMessageOpts...)
}

func TestIngestAcctResponses(t *testing.T) {
	// given
	ctx := context.WithValue(t.Context(), "trace", t.Name())

	app := setupIngestionTest(t)
	appCtx := AppContext{Context: ctx, App: app}

	// when
	putResults := IngestAcctResponses(appCtx, "p1", yodlee.AccountResponse{
		Account: []yodlee.Account{
			{
				ProviderAccountId: 1,
				Id:                1,
				LastUpdated:       "2025-06-12",
				AccountName:       "Checking Data",
			},
			{
				ProviderAccountId: 100,
				Id:                200,
				LastUpdated:       "2025-06-13",
				AccountName:       "Savings Data",
			},
		},
	})
	assert.Equal(t, 2, len(putResults))

	// then
	wantObjects := []testutil.WantObject[OpsAccount]{
		{
			Bucket: app.AcctBucket,
			Key:    "p1/1/1/1/2025-06-12",
			Value: OpsAccount{
				OpsFiMessage: OpsFiMessage{ProfileId: "p1", OriginTopic: infra.AcctResponseTopic},
				Data:         yodlee.Account{ProviderAccountId: 1, Id: 1, LastUpdated: "2025-06-12", AccountName: "Checking Data"},
			},
		},
		{
			Bucket: app.AcctBucket,
			Key:    "p1/1/100/200/2025-06-13",
			Value: OpsAccount{
				OpsFiMessage: OpsFiMessage{ProfileId: "p1", OriginTopic: infra.AcctResponseTopic},
				Data:         yodlee.Account{ProviderAccountId: 100, Id: 200, LastUpdated: "2025-06-13", AccountName: "Savings Data"},
			},
		},
	}
	testutil.AssertObjects(t, &app.AwsClient, wantObjects, fiMessageOpts...)
}

func TestIngestHoldResponses(t *testing.T) {
	// given
	ctx := context.WithValue(t.Context(), "trace", t.Name())

	app := setupIngestionTest(t)
	appCtx := AppContext{Context: ctx, App: app}

	// when
	putResults := IngestHoldResponses(appCtx, "p1", yodlee.HoldingResponse{
		Holding: []yodlee.Holding{
			{
				AccountId:   1,
				Id:          1,
				LastUpdated: "2025-06-12",
				HoldingType: "Security",
			},
			{
				AccountId:   200,
				Id:          100,
				LastUpdated: "2025-06-13",
				HoldingType: "Stock",
			},
		},
	})
	assert.Equal(t, 2, len(putResults))

	// then
	wantObjects := []testutil.WantObject[OpsHolding]{
		{
			Bucket: app.HoldBucket,
			Key:    "p1/1/1/1/2025-06-12",
			Value: OpsHolding{
				OpsFiMessage: OpsFiMessage{ProfileId: "p1", OriginTopic: infra.HoldResponseTopic},
				Data:         yodlee.Holding{AccountId: 1, Id: 1, LastUpdated: "2025-06-12", HoldingType: "Security"},
			},
		},
		{
			Bucket: app.HoldBucket,
			Key:    "p1/1/200/100/2025-06-13",
			Value: OpsHolding{
				OpsFiMessage: OpsFiMessage{ProfileId: "p1", OriginTopic: infra.HoldResponseTopic},
				Data:         yodlee.Holding{AccountId: 200, Id: 100, LastUpdated: "2025-06-13", HoldingType: "Stock"},
			},
		},
	}
	testutil.AssertObjects(t, &app.AwsClient, wantObjects, fiMessageOpts...)
}

func TestIngestTxnResponses(t *testing.T) {
	// given
	ctx := context.WithValue(t.Context(), "trace", t.Name())

	app := setupIngestionTest(t)
	appCtx := AppContext{Context: ctx, App: app}

	// when
	putResults := IngestTxnResponses(appCtx, "p1", yodlee.TransactionResponse{
		Transaction: []yodlee.TransactionWithDateTime{
			{
				AccountId:   1,
				Id:          1,
				Date:        "2025-06-11T07:06:18Z",
				CheckNumber: "123",
			},
			{
				AccountId:   200,
				Id:          200,
				Date:        "2025-06-13T07:06:18Z",
				CheckNumber: "123",
			},
		},
	})
	assert.Equal(t, 2, len(putResults))

	// then
	wantObjects := []testutil.WantObject[OpsTransaction]{
		{
			Bucket: app.TxnBucket,
			Key:    "p1/1/1/1/2025-06-11T07:06:18Z",
			Value: OpsTransaction{
				OpsFiMessage: OpsFiMessage{ProfileId: "p1", OriginTopic: infra.TxnResponseTopic},
				Data:         yodlee.TransactionWithDateTime{AccountId: 1, Id: 1, Date: "2025-06-11T07:06:18Z", CheckNumber: "123"},
			},
		},
		{
			Bucket: app.TxnBucket,
			Key:    "p1/1/200/200/2025-06-13T07:06:18Z",
			Value: OpsTransaction{
				OpsFiMessage: OpsFiMessage{ProfileId: "p1", OriginTopic: infra.TxnResponseTopic},
				Data:         yodlee.TransactionWithDateTime{AccountId: 200, Id: 200, Date: "2025-06-13T07:06:18Z", CheckNumber: "123"},
			},
		},
	}
	testutil.AssertObjects(t, &app.AwsClient, wantObjects, fiMessageOpts...)
}

func TestIngestCnctRefreshes(t *testing.T) {
	// given
	ctx := context.WithValue(t.Context(), "trace", t.Name())

	app := setupIngestionTest(t)
	appCtx := AppContext{Context: ctx, App: app}

	// when
	result := IngestCnctRefreshes(appCtx, "p1", []yodlee.DataExtractsProviderAccount{
		{
			IsDeleted: true,
			Id:        10,
		},
		{
			Id:          99,
			LastUpdated: "2025-06-13",
			RequestId:   "REQUEST",
		},
	})
	assert.Equal(t, 1, len(result.PutResults))
	require.Empty(t, result.DeleteErrors)

	// then
	wantObjects := []testutil.WantObject[OpsProviderAccountRefresh]{
		{
			Bucket: app.CnctBucket,
			Key:    "p1/1/99/2025-06-13",
			Value: OpsProviderAccountRefresh{
				OpsFiMessage: OpsFiMessage{ProfileId: "p1", OriginTopic: infra.CnctRefreshTopic},
				Data:         yodlee.DataExtractsProviderAccount{Id: 99, LastUpdated: "2025-06-13", RequestId: "REQUEST"},
			},
		},
	}
	testutil.AssertObjects(t, &app.AwsClient, wantObjects, fiMessageOpts...)

	// removed keys are commented.
	wantKeys := []testutil.WantKey{
		// Connections
		//{Bucket: App.CnctBucket, Key: "p1/1/10/2025-06-12"},
		//{Bucket: App.CnctBucket, Key: "p1/1/10/2025-06-13"},
		{Bucket: app.CnctBucket, Key: "p1/1/20/2025-06-14"},
		{Bucket: app.CnctBucket, Key: "p1/1/30/2025-06-15"},
		{Bucket: app.CnctBucket, Key: "p1/1/99/2025-06-13"},

		// Accounts
		//{Bucket: App.AcctBucket, Key: "p1/1/10/100/2025-06-12"},
		//{Bucket: App.AcctBucket, Key: "p1/1/10/100/2025-06-13"},
		{Bucket: app.AcctBucket, Key: "p2/1/20/200/2025-06-14"},
		{Bucket: app.AcctBucket, Key: "p2/1/30/400/2025-06-15"},

		// Holdings
		//{Bucket: App.HoldBucket, Key: "p1/1/100/1000/2025-06-12"},
		//{Bucket: App.HoldBucket, Key: "p1/1/100/1000/2025-06-13"},
		{Bucket: app.HoldBucket, Key: "p2/1/100/1000/2025-06-14"},
		{Bucket: app.HoldBucket, Key: "p2/1/200/2000/2025-06-15"},

		// Transactions
		//{Bucket: App.TxnBucket, Key: "p1/1/100/3000/2025-06-12T00:14:37Z"},
		//{Bucket: App.TxnBucket, Key: "p1/1/100/3000/2025-06-12T02:48:09Z"},
		{Bucket: app.TxnBucket, Key: "p2/1/100/3000/2025-06-13T02:48:09Z"},
		{Bucket: app.TxnBucket, Key: "p2/1/200/2000/2025-06-14T07:06:18Z"},
	}
	assert.ElementsMatch(t, wantKeys, testutil.GetAllKeys(t, app.AwsClient))
}

func TestIngestAcctRefreshes(t *testing.T) {
	// given
	ctx := context.WithValue(t.Context(), "trace", t.Name())

	app := setupIngestionTest(t)
	appCtx := AppContext{Context: ctx, App: app}

	// when
	result := IngestAcctsRefreshes(appCtx, "p1", []yodlee.DataExtractsAccount{
		{
			IsDeleted:         true,
			ProviderAccountId: 10,
			Id:                100,
		},
		{
			ProviderAccountId: 99,
			Id:                999,
			LastUpdated:       "2025-06-13",
			AccountName:       "Savings Data",
		},
	})
	assert.Equal(t, 1, len(result.PutResults))
	require.Empty(t, result.DeleteErrors)

	// then
	wantObjects := []testutil.WantObject[OpsAccountRefresh]{
		{
			Bucket: app.AcctBucket,
			Key:    "p1/1/99/999/2025-06-13",
			Value: OpsAccountRefresh{
				OpsFiMessage: OpsFiMessage{ProfileId: "p1", OriginTopic: infra.AcctRefreshTopic},
				Data:         yodlee.DataExtractsAccount{ProviderAccountId: 99, Id: 999, LastUpdated: "2025-06-13", AccountName: "Savings Data"},
			},
		},
	}
	testutil.AssertObjects(t, &app.AwsClient, wantObjects, fiMessageOpts...)

	// removed keys are commented.
	wantKeys := []testutil.WantKey{
		// Connections
		{Bucket: app.CnctBucket, Key: "p1/1/10/2025-06-12"},
		{Bucket: app.CnctBucket, Key: "p1/1/10/2025-06-13"},
		{Bucket: app.CnctBucket, Key: "p1/1/20/2025-06-14"},
		{Bucket: app.CnctBucket, Key: "p1/1/30/2025-06-15"},

		// Accounts
		//{Bucket: App.AcctBucket, Key: "p1/1/10/100/2025-06-12"},
		//{Bucket: App.AcctBucket, Key: "p1/1/10/100/2025-06-13"},
		{Bucket: app.AcctBucket, Key: "p2/1/20/200/2025-06-14"},
		{Bucket: app.AcctBucket, Key: "p2/1/30/400/2025-06-15"},
		{Bucket: app.AcctBucket, Key: "p1/1/99/999/2025-06-13"},

		// Holdings
		//{Bucket: App.HoldBucket, Key: "p1/1/100/1000/2025-06-12"},
		//{Bucket: App.HoldBucket, Key: "p1/1/100/1000/2025-06-13"},
		{Bucket: app.HoldBucket, Key: "p2/1/100/1000/2025-06-14"},
		{Bucket: app.HoldBucket, Key: "p2/1/200/2000/2025-06-15"},

		// Transactions
		//{Bucket: App.TxnBucket, Key: "p1/1/100/3000/2025-06-12T00:14:37Z"},
		//{Bucket: App.TxnBucket, Key: "p1/1/100/3000/2025-06-12T02:48:09Z"},
		{Bucket: app.TxnBucket, Key: "p2/1/100/3000/2025-06-13T02:48:09Z"},
		{Bucket: app.TxnBucket, Key: "p2/1/200/2000/2025-06-14T07:06:18Z"},
	}
	assert.ElementsMatch(t, wantKeys, testutil.GetAllKeys(t, app.AwsClient))
}

func TestIngestTxnRefreshes(t *testing.T) {
	// given
	ctx := context.WithValue(t.Context(), "trace", t.Name())

	app := setupIngestionTest(t)
	appCtx := AppContext{Context: ctx, App: app}

	// when
	result := IngestTxnRefreshes(appCtx, "p1", []yodlee.DataExtractsTransaction{
		{
			IsDeleted: true,
			AccountId: 100,
			Id:        3000,
		},
		{
			AccountId:   999,
			Id:          9999,
			Date:        "2025-06-13T07:06:18Z",
			CheckNumber: "123",
		},
	})
	assert.Equal(t, 1, len(result.PutResults))
	require.Empty(t, result.DeleteErrors)

	// then
	wantObjects := []testutil.WantObject[OpsTransactionRefresh]{
		{
			Bucket: app.TxnBucket,
			Key:    "p1/1/999/9999/2025-06-13T07:06:18Z",
			Value: OpsTransactionRefresh{
				OpsFiMessage: OpsFiMessage{ProfileId: "p1", OriginTopic: infra.TxnRefreshTopic},
				Data:         yodlee.DataExtractsTransaction{AccountId: 999, Id: 9999, Date: "2025-06-13T07:06:18Z", CheckNumber: "123"},
			},
		},
	}
	testutil.AssertObjects(t, &app.AwsClient, wantObjects, fiMessageOpts...)

	// removed keys are commented.
	wantKeys := []testutil.WantKey{
		// Connections
		{Bucket: app.CnctBucket, Key: "p1/1/10/2025-06-12"},
		{Bucket: app.CnctBucket, Key: "p1/1/10/2025-06-13"},
		{Bucket: app.CnctBucket, Key: "p1/1/20/2025-06-14"},
		{Bucket: app.CnctBucket, Key: "p1/1/30/2025-06-15"},

		// Accounts
		{Bucket: app.AcctBucket, Key: "p1/1/10/100/2025-06-12"},
		{Bucket: app.AcctBucket, Key: "p1/1/10/100/2025-06-13"},
		{Bucket: app.AcctBucket, Key: "p2/1/20/200/2025-06-14"},
		{Bucket: app.AcctBucket, Key: "p2/1/30/400/2025-06-15"},

		// Holdings
		{Bucket: app.HoldBucket, Key: "p1/1/100/1000/2025-06-12"},
		{Bucket: app.HoldBucket, Key: "p1/1/100/1000/2025-06-13"},
		{Bucket: app.HoldBucket, Key: "p2/1/100/1000/2025-06-14"},
		{Bucket: app.HoldBucket, Key: "p2/1/200/2000/2025-06-15"},

		// Transactions
		//{Bucket: App.TxnBucket, Key: "p1/1/100/3000/2025-06-12T00:14:37Z"},
		//{Bucket: App.TxnBucket, Key: "p1/1/100/3000/2025-06-12T02:48:09Z"},
		{Bucket: app.TxnBucket, Key: "p2/1/100/3000/2025-06-13T02:48:09Z"},
		{Bucket: app.TxnBucket, Key: "p2/1/200/2000/2025-06-14T07:06:18Z"},
		{Bucket: app.TxnBucket, Key: "p1/1/999/9999/2025-06-13T07:06:18Z"},
	}
	assert.ElementsMatch(t, wantKeys, testutil.GetAllKeys(t, app.AwsClient))
}

func TestIngestHoldRefreshes(t *testing.T) {
	// given
	ctx := context.WithValue(t.Context(), "trace", t.Name())

	app := setupIngestionTest(t)
	appCtx := AppContext{Context: ctx, App: app}

	// when
	result := IngestHoldRefreshes(appCtx, "p1", []yodlee.DataExtractsHolding{
		// holdings can't be deleted individually (only the account that contains the holdings can be deleted).
		//{
		//	IsDeleted:   true,
		//	ExtnAcctId:  "a1",
		//	ExtnHoldId:  "h1",
		//},
		{
			AccountId:   999,
			Id:          9999,
			LastUpdated: "2025-06-13",
			HoldingType: "Stock",
		},
	})
	assert.Equal(t, 1, len(result.PutResults))
	require.Empty(t, result.DeleteErrors)

	// then
	wantObjects := []testutil.WantObject[OpsHoldingRefresh]{
		{
			Bucket: app.HoldBucket,
			Key:    "p1/1/999/9999/2025-06-13",
			Value: OpsHoldingRefresh{
				OpsFiMessage: OpsFiMessage{ProfileId: "p1", OriginTopic: infra.HoldRefreshTopic},
				Data:         yodlee.DataExtractsHolding{AccountId: 999, Id: 9999, LastUpdated: "2025-06-13", HoldingType: "Stock"},
			},
		},
	}
	testutil.AssertObjects(t, &app.AwsClient, wantObjects, fiMessageOpts...)

	// removed keys are commented.
	wantKeys := []testutil.WantKey{
		// Connections
		{Bucket: app.CnctBucket, Key: "p1/1/10/2025-06-12"},
		{Bucket: app.CnctBucket, Key: "p1/1/10/2025-06-13"},
		{Bucket: app.CnctBucket, Key: "p1/1/20/2025-06-14"},
		{Bucket: app.CnctBucket, Key: "p1/1/30/2025-06-15"},

		// Accounts
		{Bucket: app.AcctBucket, Key: "p1/1/10/100/2025-06-12"},
		{Bucket: app.AcctBucket, Key: "p1/1/10/100/2025-06-13"},
		{Bucket: app.AcctBucket, Key: "p2/1/20/200/2025-06-14"},
		{Bucket: app.AcctBucket, Key: "p2/1/30/400/2025-06-15"},

		// Holdings
		{Bucket: app.HoldBucket, Key: "p1/1/100/1000/2025-06-12"},
		{Bucket: app.HoldBucket, Key: "p1/1/100/1000/2025-06-13"},
		{Bucket: app.HoldBucket, Key: "p2/1/100/1000/2025-06-14"},
		{Bucket: app.HoldBucket, Key: "p2/1/200/2000/2025-06-15"},
		{Bucket: app.HoldBucket, Key: "p1/1/999/9999/2025-06-13"},

		// Transactions
		{Bucket: app.TxnBucket, Key: "p1/1/100/3000/2025-06-12T00:14:37Z"},
		{Bucket: app.TxnBucket, Key: "p1/1/100/3000/2025-06-12T02:48:09Z"},
		{Bucket: app.TxnBucket, Key: "p2/1/100/3000/2025-06-13T02:48:09Z"},
		{Bucket: app.TxnBucket, Key: "p2/1/200/2000/2025-06-14T07:06:18Z"},
	}
	assert.ElementsMatch(t, wantKeys, testutil.GetAllKeys(t, app.AwsClient))
}

func TestIngestDeleteRetries(t *testing.T) {
	// given
	ctx := context.WithValue(t.Context(), "trace", t.Name())

	app := setupIngestionTest(t)
	appCtx := AppContext{Context: ctx, App: app}

	// when
	results := IngestDeleteRetries(appCtx, []DeleteRetry{
		{
			Kind:   ListKind,
			Bucket: app.AcctBucket,
			Prefix: "p1/1/10",
		},
		{
			Kind:   DeleteKind,
			Bucket: app.TxnBucket,
			Keys:   []string{"p1/1/100/3000/2025-06-12T00:14:37Z", "p1/1/100/3000/2025-06-12T02:48:09Z"},
		},
	})
	require.Empty(t, results)

	// then
	// removed keys are commented.
	wantKeys := []testutil.WantKey{
		// Connections
		{Bucket: app.CnctBucket, Key: "p1/1/10/2025-06-12"},
		{Bucket: app.CnctBucket, Key: "p1/1/10/2025-06-13"},
		{Bucket: app.CnctBucket, Key: "p1/1/20/2025-06-14"},
		{Bucket: app.CnctBucket, Key: "p1/1/30/2025-06-15"},

		// Accounts
		//{Bucket: App.AcctBucket, Key: "p1/1/10/100/2025-06-12"},
		//{Bucket: App.AcctBucket, Key: "p1/1/10/100/2025-06-13"},
		{Bucket: app.AcctBucket, Key: "p2/1/20/200/2025-06-14"},
		{Bucket: app.AcctBucket, Key: "p2/1/30/400/2025-06-15"},

		// Holdings
		{Bucket: app.HoldBucket, Key: "p1/1/100/1000/2025-06-12"},
		{Bucket: app.HoldBucket, Key: "p1/1/100/1000/2025-06-13"},
		{Bucket: app.HoldBucket, Key: "p2/1/100/1000/2025-06-14"},
		{Bucket: app.HoldBucket, Key: "p2/1/200/2000/2025-06-15"},

		// Transactions
		//{Bucket: App.TxnBucket, Key: "p1/1/100/3000/2025-06-12T00:14:37Z"},
		//{Bucket: App.TxnBucket, Key: "p1/1/100/3000/2025-06-12T02:48:09Z"},
		{Bucket: app.TxnBucket, Key: "p2/1/100/3000/2025-06-13T02:48:09Z"},
		{Bucket: app.TxnBucket, Key: "p2/1/200/2000/2025-06-14T07:06:18Z"},
	}
	assert.ElementsMatch(t, wantKeys, testutil.GetAllKeys(t, app.AwsClient))
}

func putResultOpts[T any]() []cmp.Option {
	var value T
	return []cmp.Option{
		cmpopts.IgnoreFields(OpsFiMessage{}, "Timestamp"),
		cmpopts.IgnoreFields(value, "Err"),
	}
}

func TestIngest_PutFailure(t *testing.T) {
	ctx := context.WithValue(t.Context(), "trace", t.Name())

	setupTest := func(failKey string) *App {
		app := setupIngestionTest(t)
		infrastub.MakeBadS3Client(&app.AwsClient, infrastub.BadS3ClientCfg{
			FailPutKey: failKey,
		})
		return app
	}

	t.Run("CnctResponse", func(t *testing.T) {
		failKey := "p1/1/90/2025-06-12"
		app := setupTest(failKey)
		appCtx := AppContext{Context: ctx, App: app}

		input := yodlee.ProviderAccountResponse{
			ProviderAccount: []yodlee.ProviderAccount{
				{
					Id:          90,
					LastUpdated: "2025-06-12",
					RequestId:   "REQUEST",
				},
			},
		}
		putResults := IngestCnctResponses(appCtx, "p1", input)

		want := []PutCnctResult{
			{
				Key:   failKey,
				Input: OpsProviderAccount{OpsFiMessage: OpsFiMessage{ProfileId: "p1", OriginTopic: infra.CnctResponseTopic}, Data: input.ProviderAccount[0]},
			},
		}
		testutil.Equal(t, want, putResults, putResultOpts[PutCnctResult]()...)
	})

	t.Run("AcctResponse", func(t *testing.T) {
		failKey := "p1/1/90/900/2025-06-12"
		app := setupTest(failKey)
		appCtx := AppContext{Context: ctx, App: app}

		input := yodlee.AccountResponse{
			Account: []yodlee.Account{
				{
					ProviderAccountId: 90,
					Id:                900,
					LastUpdated:       "2025-06-12",
					AccountName:       "Checking Data",
				},
			},
		}
		putResults := IngestAcctResponses(appCtx, "p1", input)

		want := []PutAcctResult{
			{
				Key:   failKey,
				Input: OpsAccount{OpsFiMessage: OpsFiMessage{ProfileId: "p1", OriginTopic: infra.AcctResponseTopic}, Data: input.Account[0]},
			},
		}
		testutil.Equal(t, want, putResults, putResultOpts[PutAcctResult]()...)
	})

	t.Run("HoldResponse", func(t *testing.T) {
		failKey := "p1/1/900/9000/2025-06-12"
		app := setupTest(failKey)
		appCtx := AppContext{Context: ctx, App: app}

		input := yodlee.HoldingResponse{
			Holding: []yodlee.Holding{
				{
					AccountId:   900,
					Id:          9000,
					LastUpdated: "2025-06-12",
					HoldingType: "Security",
				},
			},
		}
		putResults := IngestHoldResponses(appCtx, "p1", input)

		want := []PutHoldResult{
			{
				Key:   failKey,
				Input: OpsHolding{OpsFiMessage: OpsFiMessage{ProfileId: "p1", OriginTopic: infra.HoldResponseTopic}, Data: input.Holding[0]},
			},
		}
		testutil.Equal(t, want, putResults, putResultOpts[PutHoldResult]()...)
	})

	t.Run("TxnResponse", func(t *testing.T) {
		failKey := "p1/1/900/9000/2025-06-11T07:06:18Z"
		app := setupTest(failKey)
		appCtx := AppContext{Context: ctx, App: app}

		input := yodlee.TransactionResponse{
			Transaction: []yodlee.TransactionWithDateTime{
				{
					AccountId:   900,
					Id:          9000,
					Date:        "2025-06-11T07:06:18Z",
					CheckNumber: "123",
				},
			},
		}
		putResults := IngestTxnResponses(appCtx, "p1", input)

		want := []PutTxnResult{
			{
				Key:   failKey,
				Input: OpsTransaction{OpsFiMessage: OpsFiMessage{ProfileId: "p1", OriginTopic: infra.TxnResponseTopic}, Data: input.Transaction[0]},
			},
		}
		testutil.Equal(t, want, putResults, putResultOpts[PutTxnResult]()...)
	})

	t.Run("CnctRefresh", func(t *testing.T) {
		failKey := "p1/1/90/2025-06-12"
		app := setupTest(failKey)
		appCtx := AppContext{Context: ctx, App: app}

		input := []yodlee.DataExtractsProviderAccount{
			{
				Id:          90,
				LastUpdated: "2025-06-12",
				RequestId:   "REQUEST",
			},
		}
		result := IngestCnctRefreshes(appCtx, "p1", input)

		want := CnctRefreshResult{
			PutResults: []PutResult[OpsProviderAccountRefresh]{
				{
					Key:   failKey,
					Input: OpsProviderAccountRefresh{OpsFiMessage: OpsFiMessage{ProfileId: "p1", OriginTopic: infra.CnctRefreshTopic}, Data: input[0]},
				},
			},
		}
		testutil.Equal(t, want, result, putResultOpts[PutResult[OpsProviderAccountRefresh]]()...)
	})

	t.Run("AcctRefresh", func(t *testing.T) {
		failKey := "p1/1/90/900/2025-06-12"
		app := setupTest(failKey)
		appCtx := AppContext{Context: ctx, App: app}

		input := []yodlee.DataExtractsAccount{
			{
				ProviderAccountId: 90,
				Id:                900,
				LastUpdated:       "2025-06-12",
				AccountName:       "Checking Data",
			},
		}

		result := IngestAcctsRefreshes(appCtx, "p1", input)

		want := AcctRefreshResult{
			PutResults: []PutResult[OpsAccountRefresh]{
				{
					Key:   failKey,
					Input: OpsAccountRefresh{OpsFiMessage: OpsFiMessage{ProfileId: "p1", OriginTopic: infra.AcctRefreshTopic}, Data: input[0]},
				},
			},
		}
		testutil.Equal(t, want, result, putResultOpts[PutResult[OpsAccountRefresh]]()...)
	})

	t.Run("HoldRefresh", func(t *testing.T) {
		failKey := "p1/1/900/9000/2025-06-12"
		app := setupTest(failKey)
		appCtx := AppContext{Context: ctx, App: app}

		input := []yodlee.DataExtractsHolding{
			{
				AccountId:   900,
				Id:          9000,
				LastUpdated: "2025-06-12",
				HoldingType: "Security",
			},
		}

		result := IngestHoldRefreshes(appCtx, "p1", input)

		want := HoldRefreshResult{
			PutResults: []PutResult[OpsHoldingRefresh]{
				{
					Key:   failKey,
					Input: OpsHoldingRefresh{OpsFiMessage: OpsFiMessage{ProfileId: "p1", OriginTopic: infra.HoldRefreshTopic}, Data: input[0]},
				},
			},
		}
		testutil.Equal(t, want, result, putResultOpts[PutResult[OpsHoldingRefresh]]()...)
	})

	t.Run("TxnRefresh", func(t *testing.T) {
		failKey := "p1/1/900/9000/2025-06-11T07:06:18Z"
		app := setupTest(failKey)
		appCtx := AppContext{Context: ctx, App: app}

		input := []yodlee.DataExtractsTransaction{
			{
				AccountId:   900,
				Id:          9000,
				Date:        "2025-06-11T07:06:18Z",
				CheckNumber: "123",
			},
		}

		result := IngestTxnRefreshes(appCtx, "p1", input)

		want := TxnRefreshResult{
			PutResults: []PutResult[OpsTransactionRefresh]{
				{
					Key:   failKey,
					Input: OpsTransactionRefresh{OpsFiMessage: OpsFiMessage{ProfileId: "p1", OriginTopic: infra.TxnRefreshTopic}, Data: input[0]},
				},
			},
		}
		testutil.Equal(t, want, result, putResultOpts[PutResult[OpsTransactionRefresh]]()...)
	})
}

var deleteResultOpts = []cmp.Option{cmpopts.IgnoreFields(DeleteResult{}, "Err")}

func TestIngest_RefreshDeleteFailure(t *testing.T) {
	ctx := context.WithValue(t.Context(), "trace", t.Name())

	t.Run("CnctRefresh", func(t *testing.T) {
		app := setupIngestionTest(t)
		appCtx := AppContext{Context: ctx, App: app}

		infrastub.MakeBadS3Client(&app.AwsClient, infrastub.BadS3ClientCfg{
			FailListPrefix: map[infra.Bucket]string{
				app.TxnBucket: "p1/1/100", // fail to list txn by prefix
			},
			FailDeleteKeys: map[string]bool{
				"p1/1/100/1000/2025-06-12": true, // fail to delete a holding
			},
		})

		result := IngestCnctRefreshes(appCtx, "p1", []yodlee.DataExtractsProviderAccount{
			{
				IsDeleted: true,
				Id:        10, // contains account 100 and holding 1000 in seeded data.
			},
		})

		want := []DeleteResult{
			{Bucket: app.TxnBucket, Prefix: "p1/1/100"},
			{Bucket: app.HoldBucket, Keys: []string{"p1/1/100/1000/2025-06-12"}},
		}
		testutil.Equal(t, want, result.DeleteErrors, deleteResultOpts...)
	})

	t.Run("AcctRefresh", func(t *testing.T) {
		app := setupIngestionTest(t)
		appCtx := AppContext{Context: ctx, App: app}

		infrastub.MakeBadS3Client(&app.AwsClient, infrastub.BadS3ClientCfg{
			FailListPrefix: map[infra.Bucket]string{
				app.TxnBucket: "p1/1/100", // fail to list txn by prefix
			},
			FailDeleteKeys: map[string]bool{
				"p1/1/10/100/2025-06-12": true, // fail to delete an acct
			},
		})

		result := IngestAcctsRefreshes(appCtx, "p1", []yodlee.DataExtractsAccount{
			{
				IsDeleted:         true,
				ProviderAccountId: 10,
				Id:                100,
			},
		})

		want := []DeleteResult{
			{Bucket: app.TxnBucket, Prefix: "p1/1/100"},
			{Bucket: app.AcctBucket, Keys: []string{"p1/1/10/100/2025-06-12"}},
		}
		testutil.Equal(t, want, result.DeleteErrors, deleteResultOpts...)
	})
}
