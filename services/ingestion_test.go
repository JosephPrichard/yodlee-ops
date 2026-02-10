package svc

import (
	"context"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/stretchr/testify/assert"
	"testing"
	"yodleeops/infra/stubs"
	"yodleeops/internal/yodlee"
	"yodleeops/testutil"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/stretchr/testify/require"
)

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

	// when
	putResults := app.IngestCnctResponses(ctx, "p1", yodlee.ProviderAccountResponse{
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
			Value:  OpsProviderAccount{ProfileId: "p1", ProviderAccount: yodlee.ProviderAccount{Id: 1, LastUpdated: "2025-06-12", RequestId: "REQUEST"}},
		},
		{
			Bucket: app.CnctBucket,
			Key:    "p1/1/100/2025-06-13",
			Value:  OpsProviderAccount{ProfileId: "p1", ProviderAccount: yodlee.ProviderAccount{Id: 100, LastUpdated: "2025-06-13", RequestId: "REQUEST"}},
		},
	}
	testutil.AssertObjects(t, app.AwsClient, wantObjects)
}

func TestIngestAcctResponses(t *testing.T) {
	// given
	ctx := context.WithValue(t.Context(), "trace", t.Name())

	app := setupIngestionTest(t)

	// when
	putResults := app.IngestAcctResponses(ctx, "p1", yodlee.AccountResponse{
		Account: []yodlee.Account{
			{
				ProviderAccountId: 1,
				Id:                1,
				LastUpdated:       "2025-06-12",
				AccountName:       "Checking Account",
			},
			{
				ProviderAccountId: 100,
				Id:                200,
				LastUpdated:       "2025-06-13",
				AccountName:       "Savings Account",
			},
		},
	})
	assert.Equal(t, 2, len(putResults))

	// then
	wantObjects := []testutil.WantObject[OpsAccount]{
		{
			Bucket: app.AcctBucket,
			Key:    "p1/1/1/1/2025-06-12",
			Value:  OpsAccount{ProfileId: "p1", Account: yodlee.Account{ProviderAccountId: 1, Id: 1, LastUpdated: "2025-06-12", AccountName: "Checking Account"}},
		},
		{
			Bucket: app.AcctBucket,
			Key:    "p1/1/100/200/2025-06-13",
			Value:  OpsAccount{ProfileId: "p1", Account: yodlee.Account{ProviderAccountId: 100, Id: 200, LastUpdated: "2025-06-13", AccountName: "Savings Account"}},
		},
	}
	testutil.AssertObjects(t, app.AwsClient, wantObjects)
}

func TestIngestHoldResponses(t *testing.T) {
	// given
	ctx := context.WithValue(t.Context(), "trace", t.Name())

	app := setupIngestionTest(t)
	// when
	putResults := app.IngestHoldResponses(ctx, "p1", yodlee.HoldingResponse{
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
			Value:  OpsHolding{ProfileId: "p1", Holding: yodlee.Holding{AccountId: 1, Id: 1, LastUpdated: "2025-06-12", HoldingType: "Security"}},
		},
		{
			Bucket: app.HoldBucket,
			Key:    "p1/1/200/100/2025-06-13",
			Value:  OpsHolding{ProfileId: "p1", Holding: yodlee.Holding{AccountId: 200, Id: 100, LastUpdated: "2025-06-13", HoldingType: "Stock"}},
		},
	}
	testutil.AssertObjects(t, app.AwsClient, wantObjects)
}

func TestIngestTxnResponses(t *testing.T) {
	// given
	ctx := context.WithValue(t.Context(), "trace", t.Name())

	app := setupIngestionTest(t)

	// when
	putResults := app.IngestTxnResponses(ctx, "p1", yodlee.TransactionResponse{
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
			Value: OpsTransaction{ProfileId: "p1",
				TransactionWithDateTime: yodlee.TransactionWithDateTime{AccountId: 1, Id: 1, Date: "2025-06-11T07:06:18Z", CheckNumber: "123"}},
		},
		{
			Bucket: app.TxnBucket,
			Key:    "p1/1/200/200/2025-06-13T07:06:18Z",
			Value: OpsTransaction{ProfileId: "p1",
				TransactionWithDateTime: yodlee.TransactionWithDateTime{AccountId: 200, Id: 200, Date: "2025-06-13T07:06:18Z", CheckNumber: "123"}},
		},
	}
	testutil.AssertObjects(t, app.AwsClient, wantObjects)
}

func TestIngestCnctRefreshes(t *testing.T) {
	// given
	ctx := context.WithValue(t.Context(), "trace", t.Name())

	app := setupIngestionTest(t)

	// when
	result := app.IngestCnctRefreshes(ctx, "p1", []yodlee.DataExtractsProviderAccount{
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
			Value:  OpsProviderAccountRefresh{ProfileId: "p1", DataExtractsProviderAccount: yodlee.DataExtractsProviderAccount{Id: 99, LastUpdated: "2025-06-13", RequestId: "REQUEST"}},
		},
	}
	testutil.AssertObjects(t, app.AwsClient, wantObjects)

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

	// when
	result := app.IngestAcctsRefreshes(ctx, "p1", []yodlee.DataExtractsAccount{
		{
			IsDeleted:         true,
			ProviderAccountId: 10,
			Id:                100,
		},
		{
			ProviderAccountId: 99,
			Id:                999,
			LastUpdated:       "2025-06-13",
			AccountName:       "Savings Account",
		},
	})
	assert.Equal(t, 1, len(result.PutResults))
	require.Empty(t, result.DeleteErrors)

	// then
	wantObjects := []testutil.WantObject[OpsAccountRefresh]{
		{
			Bucket: app.AcctBucket,
			Key:    "p1/1/99/999/2025-06-13",
			Value:  OpsAccountRefresh{ProfileId: "p1", DataExtractsAccount: yodlee.DataExtractsAccount{ProviderAccountId: 99, Id: 999, LastUpdated: "2025-06-13", AccountName: "Savings Account"}},
		},
	}
	testutil.AssertObjects(t, app.AwsClient, wantObjects)

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

	// when
	result := app.IngestTxnRefreshes(ctx, "p1", []yodlee.DataExtractsTransaction{
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
			Value:  OpsTransactionRefresh{ProfileId: "p1", DataExtractsTransaction: yodlee.DataExtractsTransaction{AccountId: 999, Id: 9999, Date: "2025-06-13T07:06:18Z", CheckNumber: "123"}},
		},
	}
	testutil.AssertObjects(t, app.AwsClient, wantObjects)

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

	// when
	result := app.IngestHoldRefreshes(ctx, "p1", []yodlee.DataExtractsHolding{
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
			Value:  OpsHoldingRefresh{ProfileId: "p1", DataExtractsHolding: yodlee.DataExtractsHolding{AccountId: 999, Id: 9999, LastUpdated: "2025-06-13", HoldingType: "Stock"}},
		},
	}
	testutil.AssertObjects(t, app.AwsClient, wantObjects)

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

	// when
	results := app.IngestDeleteRetries(ctx, []DeleteRetry{
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

func TestIngest_PutFailure(t *testing.T) {
	ctx := context.WithValue(t.Context(), "trace", t.Name())

	setupTest := func(failKey string) *App {
		app := setupIngestionTest(t)

		app.AwsClient.S3Client = infrastub.MakeBadS3Client(app.AwsClient.S3Client, infrastub.BadS3ClientCfg{
			FailPutKey: failKey,
		})

		return app
	}

	t.Run("CnctResponse", func(t *testing.T) {
		failKey := "p1/1/90/2025-06-12"
		app := setupTest(failKey)

		input := yodlee.ProviderAccountResponse{
			ProviderAccount: []yodlee.ProviderAccount{
				{
					Id:          90,
					LastUpdated: "2025-06-12",
					RequestId:   "REQUEST",
				},
			},
		}
		putResults := app.IngestCnctResponses(ctx, "p1", input)

		want := []PutCnctResult{
			{Key: failKey, Input: OpsProviderAccount{ProfileId: "p1", ProviderAccount: input.ProviderAccount[0]}},
		}
		testutil.Equal(t, want, putResults, cmpopts.IgnoreFields(PutCnctResult{}, "Err"))
	})

	t.Run("AcctResponse", func(t *testing.T) {
		failKey := "p1/1/90/900/2025-06-12"
		app := setupTest(failKey)

		input := yodlee.AccountResponse{
			Account: []yodlee.Account{
				{
					ProviderAccountId: 90,
					Id:                900,
					LastUpdated:       "2025-06-12",
					AccountName:       "Checking Account",
				},
			},
		}
		putResults := app.IngestAcctResponses(ctx, "p1", input)

		want := []PutAcctResult{
			{Key: failKey, Input: OpsAccount{ProfileId: "p1", Account: input.Account[0]}},
		}
		testutil.Equal(t, want, putResults, cmpopts.IgnoreFields(PutAcctResult{}, "Err"))
	})

	t.Run("HoldResponse", func(t *testing.T) {
		failKey := "p1/1/900/9000/2025-06-12"
		app := setupTest(failKey)

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
		putResults := app.IngestHoldResponses(ctx, "p1", input)

		want := []PutHoldResult{
			{Key: failKey, Input: OpsHolding{ProfileId: "p1", Holding: input.Holding[0]}},
		}
		testutil.Equal(t, want, putResults, cmpopts.IgnoreFields(PutHoldResult{}, "Err"))
	})

	t.Run("TxnResponse", func(t *testing.T) {
		failKey := "p1/1/900/9000/2025-06-11T07:06:18Z"
		app := setupTest(failKey)

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
		putResults := app.IngestTxnResponses(ctx, "p1", input)

		want := []PutTxnResult{
			{Key: failKey, Input: OpsTransaction{ProfileId: "p1", TransactionWithDateTime: input.Transaction[0]}},
		}
		testutil.Equal(t, want, putResults, cmpopts.IgnoreFields(PutTxnResult{}, "Err"))
	})

	t.Run("CnctRefresh", func(t *testing.T) {
		failKey := "p1/1/90/2025-06-12"
		app := setupTest(failKey)

		input := []yodlee.DataExtractsProviderAccount{
			{
				Id:          90,
				LastUpdated: "2025-06-12",
				RequestId:   "REQUEST",
			},
		}
		result := app.IngestCnctRefreshes(ctx, "p1", input)

		want := CnctRefreshResult{
			PutResults: []PutResult[OpsProviderAccountRefresh]{
				{Key: failKey, Input: OpsProviderAccountRefresh{ProfileId: "p1", DataExtractsProviderAccount: input[0]}},
			},
		}
		testutil.Equal(t, want, result, cmpopts.IgnoreFields(PutResult[OpsProviderAccountRefresh]{}, "Err"))
	})

	t.Run("AcctRefresh", func(t *testing.T) {
		failKey := "p1/1/90/900/2025-06-12"
		app := setupTest(failKey)

		input := []yodlee.DataExtractsAccount{
			{
				ProviderAccountId: 90,
				Id:                900,
				LastUpdated:       "2025-06-12",
				AccountName:       "Checking Account",
			},
		}

		result := app.IngestAcctsRefreshes(ctx, "p1", input)

		want := AcctRefreshResult{
			PutResults: []PutResult[OpsAccountRefresh]{
				{Key: failKey, Input: OpsAccountRefresh{ProfileId: "p1", DataExtractsAccount: input[0]}},
			},
		}
		testutil.Equal(t, want, result, cmpopts.IgnoreFields(PutResult[OpsAccountRefresh]{}, "Err"))
	})

	t.Run("HoldRefresh", func(t *testing.T) {
		failKey := "p1/1/900/9000/2025-06-12"
		app := setupTest(failKey)

		input := []yodlee.DataExtractsHolding{
			{
				AccountId:   900,
				Id:          9000,
				LastUpdated: "2025-06-12",
				HoldingType: "Security",
			},
		}

		result := app.IngestHoldRefreshes(ctx, "p1", input)

		want := HoldRefreshResult{
			PutResults: []PutResult[OpsHoldingRefresh]{
				{Key: failKey, Input: OpsHoldingRefresh{ProfileId: "p1", DataExtractsHolding: input[0]}},
			},
		}
		testutil.Equal(t, want, result, cmpopts.IgnoreFields(PutResult[OpsHoldingRefresh]{}, "Err"))
	})

	t.Run("TxnRefresh", func(t *testing.T) {
		failKey := "p1/1/900/9000/2025-06-11T07:06:18Z"
		app := setupTest(failKey)

		input := []yodlee.DataExtractsTransaction{
			{
				AccountId:   900,
				Id:          9000,
				Date:        "2025-06-11T07:06:18Z",
				CheckNumber: "123",
			},
		}

		result := app.IngestTxnRefreshes(ctx, "p1", input)

		want := TxnRefreshResult{
			PutResults: []PutResult[OpsTransactionRefresh]{
				{Key: failKey, Input: OpsTransactionRefresh{ProfileId: "p1", DataExtractsTransaction: input[0]}},
			},
		}
		testutil.Equal(t, want, result, cmpopts.IgnoreFields(PutResult[OpsTransactionRefresh]{}, "Err"))
	})
}

func TestIngest_RefreshDeleteFailure(t *testing.T) {
	ctx := context.WithValue(t.Context(), "trace", t.Name())

	t.Run("CnctRefresh", func(t *testing.T) {
		app := setupIngestionTest(t)

		app.AwsClient.S3Client = infrastub.MakeBadS3Client(app.AwsClient.S3Client, infrastub.BadS3ClientCfg{
			FailListPrefix: map[string]string{
				app.TxnBucket: "p1/1/100", // fail to list txn by prefix
			},
			FailDeleteKeys: []string{"p1/1/100/1000/2025-06-12"}, // fail to delete a holding
		})

		result := app.IngestCnctRefreshes(ctx, "p1", []yodlee.DataExtractsProviderAccount{
			{
				IsDeleted: true,
				Id:        10, // contains account 100 and holding 1000 in seeded data.
			},
		})

		want := []DeleteResult{
			{Bucket: app.TxnBucket, Prefix: "p1/1/100"},
			{Bucket: app.HoldBucket, Keys: []string{"p1/1/100/1000/2025-06-12"}},
		}
		testutil.Equal(t, want, result.DeleteErrors, cmpopts.IgnoreFields(DeleteResult{}, "Err"))
	})

	t.Run("AcctRefresh", func(t *testing.T) {
		app := setupIngestionTest(t)

		app.AwsClient.S3Client = infrastub.MakeBadS3Client(app.AwsClient.S3Client, infrastub.BadS3ClientCfg{
			FailListPrefix: map[string]string{
				app.TxnBucket: "p1/1/100", // fail to list txn by prefix
			},
			FailDeleteKeys: []string{"p1/1/10/100/2025-06-12"}, // fail to delete an acct
		})

		result := app.IngestAcctsRefreshes(ctx, "p1", []yodlee.DataExtractsAccount{
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
		testutil.Equal(t, want, result.DeleteErrors, cmpopts.IgnoreFields(DeleteResult{}, "Err"))
	})
}
