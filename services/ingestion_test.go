package svc

import (
	"context"
	"testing"

	"yodleeops/client"
	"yodleeops/client/fakes"
	"yodleeops/testutil"
	"yodleeops/yodlee"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var fiMessageOpts = []cmp.Option{cmpopts.IgnoreFields(OpsFiMessage{}, "Timestamp")}

func setupIngestionTest(t *testing.T) *State {
	awsClient := testutil.SetupITest(t)

	state := &State{AWS: awsClient}
	state.AWS.PaginationLen = aws.Int32(1) // testing ListObjectsV2 pagination.

	return state
}

func TestIngestCnctResponses(t *testing.T) {
	// given
	ctx := context.WithValue(t.Context(), "trace", t.Name())

	state := setupIngestionTest(t)
	stateCtx := Context{Context: ctx, State: state}

	// when
	putResults := IngestCnctResponses(stateCtx, "p1", yodlee.ProviderAccountResponse{
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
			Bucket: client.CnctBucket,
			Key:    "p1/1/1/2025-06-12",
			Value: OpsProviderAccount{
				OpsFiMessage: OpsFiMessage{ProfileId: "p1", OriginTopic: client.CnctResponseTopic},
				Data:         yodlee.ProviderAccount{Id: 1, LastUpdated: "2025-06-12", RequestId: "REQUEST"},
			},
		},
		{
			Bucket: client.CnctBucket,
			Key:    "p1/1/100/2025-06-13",
			Value: OpsProviderAccount{
				OpsFiMessage: OpsFiMessage{ProfileId: "p1", OriginTopic: client.CnctResponseTopic},
				Data:         yodlee.ProviderAccount{Id: 100, LastUpdated: "2025-06-13", RequestId: "REQUEST"},
			},
		},
	}
	testutil.AssertObjects(t, &state.AWS, wantObjects, fiMessageOpts...)
}

func TestIngestAcctResponses(t *testing.T) {
	// given
	ctx := context.WithValue(t.Context(), "trace", t.Name())

	state := setupIngestionTest(t)
	stateCtx := Context{Context: ctx, State: state}

	// when
	putResults := IngestAcctResponses(stateCtx, "p1", yodlee.AccountResponse{
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
			Bucket: client.AcctBucket,
			Key:    "p1/1/1/1/2025-06-12",
			Value: OpsAccount{
				OpsFiMessage: OpsFiMessage{ProfileId: "p1", OriginTopic: client.AcctResponseTopic},
				Data:         yodlee.Account{ProviderAccountId: 1, Id: 1, LastUpdated: "2025-06-12", AccountName: "Checking Data"},
			},
		},
		{
			Bucket: client.AcctBucket,
			Key:    "p1/1/100/200/2025-06-13",
			Value: OpsAccount{
				OpsFiMessage: OpsFiMessage{ProfileId: "p1", OriginTopic: client.AcctResponseTopic},
				Data:         yodlee.Account{ProviderAccountId: 100, Id: 200, LastUpdated: "2025-06-13", AccountName: "Savings Data"},
			},
		},
	}
	testutil.AssertObjects(t, &state.AWS, wantObjects, fiMessageOpts...)
}

func TestIngestHoldResponses(t *testing.T) {
	// given
	ctx := context.WithValue(t.Context(), "trace", t.Name())

	state := setupIngestionTest(t)
	stateCtx := Context{Context: ctx, State: state}

	// when
	putResults := IngestHoldResponses(stateCtx, "p1", yodlee.HoldingResponse{
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
			Bucket: client.HoldBucket,
			Key:    "p1/1/1/1/2025-06-12",
			Value: OpsHolding{
				OpsFiMessage: OpsFiMessage{ProfileId: "p1", OriginTopic: client.HoldResponseTopic},
				Data:         yodlee.Holding{AccountId: 1, Id: 1, LastUpdated: "2025-06-12", HoldingType: "Security"},
			},
		},
		{
			Bucket: client.HoldBucket,
			Key:    "p1/1/200/100/2025-06-13",
			Value: OpsHolding{
				OpsFiMessage: OpsFiMessage{ProfileId: "p1", OriginTopic: client.HoldResponseTopic},
				Data:         yodlee.Holding{AccountId: 200, Id: 100, LastUpdated: "2025-06-13", HoldingType: "Stock"},
			},
		},
	}
	testutil.AssertObjects(t, &state.AWS, wantObjects, fiMessageOpts...)
}

func TestIngestTxnResponses(t *testing.T) {
	// given
	ctx := context.WithValue(t.Context(), "trace", t.Name())

	state := setupIngestionTest(t)
	stateCtx := Context{Context: ctx, State: state}

	// when
	putResults := IngestTxnResponses(stateCtx, "p1", yodlee.TransactionResponse{
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
			Bucket: client.TxnBucket,
			Key:    "p1/1/1/1/2025-06-11T07:06:18Z",
			Value: OpsTransaction{
				OpsFiMessage: OpsFiMessage{ProfileId: "p1", OriginTopic: client.TxnResponseTopic},
				Data:         yodlee.TransactionWithDateTime{AccountId: 1, Id: 1, Date: "2025-06-11T07:06:18Z", CheckNumber: "123"},
			},
		},
		{
			Bucket: client.TxnBucket,
			Key:    "p1/1/200/200/2025-06-13T07:06:18Z",
			Value: OpsTransaction{
				OpsFiMessage: OpsFiMessage{ProfileId: "p1", OriginTopic: client.TxnResponseTopic},
				Data:         yodlee.TransactionWithDateTime{AccountId: 200, Id: 200, Date: "2025-06-13T07:06:18Z", CheckNumber: "123"},
			},
		},
	}
	testutil.AssertObjects(t, &state.AWS, wantObjects, fiMessageOpts...)
}

func TestIngestCnctRefreshes(t *testing.T) {
	// given
	ctx := context.WithValue(t.Context(), "trace", t.Name())

	state := setupIngestionTest(t)
	stateCtx := Context{Context: ctx, State: state}

	// when
	result := IngestCnctRefreshes(stateCtx, "p1", []yodlee.DataExtractsProviderAccount{
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
			Bucket: client.CnctBucket,
			Key:    "p1/1/99/2025-06-13",
			Value: OpsProviderAccountRefresh{
				OpsFiMessage: OpsFiMessage{ProfileId: "p1", OriginTopic: client.CnctRefreshTopic},
				Data:         yodlee.DataExtractsProviderAccount{Id: 99, LastUpdated: "2025-06-13", RequestId: "REQUEST"},
			},
		},
	}
	testutil.AssertObjects(t, &state.AWS, wantObjects, fiMessageOpts...)

	// removed keys are commented.
	wantKeys := []testutil.WantKey{
		// Connections
		//{Bucket: infra.CnctBucket, Key: "p1/1/10/2025-06-12"},
		//{Bucket: infra.CnctBucket, Key: "p1/1/10/2025-06-13"},
		{Bucket: client.CnctBucket, Key: "p1/1/20/2025-06-14"},
		{Bucket: client.CnctBucket, Key: "p1/1/30/2025-06-15"},
		{Bucket: client.CnctBucket, Key: "p1/1/99/2025-06-13"},

		// Accounts
		//{Bucket: infra.AcctBucket, Key: "p1/1/10/100/2025-06-12"},
		//{Bucket: infra.AcctBucket, Key: "p1/1/10/100/2025-06-13"},
		{Bucket: client.AcctBucket, Key: "p2/1/20/200/2025-06-14"},
		{Bucket: client.AcctBucket, Key: "p2/1/30/400/2025-06-15"},

		// Holdings
		//{Bucket: infra.HoldBucket, Key: "p1/1/100/1000/2025-06-12"},
		//{Bucket: infra.HoldBucket, Key: "p1/1/100/1000/2025-06-13"},
		{Bucket: client.HoldBucket, Key: "p2/1/100/1000/2025-06-14"},
		{Bucket: client.HoldBucket, Key: "p2/1/200/2000/2025-06-15"},

		// Transactions
		//{Bucket: infra.TxnBucket, Key: "p1/1/100/3000/2025-06-12T00:14:37Z"},
		//{Bucket: infra.TxnBucket, Key: "p1/1/100/3000/2025-06-12T02:48:09Z"},
		{Bucket: client.TxnBucket, Key: "p2/1/100/3000/2025-06-13T02:48:09Z"},
		{Bucket: client.TxnBucket, Key: "p2/1/200/2000/2025-06-14T07:06:18Z"},
	}
	assert.ElementsMatch(t, wantKeys, testutil.GetAllKeys(t, state.AWS))
}

func TestIngestAcctRefreshes(t *testing.T) {
	// given
	ctx := context.WithValue(t.Context(), "trace", t.Name())

	state := setupIngestionTest(t)
	stateCtx := Context{Context: ctx, State: state}

	// when
	result := IngestAcctsRefreshes(stateCtx, "p1", []yodlee.DataExtractsAccount{
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
			Bucket: client.AcctBucket,
			Key:    "p1/1/99/999/2025-06-13",
			Value: OpsAccountRefresh{
				OpsFiMessage: OpsFiMessage{ProfileId: "p1", OriginTopic: client.AcctRefreshTopic},
				Data:         yodlee.DataExtractsAccount{ProviderAccountId: 99, Id: 999, LastUpdated: "2025-06-13", AccountName: "Savings Data"},
			},
		},
	}
	testutil.AssertObjects(t, &state.AWS, wantObjects, fiMessageOpts...)

	// removed keys are commented.
	wantKeys := []testutil.WantKey{
		// Connections
		{Bucket: client.CnctBucket, Key: "p1/1/10/2025-06-12"},
		{Bucket: client.CnctBucket, Key: "p1/1/10/2025-06-13"},
		{Bucket: client.CnctBucket, Key: "p1/1/20/2025-06-14"},
		{Bucket: client.CnctBucket, Key: "p1/1/30/2025-06-15"},

		// Accounts
		//{Bucket: infra.AcctBucket, Key: "p1/1/10/100/2025-06-12"},
		//{Bucket: infra.AcctBucket, Key: "p1/1/10/100/2025-06-13"},
		{Bucket: client.AcctBucket, Key: "p2/1/20/200/2025-06-14"},
		{Bucket: client.AcctBucket, Key: "p2/1/30/400/2025-06-15"},
		{Bucket: client.AcctBucket, Key: "p1/1/99/999/2025-06-13"},

		// Holdings
		//{Bucket: infra.HoldBucket, Key: "p1/1/100/1000/2025-06-12"},
		//{Bucket: infra.HoldBucket, Key: "p1/1/100/1000/2025-06-13"},
		{Bucket: client.HoldBucket, Key: "p2/1/100/1000/2025-06-14"},
		{Bucket: client.HoldBucket, Key: "p2/1/200/2000/2025-06-15"},

		// Transactions
		//{Bucket: infra.TxnBucket, Key: "p1/1/100/3000/2025-06-12T00:14:37Z"},
		//{Bucket: infra.TxnBucket, Key: "p1/1/100/3000/2025-06-12T02:48:09Z"},
		{Bucket: client.TxnBucket, Key: "p2/1/100/3000/2025-06-13T02:48:09Z"},
		{Bucket: client.TxnBucket, Key: "p2/1/200/2000/2025-06-14T07:06:18Z"},
	}
	assert.ElementsMatch(t, wantKeys, testutil.GetAllKeys(t, state.AWS))
}

func TestIngestTxnRefreshes(t *testing.T) {
	// given
	ctx := context.WithValue(t.Context(), "trace", t.Name())

	state := setupIngestionTest(t)
	stateCtx := Context{Context: ctx, State: state}

	// when
	result := IngestTxnRefreshes(stateCtx, "p1", []yodlee.DataExtractsTransaction{
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
			Bucket: client.TxnBucket,
			Key:    "p1/1/999/9999/2025-06-13T07:06:18Z",
			Value: OpsTransactionRefresh{
				OpsFiMessage: OpsFiMessage{ProfileId: "p1", OriginTopic: client.TxnRefreshTopic},
				Data:         yodlee.DataExtractsTransaction{AccountId: 999, Id: 9999, Date: "2025-06-13T07:06:18Z", CheckNumber: "123"},
			},
		},
	}
	testutil.AssertObjects(t, &state.AWS, wantObjects, fiMessageOpts...)

	// removed keys are commented.
	wantKeys := []testutil.WantKey{
		// Connections
		{Bucket: client.CnctBucket, Key: "p1/1/10/2025-06-12"},
		{Bucket: client.CnctBucket, Key: "p1/1/10/2025-06-13"},
		{Bucket: client.CnctBucket, Key: "p1/1/20/2025-06-14"},
		{Bucket: client.CnctBucket, Key: "p1/1/30/2025-06-15"},

		// Accounts
		{Bucket: client.AcctBucket, Key: "p1/1/10/100/2025-06-12"},
		{Bucket: client.AcctBucket, Key: "p1/1/10/100/2025-06-13"},
		{Bucket: client.AcctBucket, Key: "p2/1/20/200/2025-06-14"},
		{Bucket: client.AcctBucket, Key: "p2/1/30/400/2025-06-15"},

		// Holdings
		{Bucket: client.HoldBucket, Key: "p1/1/100/1000/2025-06-12"},
		{Bucket: client.HoldBucket, Key: "p1/1/100/1000/2025-06-13"},
		{Bucket: client.HoldBucket, Key: "p2/1/100/1000/2025-06-14"},
		{Bucket: client.HoldBucket, Key: "p2/1/200/2000/2025-06-15"},

		// Transactions
		//{Bucket: infra.TxnBucket, Key: "p1/1/100/3000/2025-06-12T00:14:37Z"},
		//{Bucket: infra.TxnBucket, Key: "p1/1/100/3000/2025-06-12T02:48:09Z"},
		{Bucket: client.TxnBucket, Key: "p2/1/100/3000/2025-06-13T02:48:09Z"},
		{Bucket: client.TxnBucket, Key: "p2/1/200/2000/2025-06-14T07:06:18Z"},
		{Bucket: client.TxnBucket, Key: "p1/1/999/9999/2025-06-13T07:06:18Z"},
	}
	assert.ElementsMatch(t, wantKeys, testutil.GetAllKeys(t, state.AWS))
}

func TestIngestHoldRefreshes(t *testing.T) {
	// given
	ctx := context.WithValue(t.Context(), "trace", t.Name())

	state := setupIngestionTest(t)
	stateCtx := Context{Context: ctx, State: state}

	// when
	result := IngestHoldRefreshes(stateCtx, "p1", []yodlee.DataExtractsHolding{
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
			Bucket: client.HoldBucket,
			Key:    "p1/1/999/9999/2025-06-13",
			Value: OpsHoldingRefresh{
				OpsFiMessage: OpsFiMessage{ProfileId: "p1", OriginTopic: client.HoldRefreshTopic},
				Data:         yodlee.DataExtractsHolding{AccountId: 999, Id: 9999, LastUpdated: "2025-06-13", HoldingType: "Stock"},
			},
		},
	}
	testutil.AssertObjects(t, &state.AWS, wantObjects, fiMessageOpts...)

	// removed keys are commented.
	wantKeys := []testutil.WantKey{
		// Connections
		{Bucket: client.CnctBucket, Key: "p1/1/10/2025-06-12"},
		{Bucket: client.CnctBucket, Key: "p1/1/10/2025-06-13"},
		{Bucket: client.CnctBucket, Key: "p1/1/20/2025-06-14"},
		{Bucket: client.CnctBucket, Key: "p1/1/30/2025-06-15"},

		// Accounts
		{Bucket: client.AcctBucket, Key: "p1/1/10/100/2025-06-12"},
		{Bucket: client.AcctBucket, Key: "p1/1/10/100/2025-06-13"},
		{Bucket: client.AcctBucket, Key: "p2/1/20/200/2025-06-14"},
		{Bucket: client.AcctBucket, Key: "p2/1/30/400/2025-06-15"},

		// Holdings
		{Bucket: client.HoldBucket, Key: "p1/1/100/1000/2025-06-12"},
		{Bucket: client.HoldBucket, Key: "p1/1/100/1000/2025-06-13"},
		{Bucket: client.HoldBucket, Key: "p2/1/100/1000/2025-06-14"},
		{Bucket: client.HoldBucket, Key: "p2/1/200/2000/2025-06-15"},
		{Bucket: client.HoldBucket, Key: "p1/1/999/9999/2025-06-13"},

		// Transactions
		{Bucket: client.TxnBucket, Key: "p1/1/100/3000/2025-06-12T00:14:37Z"},
		{Bucket: client.TxnBucket, Key: "p1/1/100/3000/2025-06-12T02:48:09Z"},
		{Bucket: client.TxnBucket, Key: "p2/1/100/3000/2025-06-13T02:48:09Z"},
		{Bucket: client.TxnBucket, Key: "p2/1/200/2000/2025-06-14T07:06:18Z"},
	}
	assert.ElementsMatch(t, wantKeys, testutil.GetAllKeys(t, state.AWS))
}

func TestIngestDeleteRetries(t *testing.T) {
	// given
	ctx := context.WithValue(t.Context(), "trace", t.Name())

	state := setupIngestionTest(t)
	stateCtx := Context{Context: ctx, State: state}

	// when
	results := IngestDeleteRetries(stateCtx, []DeleteRetry{
		{
			Kind:   ListKind,
			Bucket: client.AcctBucket,
			Prefix: "p1/1/10",
		},
		{
			Kind:   DeleteKind,
			Bucket: client.TxnBucket,
			Keys:   []string{"p1/1/100/3000/2025-06-12T00:14:37Z", "p1/1/100/3000/2025-06-12T02:48:09Z"},
		},
	})
	require.Empty(t, results)

	// then
	// removed keys are commented.
	wantKeys := []testutil.WantKey{
		// Connections
		{Bucket: client.CnctBucket, Key: "p1/1/10/2025-06-12"},
		{Bucket: client.CnctBucket, Key: "p1/1/10/2025-06-13"},
		{Bucket: client.CnctBucket, Key: "p1/1/20/2025-06-14"},
		{Bucket: client.CnctBucket, Key: "p1/1/30/2025-06-15"},

		// Accounts
		//{Bucket: infra.AcctBucket, Key: "p1/1/10/100/2025-06-12"},
		//{Bucket: infra.AcctBucket, Key: "p1/1/10/100/2025-06-13"},
		{Bucket: client.AcctBucket, Key: "p2/1/20/200/2025-06-14"},
		{Bucket: client.AcctBucket, Key: "p2/1/30/400/2025-06-15"},

		// Holdings
		{Bucket: client.HoldBucket, Key: "p1/1/100/1000/2025-06-12"},
		{Bucket: client.HoldBucket, Key: "p1/1/100/1000/2025-06-13"},
		{Bucket: client.HoldBucket, Key: "p2/1/100/1000/2025-06-14"},
		{Bucket: client.HoldBucket, Key: "p2/1/200/2000/2025-06-15"},

		// Transactions
		//{Bucket: infra.TxnBucket, Key: "p1/1/100/3000/2025-06-12T00:14:37Z"},
		//{Bucket: infra.TxnBucket, Key: "p1/1/100/3000/2025-06-12T02:48:09Z"},
		{Bucket: client.TxnBucket, Key: "p2/1/100/3000/2025-06-13T02:48:09Z"},
		{Bucket: client.TxnBucket, Key: "p2/1/200/2000/2025-06-14T07:06:18Z"},
	}
	assert.ElementsMatch(t, wantKeys, testutil.GetAllKeys(t, state.AWS))
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

	setupTest := func(failKey string) *State {
		state := setupIngestionTest(t)
		fakes.MakeBadS3Client(&state.AWS, fakes.BadS3Config{
			FailPutKey: failKey,
		})
		return state
	}

	t.Run("CnctResponse", func(t *testing.T) {
		failKey := "p1/1/90/2025-06-12"
		state := setupTest(failKey)
		stateCtx := Context{Context: ctx, State: state}

		input := yodlee.ProviderAccountResponse{
			ProviderAccount: []yodlee.ProviderAccount{
				{
					Id:          90,
					LastUpdated: "2025-06-12",
					RequestId:   "REQUEST",
				},
			},
		}
		putResults := IngestCnctResponses(stateCtx, "p1", input)

		want := []PutCnctResult{
			{
				Key:   failKey,
				Input: OpsProviderAccount{OpsFiMessage: OpsFiMessage{ProfileId: "p1", OriginTopic: client.CnctResponseTopic}, Data: input.ProviderAccount[0]},
			},
		}
		testutil.Equal(t, want, putResults, putResultOpts[PutCnctResult]()...)
	})

	t.Run("AcctResponse", func(t *testing.T) {
		failKey := "p1/1/90/900/2025-06-12"
		state := setupTest(failKey)
		stateCtx := Context{Context: ctx, State: state}

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
		putResults := IngestAcctResponses(stateCtx, "p1", input)

		want := []PutAcctResult{
			{
				Key:   failKey,
				Input: OpsAccount{OpsFiMessage: OpsFiMessage{ProfileId: "p1", OriginTopic: client.AcctResponseTopic}, Data: input.Account[0]},
			},
		}
		testutil.Equal(t, want, putResults, putResultOpts[PutAcctResult]()...)
	})

	t.Run("HoldResponse", func(t *testing.T) {
		failKey := "p1/1/900/9000/2025-06-12"
		state := setupTest(failKey)
		stateCtx := Context{Context: ctx, State: state}

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
		putResults := IngestHoldResponses(stateCtx, "p1", input)

		want := []PutHoldResult{
			{
				Key:   failKey,
				Input: OpsHolding{OpsFiMessage: OpsFiMessage{ProfileId: "p1", OriginTopic: client.HoldResponseTopic}, Data: input.Holding[0]},
			},
		}
		testutil.Equal(t, want, putResults, putResultOpts[PutHoldResult]()...)
	})

	t.Run("TxnResponse", func(t *testing.T) {
		failKey := "p1/1/900/9000/2025-06-11T07:06:18Z"
		state := setupTest(failKey)
		stateCtx := Context{Context: ctx, State: state}

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
		putResults := IngestTxnResponses(stateCtx, "p1", input)

		want := []PutTxnResult{
			{
				Key:   failKey,
				Input: OpsTransaction{OpsFiMessage: OpsFiMessage{ProfileId: "p1", OriginTopic: client.TxnResponseTopic}, Data: input.Transaction[0]},
			},
		}
		testutil.Equal(t, want, putResults, putResultOpts[PutTxnResult]()...)
	})

	t.Run("CnctRefresh", func(t *testing.T) {
		failKey := "p1/1/90/2025-06-12"
		state := setupTest(failKey)
		stateCtx := Context{Context: ctx, State: state}

		input := []yodlee.DataExtractsProviderAccount{
			{
				Id:          90,
				LastUpdated: "2025-06-12",
				RequestId:   "REQUEST",
			},
		}
		result := IngestCnctRefreshes(stateCtx, "p1", input)

		want := CnctRefreshResult{
			PutResults: []PutResult[OpsProviderAccountRefresh]{
				{
					Key:   failKey,
					Input: OpsProviderAccountRefresh{OpsFiMessage: OpsFiMessage{ProfileId: "p1", OriginTopic: client.CnctRefreshTopic}, Data: input[0]},
				},
			},
		}
		testutil.Equal(t, want, result, putResultOpts[PutResult[OpsProviderAccountRefresh]]()...)
	})

	t.Run("AcctRefresh", func(t *testing.T) {
		failKey := "p1/1/90/900/2025-06-12"
		state := setupTest(failKey)
		stateCtx := Context{Context: ctx, State: state}

		input := []yodlee.DataExtractsAccount{
			{
				ProviderAccountId: 90,
				Id:                900,
				LastUpdated:       "2025-06-12",
				AccountName:       "Checking Data",
			},
		}

		result := IngestAcctsRefreshes(stateCtx, "p1", input)

		want := AcctRefreshResult{
			PutResults: []PutResult[OpsAccountRefresh]{
				{
					Key:   failKey,
					Input: OpsAccountRefresh{OpsFiMessage: OpsFiMessage{ProfileId: "p1", OriginTopic: client.AcctRefreshTopic}, Data: input[0]},
				},
			},
		}
		testutil.Equal(t, want, result, putResultOpts[PutResult[OpsAccountRefresh]]()...)
	})

	t.Run("HoldRefresh", func(t *testing.T) {
		failKey := "p1/1/900/9000/2025-06-12"
		state := setupTest(failKey)
		stateCtx := Context{Context: ctx, State: state}

		input := []yodlee.DataExtractsHolding{
			{
				AccountId:   900,
				Id:          9000,
				LastUpdated: "2025-06-12",
				HoldingType: "Security",
			},
		}

		result := IngestHoldRefreshes(stateCtx, "p1", input)

		want := HoldRefreshResult{
			PutResults: []PutResult[OpsHoldingRefresh]{
				{
					Key:   failKey,
					Input: OpsHoldingRefresh{OpsFiMessage: OpsFiMessage{ProfileId: "p1", OriginTopic: client.HoldRefreshTopic}, Data: input[0]},
				},
			},
		}
		testutil.Equal(t, want, result, putResultOpts[PutResult[OpsHoldingRefresh]]()...)
	})

	t.Run("TxnRefresh", func(t *testing.T) {
		failKey := "p1/1/900/9000/2025-06-11T07:06:18Z"
		state := setupTest(failKey)
		stateCtx := Context{Context: ctx, State: state}

		input := []yodlee.DataExtractsTransaction{
			{
				AccountId:   900,
				Id:          9000,
				Date:        "2025-06-11T07:06:18Z",
				CheckNumber: "123",
			},
		}

		result := IngestTxnRefreshes(stateCtx, "p1", input)

		want := TxnRefreshResult{
			PutResults: []PutResult[OpsTransactionRefresh]{
				{
					Key:   failKey,
					Input: OpsTransactionRefresh{OpsFiMessage: OpsFiMessage{ProfileId: "p1", OriginTopic: client.TxnRefreshTopic}, Data: input[0]},
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
		state := setupIngestionTest(t)
		stateCtx := Context{Context: ctx, State: state}

		fakes.MakeBadS3Client(&state.AWS, fakes.BadS3Config{
			FailListPrefix: map[client.Bucket]string{
				client.TxnBucket: "p1/1/100", // fail to list txn by prefix
			},
			FailDeleteKeys: map[string]bool{
				"p1/1/100/1000/2025-06-12": true, // fail to delete a holding
			},
		})

		result := IngestCnctRefreshes(stateCtx, "p1", []yodlee.DataExtractsProviderAccount{
			{
				IsDeleted: true,
				Id:        10, // contains account 100 and holding 1000 in seeded data.
			},
		})

		want := []DeleteResult{
			{Bucket: client.TxnBucket, Prefix: "p1/1/100"},
			{Bucket: client.HoldBucket, Keys: []string{"p1/1/100/1000/2025-06-12"}},
		}
		testutil.Equal(t, want, result.DeleteErrors, deleteResultOpts...)
	})

	t.Run("AcctRefresh", func(t *testing.T) {
		state := setupIngestionTest(t)
		stateCtx := Context{Context: ctx, State: state}

		fakes.MakeBadS3Client(&state.AWS, fakes.BadS3Config{
			FailListPrefix: map[client.Bucket]string{
				client.TxnBucket: "p1/1/100", // fail to list txn by prefix
			},
			FailDeleteKeys: map[string]bool{
				"p1/1/10/100/2025-06-12": true, // fail to delete an acct
			},
		})

		result := IngestAcctsRefreshes(stateCtx, "p1", []yodlee.DataExtractsAccount{
			{
				IsDeleted:         true,
				ProviderAccountId: 10,
				Id:                100,
			},
		})

		want := []DeleteResult{
			{Bucket: client.TxnBucket, Prefix: "p1/1/100"},
			{Bucket: client.AcctBucket, Keys: []string{"p1/1/10/100/2025-06-12"}},
		}
		testutil.Equal(t, want, result.DeleteErrors, deleteResultOpts...)
	})
}
