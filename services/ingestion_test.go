package svc

import (
	"context"
	cfg "filogger/config"
	"filogger/pb"
	"filogger/testutil"
	"github.com/google/go-cmp/cmp/cmpopts"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func makeTestAwsClient(t *testing.T) *App {
	testCfg := testutil.SetupITest(t, testutil.Aws)

	app := &App{AwsClient: cfg.MakeAwsClient(testCfg.AwsConfig)}
	app.AwsClient.PageLength = aws.Int32(1) // testing ListObjectsV2 pagination.

	testutil.SeedS3Buckets(t, &app.AwsClient)
	return app
}

func TestIngestCnctEnrichments(t *testing.T) {
	// given
	ctx := context.WithValue(t.Context(), "trace", t.Name())

	app := makeTestAwsClient(t)

	// when
	putErrors := app.IngestCnctEnrichments(ctx, []ExtnCnctEnrichment{
		{
			PrtyId:       "p1",
			PrtyIdTypeCd: "Y",
			ExtnCnctId:   "c1",
			BusDt:        "2025-06-12",
			VendorName:   "BankOfAmerica",
		},
		{
			PrtyId:       "p300",
			PrtyIdTypeCd: "Y",
			ExtnCnctId:   "c10",
			BusDt:        "2025-06-13",
			VendorName:   "GoldmanSachs",
		},
	})
	require.Empty(t, putErrors)

	// then
	wantObjects := []testutil.WantObject{
		{
			Bucket: app.CnctBucket,
			Key:    "p1/Y/c1/2025-06-12",
			Value:  &pb.ExtnCnctEntity{PrtyId: "p1", PrtyIdTypeCd: "Y", ExtnCnctId: "c1", BusDt: "2025-06-12", VendorName: "BankOfAmerica"},
		},
		{
			Bucket: app.CnctBucket,
			Key:    "p300/Y/c10/2025-06-13",
			Value:  &pb.ExtnCnctEntity{PrtyId: "p300", PrtyIdTypeCd: "Y", ExtnCnctId: "c10", BusDt: "2025-06-13", VendorName: "GoldmanSachs"},
		},
	}
	testutil.AssertProtoObjects(t, &app.AwsClient, wantObjects, func() proto.Message { return &pb.ExtnCnctEntity{} })
}

func TestIngestAcctEnrichments(t *testing.T) {
	// given
	ctx := context.WithValue(t.Context(), "trace", t.Name())

	app := makeTestAwsClient(t)

	// when
	putErrors := app.IngestAcctEnrichments(ctx, []ExtnAcctEnrichment{
		{
			PrtyId:       "p1",
			PrtyIdTypeCd: "Y",
			ExtnCnctId:   "c1",
			ExtnAcctId:   "a1",
			BusDt:        "2025-06-12",
			AcctName:     "Checking Account",
		},
		{
			PrtyId:       "p300",
			PrtyIdTypeCd: "Y",
			ExtnCnctId:   "c100",
			ExtnAcctId:   "a200",
			BusDt:        "2025-06-13",
			AcctName:     "Savings Account",
		},
	})
	require.Empty(t, putErrors)

	// then
	wantObjects := []testutil.WantObject{
		{
			Bucket: app.AcctBucket,
			Key:    "p1/Y/c1/a1/2025-06-12",
			Value:  &pb.ExtnAcctEntity{PrtyId: "p1", PrtyIdTypeCd: "Y", ExtnCnctId: "c1", ExtnAcctId: "a1", BusDt: "2025-06-12", AcctName: "Checking Account"},
		},
		{
			Bucket: app.AcctBucket,
			Key:    "p300/Y/c100/a200/2025-06-13",
			Value:  &pb.ExtnAcctEntity{PrtyId: "p300", PrtyIdTypeCd: "Y", ExtnCnctId: "c100", ExtnAcctId: "a200", BusDt: "2025-06-13", AcctName: "Savings Account"},
		},
	}
	testutil.AssertProtoObjects(t, &app.AwsClient, wantObjects, func() proto.Message { return &pb.ExtnAcctEntity{} })
}

func TestIngestHoldEnrichments(t *testing.T) {
	// given
	ctx := context.WithValue(t.Context(), "trace", t.Name())

	app := makeTestAwsClient(t)
	// when
	putErrors := app.IngestHoldEnrichments(ctx, []ExtnHoldEnrichment{
		{
			PrtyId:       "p1",
			PrtyIdTypeCd: "Y",
			ExtnAcctId:   "a1",
			ExtnHoldId:   "h1",
			BusDt:        "2025-06-12",
			HoldName:     "Security",
		},
		{
			PrtyId:       "p300",
			PrtyIdTypeCd: "Y",
			ExtnAcctId:   "a200",
			ExtnHoldId:   "h100",
			BusDt:        "2025-06-13",
			HoldName:     "Stock",
		},
	})
	require.Empty(t, putErrors)

	// then
	wantObjects := []testutil.WantObject{
		{
			Bucket: app.HoldBucket,
			Key:    "p1/Y/a1/h1/2025-06-12",
			Value:  &pb.ExtnHoldEntity{PrtyId: "p1", PrtyIdTypeCd: "Y", ExtnAcctId: "a1", ExtnHoldId: "h1", BusDt: "2025-06-12", HoldName: "Security"},
		},
		{
			Bucket: app.HoldBucket,
			Key:    "p300/Y/a200/h100/2025-06-13",
			Value:  &pb.ExtnHoldEntity{PrtyId: "p300", PrtyIdTypeCd: "Y", ExtnAcctId: "a200", ExtnHoldId: "h100", BusDt: "2025-06-13", HoldName: "Stock"},
		},
	}
	testutil.AssertProtoObjects(t, &app.AwsClient, wantObjects, func() proto.Message { return &pb.ExtnHoldEntity{} })
}

func TestIngestTxnEnrichments(t *testing.T) {
	// given
	ctx := context.WithValue(t.Context(), "trace", t.Name())

	app := makeTestAwsClient(t)

	// when
	putErrors := app.IngestTxnEnrichments(ctx, []ExtnTxnEnrichment{
		// both of these insert new records because txns are completely unique due to timestamp being part of the key.
		{
			PrtyId:       "p1",
			PrtyIdTypeCd: "Y",
			ExtnAcctId:   "a1",
			ExtnTxnId:    "t1",
			BusDt:        "2025-06-11",
			TxnDt:        "2025-06-11T07:06:18Z",
			TxnAmt:       4523,
		},
		{
			PrtyId:       "p300",
			PrtyIdTypeCd: "Y",
			ExtnAcctId:   "a200",
			ExtnTxnId:    "t200",
			BusDt:        "2025-06-13",
			TxnDt:        "2025-06-13T07:06:18Z",
			TxnAmt:       -1299,
		},
	})
	require.Empty(t, putErrors)

	// then
	wantObjects := []testutil.WantObject{
		{
			Bucket: app.TxnBucket,
			Key:    "p1/Y/a1/t1/2025-06-11T07:06:18Z",
			Value:  &pb.ExtnTxnEntity{PrtyId: "p1", PrtyIdTypeCd: "Y", ExtnAcctId: "a1", ExtnTxnId: "t1", BusDt: "2025-06-11", TxnDt: "2025-06-11T07:06:18Z", TxnAmt: 4523},
		},
		{
			Bucket: app.TxnBucket,
			Key:    "p300/Y/a200/t200/2025-06-13T07:06:18Z",
			Value:  &pb.ExtnTxnEntity{PrtyId: "p300", PrtyIdTypeCd: "Y", ExtnAcctId: "a200", ExtnTxnId: "t200", BusDt: "2025-06-13", TxnDt: "2025-06-13T07:06:18Z", TxnAmt: -1299},
		},
	}
	testutil.AssertProtoObjects(t, &app.AwsClient, wantObjects, func() proto.Message { return &pb.ExtnTxnEntity{} })
}

func TestIngestCnctRefreshes(t *testing.T) {
	// given
	ctx := context.WithValue(t.Context(), "trace", t.Name())

	app := makeTestAwsClient(t)

	// when
	result := app.IngestCnctRefreshes(ctx, []ExtnCnctRefresh{
		{
			IsDeleted:    true,
			PrtyId:       "p1",
			PrtyIdTypeCd: "Y",
			ExtnCnctId:   "c1",
			BusDt:        "2025-06-12",
			VendorName:   "BankOfAmerica",
		},
		{
			PrtyId:       "p300",
			PrtyIdTypeCd: "Y",
			ExtnCnctId:   "c10",
			BusDt:        "2025-06-13",
			VendorName:   "GoldmanSachs",
		},
	})
	require.Empty(t, result.PutErrors)
	require.Empty(t, result.DeleteErrors)

	// then
	wantObjects := []testutil.WantObject{
		{
			Bucket: app.CnctBucket,
			Key:    "p300/Y/c10/2025-06-13",
			Value:  &pb.ExtnCnctEntity{PrtyId: "p300", PrtyIdTypeCd: "Y", ExtnCnctId: "c10", BusDt: "2025-06-13", VendorName: "GoldmanSachs"},
		},
	}
	testutil.AssertProtoObjects(t, &app.AwsClient, wantObjects, func() proto.Message { return &pb.ExtnCnctEntity{} })

	// removed keys are commented.
	wantKeys := []string{
		//"p1/Y/c1/2025-06-12",
		//"p1/Y/c1/2025-06-13",
		"p1/Y/c2/2025-06-14",
		"p1/Y/c3/2025-06-15",
		"p300/Y/c10/2025-06-13",

		//"p1/Y/c1/a1/2025-06-12",
		//"p1/Y/c1/a1/2025-06-13",
		"p2/Y/c2/a2/2025-06-14",
		"p2/Y/c3/a3/2025-06-15",

		//"p1/Y/a1/h1/2025-06-12",
		//"p1/Y/a1/h1/2025-06-13",
		"p2/Y/a1/h1/2025-06-14",
		"p2/Y/a2/h2/2025-06-15",

		//"p1/Y/a1/t1/2025-06-12T00:14:37Z",
		//"p1/Y/a1/t1/2025-06-12T02:48:09Z",
		"p2/Y/a1/t1/2025-06-13T02:48:09Z",
		"p2/Y/a2/t2/2025-06-14T07:06:18Z",
	}
	assert.ElementsMatch(t, wantKeys, testutil.GetAllKeys(t, &app.AwsClient))
}

func TestIngestAcctRefreshes(t *testing.T) {
	// given
	ctx := context.WithValue(t.Context(), "trace", t.Name())

	app := makeTestAwsClient(t)

	// when
	result := app.IngestAcctsRefreshes(ctx, []ExtnAcctRefresh{
		{
			IsDeleted:    true,
			PrtyId:       "p1",
			PrtyIdTypeCd: "Y",
			ExtnCnctId:   "c1",
			ExtnAcctId:   "a1",
			BusDt:        "2025-06-12",
			AcctName:     "Checking Account",
		},
		{
			PrtyId:       "p300",
			PrtyIdTypeCd: "Y",
			ExtnCnctId:   "c100",
			ExtnAcctId:   "a200",
			BusDt:        "2025-06-13",
			AcctName:     "Savings Account",
		},
	})
	require.Empty(t, result.PutErrors)
	require.Empty(t, result.DeleteErrors)

	// then
	wantObjects := []testutil.WantObject{
		{
			Bucket: app.AcctBucket,
			Key:    "p300/Y/c100/a200/2025-06-13",
			Value:  &pb.ExtnAcctEntity{PrtyId: "p300", PrtyIdTypeCd: "Y", ExtnCnctId: "c100", ExtnAcctId: "a200", BusDt: "2025-06-13", AcctName: "Savings Account"},
		},
	}
	testutil.AssertProtoObjects(t, &app.AwsClient, wantObjects, func() proto.Message { return &pb.ExtnAcctEntity{} })

	// removed keys are commented.
	wantKeys := []string{
		"p1/Y/c1/2025-06-12",
		"p1/Y/c1/2025-06-13",
		"p1/Y/c2/2025-06-14",
		"p1/Y/c3/2025-06-15",

		//"p1/Y/c1/a1/2025-06-12",
		//"p1/Y/c1/a1/2025-06-13",
		"p2/Y/c2/a2/2025-06-14",
		"p2/Y/c3/a3/2025-06-15",
		"p300/Y/c100/a200/2025-06-13",

		//"p1/Y/a1/h1/2025-06-12",
		//"p1/Y/a1/h1/2025-06-13",
		"p2/Y/a1/h1/2025-06-14",
		"p2/Y/a2/h2/2025-06-15",

		//"p1/Y/a1/t1/2025-06-12T00:14:37Z",
		//"p1/Y/a1/t1/2025-06-12T02:48:09Z",
		"p2/Y/a1/t1/2025-06-13T02:48:09Z",
		"p2/Y/a2/t2/2025-06-14T07:06:18Z",
	}
	assert.ElementsMatch(t, wantKeys, testutil.GetAllKeys(t, &app.AwsClient))
}

func TestIngestTxnRefreshes(t *testing.T) {
	// given
	ctx := context.WithValue(t.Context(), "trace", t.Name())

	app := makeTestAwsClient(t)

	// when
	result := app.IngestTxnRefreshes(ctx, []ExtnTxnRefresh{
		{
			IsDeleted:    true,
			PrtyId:       "p1",
			PrtyIdTypeCd: "Y",
			ExtnAcctId:   "a1",
			ExtnTxnId:    "t1",
			BusDt:        "2025-06-11",
			TxnDt:        "2025-06-11T07:06:18Z",
			TxnAmt:       4523,
		},
		{
			PrtyId:       "p300",
			PrtyIdTypeCd: "Y",
			ExtnAcctId:   "a200",
			ExtnTxnId:    "t200",
			BusDt:        "2025-06-13",
			TxnDt:        "2025-06-13T07:06:18Z",
			TxnAmt:       -1299,
		},
	})
	require.Empty(t, result.PutErrors)
	require.Empty(t, result.DeleteErrors)

	// then
	wantObjects := []testutil.WantObject{
		{
			Bucket: app.TxnBucket,
			Key:    "p300/Y/a200/t200/2025-06-13T07:06:18Z",
			Value:  &pb.ExtnTxnEntity{PrtyId: "p300", PrtyIdTypeCd: "Y", ExtnAcctId: "a200", ExtnTxnId: "t200", BusDt: "2025-06-13", TxnDt: "2025-06-13T07:06:18Z", TxnAmt: -1299},
		},
	}
	testutil.AssertProtoObjects(t, &app.AwsClient, wantObjects, func() proto.Message { return &pb.ExtnTxnEntity{} })

	// removed keys are commented.
	wantKeys := []string{
		"p1/Y/c1/2025-06-12",
		"p1/Y/c1/2025-06-13",
		"p1/Y/c2/2025-06-14",
		"p1/Y/c3/2025-06-15",

		"p1/Y/c1/a1/2025-06-12",
		"p1/Y/c1/a1/2025-06-13",
		"p2/Y/c2/a2/2025-06-14",
		"p2/Y/c3/a3/2025-06-15",

		"p1/Y/a1/h1/2025-06-12",
		"p1/Y/a1/h1/2025-06-13",
		"p2/Y/a1/h1/2025-06-14",
		"p2/Y/a2/h2/2025-06-15",

		//"p1/Y/a1/t1/2025-06-12T00:14:37Z",
		//"p1/Y/a1/t1/2025-06-12T02:48:09Z",
		"p2/Y/a1/t1/2025-06-13T02:48:09Z",
		"p2/Y/a2/t2/2025-06-14T07:06:18Z",
		"p300/Y/a200/t200/2025-06-13T07:06:18Z",
	}
	assert.ElementsMatch(t, wantKeys, testutil.GetAllKeys(t, &app.AwsClient))
}

func TestIngestHoldRefreshes(t *testing.T) {
	// given
	ctx := context.WithValue(t.Context(), "trace", t.Name())

	app := makeTestAwsClient(t)

	// when
	result := app.IngestHoldRefreshes(ctx, []ExtnHoldRefresh{
		{
			IsDeleted:    true,
			PrtyId:       "p1",
			PrtyIdTypeCd: "Y",
			ExtnAcctId:   "a1",
			ExtnHoldId:   "h1",
			BusDt:        "2025-06-12",
			HoldName:     "Security",
		},
		{
			PrtyId:       "p300",
			PrtyIdTypeCd: "Y",
			ExtnAcctId:   "a200",
			ExtnHoldId:   "h100",
			BusDt:        "2025-06-13",
			HoldName:     "Stock",
		},
	})
	require.Empty(t, result.PutErrors)
	require.Empty(t, result.DeleteErrors)

	// then
	wantObjects := []testutil.WantObject{
		{
			Bucket: app.HoldBucket,
			Key:    "p300/Y/a200/h100/2025-06-13",
			Value:  &pb.ExtnHoldEntity{PrtyId: "p300", PrtyIdTypeCd: "Y", ExtnAcctId: "a200", ExtnHoldId: "h100", BusDt: "2025-06-13", HoldName: "Stock"},
		},
	}
	testutil.AssertProtoObjects(t, &app.AwsClient, wantObjects, func() proto.Message { return &pb.ExtnHoldEntity{} })

	// removed keys are commented.
	wantKeys := []string{
		"p1/Y/c1/2025-06-12",
		"p1/Y/c1/2025-06-13",
		"p1/Y/c2/2025-06-14",
		"p1/Y/c3/2025-06-15",

		"p1/Y/c1/a1/2025-06-12",
		"p1/Y/c1/a1/2025-06-13",
		"p2/Y/c2/a2/2025-06-14",
		"p2/Y/c3/a3/2025-06-15",

		//"p1/Y/a1/h1/2025-06-12",
		//"p1/Y/a1/h1/2025-06-13",
		"p2/Y/a1/h1/2025-06-14",
		"p2/Y/a2/h2/2025-06-15",
		"p300/Y/a200/h100/2025-06-13",

		"p1/Y/a1/t1/2025-06-12T00:14:37Z",
		"p1/Y/a1/t1/2025-06-12T02:48:09Z",
		"p2/Y/a1/t1/2025-06-13T02:48:09Z",
		"p2/Y/a2/t2/2025-06-14T07:06:18Z",
	}
	assert.ElementsMatch(t, wantKeys, testutil.GetAllKeys(t, &app.AwsClient))
}

func TestIngestDeleteRetries(t *testing.T) {
	// given
	ctx := context.WithValue(t.Context(), "trace", t.Name())

	app := makeTestAwsClient(t)

	// when
	results := app.IngestDeleteRetries(ctx, []DeleteRetry{
		{
			Kind:   ListKind,
			Bucket: app.AcctBucket,
			Prefix: "p1/Y/c1",
		},
		{
			Kind:   DeleteKind,
			Bucket: app.HoldBucket,
			Keys:   []string{"p1/Y/a1/h1/2025-06-12", "p1/Y/a1/h1/2025-06-13"},
		},
		{
			Kind:   DeleteKind,
			Bucket: app.TxnBucket,
			Keys:   []string{"p1/Y/a1/t1/2025-06-12T00:14:37Z", "p1/Y/a1/t1/2025-06-12T02:48:09Z"},
		},
	})
	require.Empty(t, results)

	// then
	// removed keys are commented.
	wantKeys := []string{
		"p1/Y/c1/2025-06-12",
		"p1/Y/c1/2025-06-13",
		"p1/Y/c2/2025-06-14",
		"p1/Y/c3/2025-06-15",

		//"p1/Y/c1/a1/2025-06-12",
		//"p1/Y/c1/a1/2025-06-13",
		"p2/Y/c2/a2/2025-06-14",
		"p2/Y/c3/a3/2025-06-15",

		//"p1/Y/a1/h1/2025-06-12",
		//"p1/Y/a1/h1/2025-06-13",
		"p2/Y/a1/h1/2025-06-14",
		"p2/Y/a2/h2/2025-06-15",

		//"p1/Y/a1/t1/2025-06-12T00:14:37Z",
		//"p1/Y/a1/t1/2025-06-12T02:48:09Z",
		"p2/Y/a1/t1/2025-06-13T02:48:09Z",
		"p2/Y/a2/t2/2025-06-14T07:06:18Z",
	}
	assert.ElementsMatch(t, wantKeys, testutil.GetAllKeys(t, &app.AwsClient))
}

func TestIngest_PutFailure(t *testing.T) {
	ctx := context.WithValue(t.Context(), "trace", t.Name())

	setupTest := func(failKey string) *App {
		app := makeTestAwsClient(t)

		stable := app.AwsClient.S3Client
		app.AwsClient.S3Client = &cfg.StubUnstableS3Client{
			StableS3Client: stable,
			FailPutKey:     failKey,
		}

		return app
	}

	t.Run("CnctEnrichment", func(t *testing.T) {
		failKey := "p1/Y/c1/2025-06-12"
		app := setupTest(failKey)

		input := []ExtnCnctEnrichment{
			{
				PrtyId:       "p1",
				PrtyIdTypeCd: "Y",
				ExtnCnctId:   "c1",
				BusDt:        "2025-06-12",
				VendorName:   "BankOfAmerica",
			},
		}

		putErrors := app.IngestCnctEnrichments(ctx, input)

		want := []PutResult{
			{Key: failKey, Origin: input[0]},
		}
		testutil.Equal(t, want, putErrors, cmpopts.IgnoreFields(PutResult{}, "Err"))
	})

	t.Run("AcctEnrichment", func(t *testing.T) {
		failKey := "p1/Y/c1/a1/2025-06-12"
		app := setupTest(failKey)

		input := []ExtnAcctEnrichment{
			{
				PrtyId:       "p1",
				PrtyIdTypeCd: "Y",
				ExtnCnctId:   "c1",
				ExtnAcctId:   "a1",
				BusDt:        "2025-06-12",
				AcctName:     "Checking Account",
			},
		}

		putErrors := app.IngestAcctEnrichments(ctx, input)

		want := []PutResult{
			{Key: failKey, Origin: input[0]},
		}
		testutil.Equal(t, want, putErrors, cmpopts.IgnoreFields(PutResult{}, "Err"))
	})

	t.Run("HoldEnrichment", func(t *testing.T) {
		failKey := "p1/Y/a1/h1/2025-06-12"
		app := setupTest(failKey)

		input := []ExtnHoldEnrichment{
			{
				PrtyId:       "p1",
				PrtyIdTypeCd: "Y",
				ExtnAcctId:   "a1",
				ExtnHoldId:   "h1",
				BusDt:        "2025-06-12",
				HoldName:     "Security",
			},
		}

		putErrors := app.IngestHoldEnrichments(ctx, input)

		want := []PutResult{
			{Key: failKey, Origin: input[0]},
		}
		testutil.Equal(t, want, putErrors, cmpopts.IgnoreFields(PutResult{}, "Err"))
	})

	t.Run("TxnEnrichment", func(t *testing.T) {
		failKey := "p1/Y/a1/t1/2025-06-11T07:06:18Z"
		app := setupTest(failKey)

		input := []ExtnTxnEnrichment{
			{
				PrtyId:       "p1",
				PrtyIdTypeCd: "Y",
				ExtnAcctId:   "a1",
				ExtnTxnId:    "t1",
				BusDt:        "2025-06-11",
				TxnDt:        "2025-06-11T07:06:18Z",
				TxnAmt:       4523,
			},
		}

		putErrors := app.IngestTxnEnrichments(ctx, input)

		want := []PutResult{
			{Key: failKey, Origin: input[0]},
		}
		testutil.Equal(t, want, putErrors, cmpopts.IgnoreFields(PutResult{}, "Err"))
	})

	t.Run("CnctRefresh", func(t *testing.T) {
		failKey := "p1/Y/c1/2025-06-12"
		app := setupTest(failKey)

		input := []ExtnCnctRefresh{
			{
				PrtyId:       "p1",
				PrtyIdTypeCd: "Y",
				ExtnCnctId:   "c1",
				BusDt:        "2025-06-12",
				VendorName:   "BankOfAmerica",
			},
		}

		result := app.IngestCnctRefreshes(ctx, input)

		want := []PutResult{
			{Key: failKey, Origin: input[0]},
		}
		testutil.Equal(t, want, result.PutErrors, cmpopts.IgnoreFields(PutResult{}, "Err"))
	})

	t.Run("AcctRefresh", func(t *testing.T) {
		failKey := "p1/Y/c1/a1/2025-06-12"
		app := setupTest(failKey)

		input := []ExtnAcctRefresh{
			{
				PrtyId:       "p1",
				PrtyIdTypeCd: "Y",
				ExtnCnctId:   "c1",
				ExtnAcctId:   "a1",
				BusDt:        "2025-06-12",
				AcctName:     "Checking Account",
			},
		}

		result := app.IngestAcctsRefreshes(ctx, input)

		want := []PutResult{
			{Key: failKey, Origin: input[0]},
		}
		testutil.Equal(t, want, result.PutErrors, cmpopts.IgnoreFields(PutResult{}, "Err"))
	})

	t.Run("HoldRefresh", func(t *testing.T) {
		failKey := "p1/Y/a1/h1/2025-06-12"
		app := setupTest(failKey)

		input := []ExtnHoldRefresh{
			{
				PrtyId:       "p1",
				PrtyIdTypeCd: "Y",
				ExtnAcctId:   "a1",
				ExtnHoldId:   "h1",
				BusDt:        "2025-06-12",
				HoldName:     "Security",
			},
		}

		result := app.IngestHoldRefreshes(ctx, input)

		want := []PutResult{
			{Key: failKey, Origin: input[0]},
		}
		testutil.Equal(t, want, result.PutErrors, cmpopts.IgnoreFields(PutResult{}, "Err"))
	})

	t.Run("TxnRefresh", func(t *testing.T) {
		failKey := "p1/Y/a1/t1/2025-06-11T07:06:18Z"
		app := setupTest(failKey)

		input := []ExtnTxnRefresh{
			{
				PrtyId:       "p1",
				PrtyIdTypeCd: "Y",
				ExtnAcctId:   "a1",
				ExtnTxnId:    "t1",
				BusDt:        "2025-06-11",
				TxnDt:        "2025-06-11T07:06:18Z",
				TxnAmt:       4523,
			},
		}

		result := app.IngestTxnRefreshes(ctx, input)

		want := []PutResult{
			{Key: failKey, Origin: input[0]},
		}
		testutil.Equal(t, want, result.PutErrors, cmpopts.IgnoreFields(PutResult{}, "Err"))
	})
}

func TestIngest_RefreshDeleteFailure(t *testing.T) {
	ctx := context.WithValue(t.Context(), "trace", t.Name())

	t.Run("CnctRefresh", func(t *testing.T) {
		app := makeTestAwsClient(t)

		stable := app.AwsClient.S3Client
		app.AwsClient.S3Client = &cfg.StubUnstableS3Client{
			StableS3Client: stable,
			FailListPrefix: map[string]string{
				app.TxnBucket: "p1/Y/a1", // fail to list txn by prefix
			},
			FailDeleteKeys: []string{"p1/Y/a1/h1/2025-06-12"}, // fail to delete a holding
		}

		result := app.IngestCnctRefreshes(ctx, []ExtnCnctRefresh{
			{
				IsDeleted:    true,
				PrtyId:       "p1",
				PrtyIdTypeCd: "Y",
				ExtnCnctId:   "c1",
				BusDt:        "2025-06-12",
				VendorName:   "BankOfAmerica",
			},
		})

		want := []DeleteResult{{
			Bucket: app.TxnBucket, Prefix: "p1/Y/a1"},
			{Bucket: app.HoldBucket, Keys: []string{"p1/Y/a1/h1/2025-06-12"}},
		}
		testutil.Equal(t, want, result.DeleteErrors, cmpopts.IgnoreFields(DeleteResult{}, "Err"))
	})

	t.Run("AcctRefresh", func(t *testing.T) {
		app := makeTestAwsClient(t)

		stable := app.AwsClient.S3Client
		app.AwsClient.S3Client = &cfg.StubUnstableS3Client{
			StableS3Client: stable,
			FailListPrefix: map[string]string{
				app.TxnBucket: "p1/Y/a1", // fail to list txn by prefix
			},
			FailDeleteKeys: []string{"p1/Y/c1/a1/2025-06-12"}, // fail to delete an acct
		}

		result := app.IngestAcctsRefreshes(ctx, []ExtnAcctRefresh{
			{
				IsDeleted:    true,
				PrtyId:       "p1",
				PrtyIdTypeCd: "Y",
				ExtnCnctId:   "c1",
				ExtnAcctId:   "a1",
				BusDt:        "2025-06-12",
				AcctName:     "Checking Account",
			},
		})

		want := []DeleteResult{{
			Bucket: app.TxnBucket, Prefix: "p1/Y/a1"},
			{Bucket: app.AcctBucket, Keys: []string{"p1/Y/c1/a1/2025-06-12"}},
		}
		testutil.Equal(t, want, result.DeleteErrors, cmpopts.IgnoreFields(DeleteResult{}, "Err"))
	})
}
