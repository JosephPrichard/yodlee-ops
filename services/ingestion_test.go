package svc

import (
	"context"
	"filogger/pb"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"testing"
)

func getAllKeys(t *testing.T, app *App) []string {
	var keys []string

	for _, bucket := range []string{app.CnctBucket, app.AcctBucket, app.HoldBucket, app.TxnBucket} {
		paginator := s3.NewListObjectsV2Paginator(app.S3Client, &s3.ListObjectsV2Input{Bucket: aws.String(bucket)})

		for paginator.HasMorePages() {
			page, err := paginator.NextPage(context.Background())
			require.NoError(t, err)
			for _, obj := range page.Contents {
				keys = append(keys, *obj.Key)
			}
		}
	}

	return keys
}

func TestIngestCnctEnrichments(t *testing.T) {
	// given
	ctx := context.WithValue(t.Context(), "trace", t.Name())

	app := SetupAppTest(t)
	app.Aws.PageLength = aws.Int32(1) // testing ListObjectsV2 pagination.
	seedS3Buckets(t, app)

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
	wantObjects := []wantObject{
		{
			bucket: app.CnctBucket,
			key:    "p1/Y/c1/2025-06-12",
			value:  &pb.ExtnCnctEntity{PrtyId: "p1", PrtyIdTypeCd: "Y", ExtnCnctId: "c1", BusDt: "2025-06-12", VendorName: "BankOfAmerica"},
		},
		{
			bucket: app.CnctBucket,
			key:    "p300/Y/c10/2025-06-13",
			value:  &pb.ExtnCnctEntity{PrtyId: "p300", PrtyIdTypeCd: "Y", ExtnCnctId: "c10", BusDt: "2025-06-13", VendorName: "GoldmanSachs"},
		},
	}
	assertProtoObjects(t, app, wantObjects, func() proto.Message { return &pb.ExtnCnctEntity{} })
}

func TestIngestAcctEnrichments(t *testing.T) {
	// given
	ctx := context.WithValue(t.Context(), "trace", t.Name())

	app := SetupAppTest(t)
	app.Aws.PageLength = aws.Int32(1) // testing ListObjectsV2 pagination.
	seedS3Buckets(t, app)

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
	wantObjects := []wantObject{
		{
			bucket: app.AcctBucket,
			key:    "p1/Y/c1/a1/2025-06-12",
			value:  &pb.ExtnAcctEntity{PrtyId: "p1", PrtyIdTypeCd: "Y", ExtnCnctId: "c1", ExtnAcctId: "a1", BusDt: "2025-06-12", AcctName: "Checking Account"},
		},
		{
			bucket: app.AcctBucket,
			key:    "p300/Y/c100/a200/2025-06-13",
			value:  &pb.ExtnAcctEntity{PrtyId: "p300", PrtyIdTypeCd: "Y", ExtnCnctId: "c100", ExtnAcctId: "a200", BusDt: "2025-06-13", AcctName: "Savings Account"},
		},
	}
	assertProtoObjects(t, app, wantObjects, func() proto.Message { return &pb.ExtnAcctEntity{} })
}

func TestIngestHoldEnrichments(t *testing.T) {
	// given
	ctx := context.WithValue(t.Context(), "trace", t.Name())

	app := SetupAppTest(t)
	app.Aws.PageLength = aws.Int32(1) // testing ListObjectsV2 pagination.
	seedS3Buckets(t, app)

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
	wantObjects := []wantObject{
		{
			bucket: app.HoldBucket,
			key:    "p1/Y/a1/h1/2025-06-12",
			value:  &pb.ExtnHoldEntity{PrtyId: "p1", PrtyIdTypeCd: "Y", ExtnAcctId: "a1", ExtnHoldId: "h1", BusDt: "2025-06-12", HoldName: "Security"},
		},
		{
			bucket: app.HoldBucket,
			key:    "p300/Y/a200/h100/2025-06-13",
			value:  &pb.ExtnHoldEntity{PrtyId: "p300", PrtyIdTypeCd: "Y", ExtnAcctId: "a200", ExtnHoldId: "h100", BusDt: "2025-06-13", HoldName: "Stock"},
		},
	}
	assertProtoObjects(t, app, wantObjects, func() proto.Message { return &pb.ExtnHoldEntity{} })
}

func TestIngestTxnEnrichments(t *testing.T) {
	// given
	ctx := context.WithValue(t.Context(), "trace", t.Name())

	app := SetupAppTest(t)
	app.Aws.PageLength = aws.Int32(1) // testing ListObjectsV2 pagination.
	seedS3Buckets(t, app)

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
	wantObjects := []wantObject{
		{
			bucket: app.TxnBucket,
			key:    "p1/Y/a1/t1/2025-06-11T07:06:18Z",
			value:  &pb.ExtnTxnEntity{PrtyId: "p1", PrtyIdTypeCd: "Y", ExtnAcctId: "a1", ExtnTxnId: "t1", BusDt: "2025-06-11", TxnDt: "2025-06-11T07:06:18Z", TxnAmt: 4523},
		},
		{
			bucket: app.TxnBucket,
			key:    "p300/Y/a200/t200/2025-06-13T07:06:18Z",
			value:  &pb.ExtnTxnEntity{PrtyId: "p300", PrtyIdTypeCd: "Y", ExtnAcctId: "a200", ExtnTxnId: "t200", BusDt: "2025-06-13", TxnDt: "2025-06-13T07:06:18Z", TxnAmt: -1299},
		},
	}
	assertProtoObjects(t, app, wantObjects, func() proto.Message { return &pb.ExtnTxnEntity{} })
}

func TestIngestCnctRefreshes(t *testing.T) {
	// given
	ctx := context.WithValue(t.Context(), "trace", t.Name())

	app := SetupAppTest(t)
	app.Aws.PageLength = aws.Int32(1) // testing ListObjectsV2 pagination.
	seedS3Buckets(t, app)

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
	wantObjects := []wantObject{
		{
			bucket: app.CnctBucket,
			key:    "p300/Y/c10/2025-06-13",
			value:  &pb.ExtnCnctEntity{PrtyId: "p300", PrtyIdTypeCd: "Y", ExtnCnctId: "c10", BusDt: "2025-06-13", VendorName: "GoldmanSachs"},
		},
	}
	assertProtoObjects(t, app, wantObjects, func() proto.Message { return &pb.ExtnCnctEntity{} })

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
	assert.ElementsMatch(t, wantKeys, getAllKeys(t, app))
}

func TestIngestAcctRefreshes(t *testing.T) {
	// given
	ctx := context.WithValue(t.Context(), "trace", t.Name())

	app := SetupAppTest(t)
	app.Aws.PageLength = aws.Int32(1) // testing ListObjectsV2 pagination.
	seedS3Buckets(t, app)

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
	wantObjects := []wantObject{
		{
			bucket: app.AcctBucket,
			key:    "p300/Y/c100/a200/2025-06-13",
			value:  &pb.ExtnAcctEntity{PrtyId: "p300", PrtyIdTypeCd: "Y", ExtnCnctId: "c100", ExtnAcctId: "a200", BusDt: "2025-06-13", AcctName: "Savings Account"},
		},
	}
	assertProtoObjects(t, app, wantObjects, func() proto.Message { return &pb.ExtnAcctEntity{} })

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
	assert.ElementsMatch(t, wantKeys, getAllKeys(t, app))
}

func TestIngestTxnRefreshes(t *testing.T) {
	// given
	ctx := context.WithValue(t.Context(), "trace", t.Name())

	app := SetupAppTest(t)
	app.Aws.PageLength = aws.Int32(1) // testing ListObjectsV2 pagination.
	seedS3Buckets(t, app)

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
	wantObjects := []wantObject{
		{
			bucket: app.TxnBucket,
			key:    "p300/Y/a200/t200/2025-06-13T07:06:18Z",
			value:  &pb.ExtnTxnEntity{PrtyId: "p300", PrtyIdTypeCd: "Y", ExtnAcctId: "a200", ExtnTxnId: "t200", BusDt: "2025-06-13", TxnDt: "2025-06-13T07:06:18Z", TxnAmt: -1299},
		},
	}
	assertProtoObjects(t, app, wantObjects, func() proto.Message { return &pb.ExtnTxnEntity{} })

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
	assert.ElementsMatch(t, wantKeys, getAllKeys(t, app))
}

func TestIngestHoldRefreshes(t *testing.T) {
	// given
	ctx := context.WithValue(t.Context(), "trace", t.Name())

	app := SetupAppTest(t)
	app.Aws.PageLength = aws.Int32(1) // testing ListObjectsV2 pagination.
	seedS3Buckets(t, app)

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
	wantObjects := []wantObject{
		{
			bucket: app.HoldBucket,
			key:    "p300/Y/a200/h100/2025-06-13",
			value:  &pb.ExtnHoldEntity{PrtyId: "p300", PrtyIdTypeCd: "Y", ExtnAcctId: "a200", ExtnHoldId: "h100", BusDt: "2025-06-13", HoldName: "Stock"},
		},
	}
	assertProtoObjects(t, app, wantObjects, func() proto.Message { return &pb.ExtnHoldEntity{} })

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
	assert.ElementsMatch(t, wantKeys, getAllKeys(t, app))
}

func TestIngestDeleteRetries(t *testing.T) {
	// given
	ctx := context.WithValue(t.Context(), "trace", t.Name())

	app := SetupAppTest(t)
	app.Aws.PageLength = aws.Int32(1) // testing ListObjectsV2 pagination.
	seedS3Buckets(t, app)

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
	assert.ElementsMatch(t, wantKeys, getAllKeys(t, app))
}
