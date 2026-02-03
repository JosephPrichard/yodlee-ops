package svc

import (
	"bytes"
	"context"
	"filogger/pb"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
	"io"
	"testing"
)

func seedS3BucketsForInsertion(t *testing.T, app *App) {
	// seed the bucket with data to test deletions AND to ensure that inserts handle existing keys properly
	// "body" can be anything because insertion does not look at this information
	for _, record := range []struct {
		Bucket string
		Key    string
	}{
		{Bucket: app.CnctBucket, Key: "p1/Y/c1/2025-06-12"},
		{Bucket: app.CnctBucket, Key: "p1/Y/c1/2025-06-13"},
		{Bucket: app.CnctBucket, Key: "p1/Y/c2/2025-06-14"},
		{Bucket: app.CnctBucket, Key: "p1/Y/c3/2025-06-15"},

		{Bucket: app.AcctBucket, Key: "p1/Y/c1/a1/2025-06-12"},
		{Bucket: app.AcctBucket, Key: "p1/Y/c1/a1/2025-06-13"},
		{Bucket: app.AcctBucket, Key: "p2/Y/c2/a2/2025-06-14"},
		{Bucket: app.AcctBucket, Key: "p2/Y/c3/a3/2025-06-15"},

		{Bucket: app.HoldBucket, Key: "p1/Y/a1/h1/2025-06-12"},
		{Bucket: app.HoldBucket, Key: "p1/Y/a1/h1/2025-06-13"},
		{Bucket: app.HoldBucket, Key: "p2/Y/a1/h1/2025-06-14"},
		{Bucket: app.HoldBucket, Key: "p2/Y/a2/h2/2025-06-15"},

		{Bucket: app.TxnBucket, Key: "p1/Y/a1/t1/2025-06-12T00:14:37Z"},
		{Bucket: app.TxnBucket, Key: "p1/Y/a1/t1/2025-06-12T02:48:09Z"},
		{Bucket: app.TxnBucket, Key: "p2/Y/a1/t1/2025-06-13T02:48:09Z"},
		{Bucket: app.TxnBucket, Key: "p2/Y/a2/t2/2025-06-14T07:06:18Z"},
	} {
		_, err := app.S3Client.PutObject(t.Context(), &s3.PutObjectInput{
			Bucket: aws.String(record.Bucket),
			Key:    aws.String(record.Key),
			Body:   bytes.NewReader([]byte("test")),
		})
		require.NoError(t, err)
	}
}

type wantObject struct {
	bucket string
	key    string
	value  proto.Message
}

func assertProtoObjects(t *testing.T, app *App, objects []wantObject, makeValue func() proto.Message) {
	t.Helper()

	for _, object := range objects {
		data, err := app.S3Client.GetObject(context.Background(), &s3.GetObjectInput{
			Bucket: aws.String(object.bucket),
			Key:    aws.String(object.key),
		})
		if err != nil {
			t.Errorf("failed to get object %s/%s: %v", object.bucket, object.key, err)
			continue
		}

		body, err := io.ReadAll(data.Body)
		require.NoError(t, err)

		t.Logf("got object %s/%s: %s", object.bucket, object.key, string(body))

		s3Value := makeValue()
		require.NoError(t, proto.Unmarshal(body, s3Value))

		Equal(t, object.value, s3Value, protocmp.Transform())
	}
}

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
	ctx := context.WithValue(t.Context(), "trace", t.Name())

	app := SetupAppTest(t)
	seedS3BucketsForInsertion(t, app)

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
	ctx := context.WithValue(t.Context(), "trace", t.Name())

	app := SetupAppTest(t)
	seedS3BucketsForInsertion(t, app)

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
	ctx := context.WithValue(t.Context(), "trace", t.Name())

	app := SetupAppTest(t)
	seedS3BucketsForInsertion(t, app)

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
	ctx := context.WithValue(t.Context(), "trace", t.Name())

	app := SetupAppTest(t)
	seedS3BucketsForInsertion(t, app)

	putErrors :=app.IngestTxnEnrichments(ctx, []ExtnTxnEnrichment{
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
	ctx := context.WithValue(t.Context(), "trace", t.Name())

	app := SetupAppTest(t)
	seedS3BucketsForInsertion(t, app)

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
	require.Empty(t, result.PutErrs)
	require.Empty(t, result.DeleteErrs)

	wantObjects := []wantObject{
		{
			bucket: app.CnctBucket,
			key:    "p300/Y/c10/2025-06-13",
			value:  &pb.ExtnCnctEntity{PrtyId: "p300", PrtyIdTypeCd: "Y", ExtnCnctId: "c10", BusDt: "2025-06-13", VendorName: "GoldmanSachs"},
		},
	}
	assertProtoObjects(t, app, wantObjects, func() proto.Message { return &pb.ExtnCnctEntity{} })

	// removed keys are excluded.
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
	ctx := context.WithValue(t.Context(), "trace", t.Name())

	app := SetupAppTest(t)
	seedS3BucketsForInsertion(t, app)

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
	require.Empty(t, result.PutErrs)
	require.Empty(t, result.DeleteErrs)

	wantObjects := []wantObject{
		{
			bucket: app.AcctBucket,
			key:    "p300/Y/c100/a200/2025-06-13",
			value:  &pb.ExtnAcctEntity{PrtyId: "p300", PrtyIdTypeCd: "Y", ExtnCnctId: "c100", ExtnAcctId: "a200", BusDt: "2025-06-13", AcctName: "Savings Account"},
		},
	}
	assertProtoObjects(t, app, wantObjects, func() proto.Message { return &pb.ExtnAcctEntity{} })

	// removed keys are excluded.
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
	ctx := context.WithValue(t.Context(), "trace", t.Name())

	app := SetupAppTest(t)
	seedS3BucketsForInsertion(t, app)

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
	require.Empty(t, result.PutErrs)
	require.Empty(t, result.DeleteErrs)

	wantObjects := []wantObject{
		{
			bucket: app.TxnBucket,
			key:    "p300/Y/a200/t200/2025-06-13T07:06:18Z",
			value:  &pb.ExtnTxnEntity{PrtyId: "p300", PrtyIdTypeCd: "Y", ExtnAcctId: "a200", ExtnTxnId: "t200", BusDt: "2025-06-13", TxnDt: "2025-06-13T07:06:18Z", TxnAmt: -1299},
		},
	}
	assertProtoObjects(t, app, wantObjects, func() proto.Message { return &pb.ExtnTxnEntity{} })

	// removed keys are excluded.
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
	ctx := context.WithValue(t.Context(), "trace", t.Name())

	app := SetupAppTest(t)
	seedS3BucketsForInsertion(t, app)

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
	require.Empty(t, result.PutErrs)
	require.Empty(t, result.DeleteErrs)

	wantObjects := []wantObject{
		{
			bucket: app.HoldBucket,
			key:    "p300/Y/a200/h100/2025-06-13",
			value:  &pb.ExtnHoldEntity{PrtyId: "p300", PrtyIdTypeCd: "Y", ExtnAcctId: "a200", ExtnHoldId: "h100", BusDt: "2025-06-13", HoldName: "Stock"},
		},
	}
	assertProtoObjects(t, app, wantObjects, func() proto.Message { return &pb.ExtnHoldEntity{} })

	// removed keys are excluded.
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
