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

func seedS3BucketsForInsertion(t *testing.T) {
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

func assertObjects(t *testing.T, objects []wantObject, makeValue func() proto.Message) {
	for _, object := range objects {
		data, err := app.S3Client.GetObject(context.Background(), &s3.GetObjectInput{
			Bucket: aws.String(object.bucket),
			Key:    aws.String(object.key),
		})
		assert.NoError(t, err)
		if err != nil {
			continue
		}

		body, err := io.ReadAll(data.Body)
		require.NoError(t, err)

		s3Value := makeValue()
		require.NoError(t, proto.Unmarshal(body, s3Value))

		Equal(t, object.value, s3Value, protocmp.Transform())
	}
}

func TestIngestCnctEnrichments(t *testing.T) {
	ctx := context.WithValue(t.Context(), "trace", t.Name())

	SetupAppTest(t)
	seedS3BucketsForInsertion(t)

	err := IngestCnctEnrichments(ctx, []ExtnCnctEnrichment{
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
	require.NoError(t, err)

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
	assertObjects(t, wantObjects, func() proto.Message { return &pb.ExtnCnctEntity{} })
}

func TestIngestAcctEnrichments(t *testing.T) {
	ctx := context.WithValue(t.Context(), "trace", t.Name())

	SetupAppTest(t)
	seedS3BucketsForInsertion(t)

	err := IngestAcctEnrichments(ctx, []ExtnAcctEnrichment{
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
	require.NoError(t, err)

	wantObjects := []wantObject{
		{
			bucket: app.CnctBucket,
			key:    "p1/Y/c1/a1/2025-06-12",
			value:  &pb.ExtnAcctEntity{PrtyId: "p1", PrtyIdTypeCd: "Y", ExtnCnctId: "c1", ExtnAcctId: "a1", BusDt: "2025-06-12", AcctName: "Checking Account"},
		},
		{
			bucket: app.CnctBucket,
			key:    "p300/Y/c100/a200/2025-06-13",
			value:  &pb.ExtnAcctEntity{PrtyId: "p300", PrtyIdTypeCd: "Y", ExtnCnctId: "c100", ExtnAcctId: "a200", BusDt: "2025-06-13", AcctName: "Savings Account"},
		},
	}
	assertObjects(t, wantObjects, func() proto.Message { return &pb.ExtnAcctEntity{} })
}

func TestIngestHoldEnrichments(t *testing.T) {
	ctx := context.WithValue(t.Context(), "trace", t.Name())

	SetupAppTest(t)
	seedS3BucketsForInsertion(t)

	err := IngestHoldEnrichments(ctx, []ExtnHoldEnrichment{
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
	require.NoError(t, err)

	wantObjects := []wantObject{
		{
			bucket: app.CnctBucket,
			key:    "p1/Y/a1/h1/2025-06-12",
			value:  &pb.ExtnHoldEntity{PrtyId: "p1", PrtyIdTypeCd: "Y", ExtnAcctId: "a1", ExtnHoldId: "h1", BusDt: "2025-06-12", HoldName: "Security"},
		},
		{
			bucket: app.CnctBucket,
			key:    "p300/Y/a200/h100/2025-06-13",
			value:  &pb.ExtnHoldEntity{PrtyId: "p300", PrtyIdTypeCd: "Y", ExtnAcctId: "a200", ExtnHoldId: "h100", BusDt: "2025-06-13", HoldName: "Stock"},
		},
	}
	assertObjects(t, wantObjects, func() proto.Message { return &pb.ExtnHoldEntity{} })
}

func TestIngestTxnEnrichments(t *testing.T) {
	ctx := context.WithValue(t.Context(), "trace", t.Name())

	SetupAppTest(t)
	seedS3BucketsForInsertion(t)

	err := IngestTxnEnrichments(ctx, []ExtnTxnEnrichment{
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
	require.NoError(t, err)

	wantObjects := []wantObject{
		{
			bucket: app.CnctBucket,
			key:    "p1/Y/a1/t1/2025-06-11T07:06:18Z",
			value:  &pb.ExtnTxnEntity{PrtyId: "p1", PrtyIdTypeCd: "Y", ExtnAcctId: "a1", ExtnTxnId: "t1", BusDt: "2025-06-11", TxnDt: "2025-06-11T07:06:18Z", TxnAmt: 4523},
		},
		{
			bucket: app.CnctBucket,
			key:    "p300/Y/a200/t200/2025-06-13T07:06:18Z",
			value:  &pb.ExtnTxnEntity{PrtyId: "p300", PrtyIdTypeCd: "Y", ExtnAcctId: "a200", ExtnTxnId: "t200", BusDt: "2025-06-13", TxnDt: "2025-06-13T07:06:18Z", TxnAmt: -1299},
		},
	}
	assertObjects(t, wantObjects, func() proto.Message { return &pb.ExtnTxnEntity{} })
}
