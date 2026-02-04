package testutil

import (
	"bytes"
	cfg "filogger/config"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"
)

func SeedS3Buckets(t *testing.T, awsClient *cfg.AwsClient) {
	// seed the bucket with data to test deletions AND to ensure that inserts handle existing keys properly
	// "body" can be anything because insertion does not look at this information
	for _, record := range []struct {
		Bucket string
		Key    string
	}{
		{Bucket: awsClient.CnctBucket, Key: "p1/Y/c1/2025-06-12"},
		{Bucket: awsClient.CnctBucket, Key: "p1/Y/c1/2025-06-13"},
		{Bucket: awsClient.CnctBucket, Key: "p1/Y/c2/2025-06-14"},
		{Bucket: awsClient.CnctBucket, Key: "p1/Y/c3/2025-06-15"},

		{Bucket: awsClient.AcctBucket, Key: "p1/Y/c1/a1/2025-06-12"},
		{Bucket: awsClient.AcctBucket, Key: "p1/Y/c1/a1/2025-06-13"},
		{Bucket: awsClient.AcctBucket, Key: "p2/Y/c2/a2/2025-06-14"},
		{Bucket: awsClient.AcctBucket, Key: "p2/Y/c3/a3/2025-06-15"},

		{Bucket: awsClient.HoldBucket, Key: "p1/Y/a1/h1/2025-06-12"},
		{Bucket: awsClient.HoldBucket, Key: "p1/Y/a1/h1/2025-06-13"},
		{Bucket: awsClient.HoldBucket, Key: "p2/Y/a1/h1/2025-06-14"},
		{Bucket: awsClient.HoldBucket, Key: "p2/Y/a2/h2/2025-06-15"},

		{Bucket: awsClient.TxnBucket, Key: "p1/Y/a1/t1/2025-06-12T00:14:37Z"},
		{Bucket: awsClient.TxnBucket, Key: "p1/Y/a1/t1/2025-06-12T02:48:09Z"},
		{Bucket: awsClient.TxnBucket, Key: "p2/Y/a1/t1/2025-06-13T02:48:09Z"},
		{Bucket: awsClient.TxnBucket, Key: "p2/Y/a2/t2/2025-06-14T07:06:18Z"},
	} {
		_, err := awsClient.S3Client.PutObject(t.Context(), &s3.PutObjectInput{
			Bucket: aws.String(record.Bucket),
			Key:    aws.String(record.Key),
			Body:   bytes.NewReader([]byte("test")),
		})
		require.NoError(t, err)
	}
}