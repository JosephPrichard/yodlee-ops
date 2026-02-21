package testutil

import (
	"bytes"
	"testing"
	"yodleeops/infra"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"
)

func SeedS3Buckets(t *testing.T, awsClient infra.AwsClient) {
	// seed the bucket with data to test deletions AND to ensure that inserts handle existing keys properly
	// "body" can be anything because insertion does not look at this information
	for _, record := range []struct {
		Bucket infra.Bucket
		Key    string
	}{
		{Bucket: awsClient.CnctBucket, Key: "p1/1/10/2025-06-12"},
		{Bucket: awsClient.CnctBucket, Key: "p1/1/10/2025-06-13"},
		{Bucket: awsClient.CnctBucket, Key: "p1/1/20/2025-06-14"},
		{Bucket: awsClient.CnctBucket, Key: "p1/1/30/2025-06-15"},

		// Accounts
		{Bucket: awsClient.AcctBucket, Key: "p1/1/10/100/2025-06-12"},
		{Bucket: awsClient.AcctBucket, Key: "p1/1/10/100/2025-06-13"},
		{Bucket: awsClient.AcctBucket, Key: "p2/1/20/200/2025-06-14"},
		{Bucket: awsClient.AcctBucket, Key: "p2/1/30/400/2025-06-15"},

		// Holdings
		{Bucket: awsClient.HoldBucket, Key: "p1/1/100/1000/2025-06-12"},
		{Bucket: awsClient.HoldBucket, Key: "p1/1/100/1000/2025-06-13"},
		{Bucket: awsClient.HoldBucket, Key: "p2/1/100/1000/2025-06-14"},
		{Bucket: awsClient.HoldBucket, Key: "p2/1/200/2000/2025-06-15"},

		// Transactions
		{Bucket: awsClient.TxnBucket, Key: "p1/1/100/3000/2025-06-12T00:14:37Z"},
		{Bucket: awsClient.TxnBucket, Key: "p1/1/100/3000/2025-06-12T02:48:09Z"},
		{Bucket: awsClient.TxnBucket, Key: "p2/1/100/3000/2025-06-13T02:48:09Z"},
		{Bucket: awsClient.TxnBucket, Key: "p2/1/200/2000/2025-06-14T07:06:18Z"},
	} {
		_, err := awsClient.S3Client.PutObject(t.Context(), &s3.PutObjectInput{
			Bucket: aws.String(string(record.Bucket)),
			Key:    aws.String(record.Key),
			Body:   bytes.NewReader([]byte("test")),
		})
		require.NoError(t, err)
	}
}
