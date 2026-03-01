package testutil

import (
	"bytes"
	"testing"

	"yodleeops/infra"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"
)

func SeedS3Buckets(t *testing.T, a infra.AWS) {
	// seed the bucket with data to test deletions AND to ensure that inserts handle existing keys properly
	// "body" can be anything because insertion does not look at this information
	for _, record := range []struct {
		Bucket infra.Bucket
		Key    string
	}{
		{Bucket: a.Buckets.Connections, Key: "p1/1/10/2025-06-12"},
		{Bucket: a.Buckets.Connections, Key: "p1/1/10/2025-06-13"},
		{Bucket: a.Buckets.Connections, Key: "p1/1/20/2025-06-14"},
		{Bucket: a.Buckets.Connections, Key: "p1/1/30/2025-06-15"},

		// Accounts
		{Bucket: a.Buckets.Accounts, Key: "p1/1/10/100/2025-06-12"},
		{Bucket: a.Buckets.Accounts, Key: "p1/1/10/100/2025-06-13"},
		{Bucket: a.Buckets.Accounts, Key: "p2/1/20/200/2025-06-14"},
		{Bucket: a.Buckets.Accounts, Key: "p2/1/30/400/2025-06-15"},

		// Holdings
		{Bucket: a.Buckets.Holdings, Key: "p1/1/100/1000/2025-06-12"},
		{Bucket: a.Buckets.Holdings, Key: "p1/1/100/1000/2025-06-13"},
		{Bucket: a.Buckets.Holdings, Key: "p2/1/100/1000/2025-06-14"},
		{Bucket: a.Buckets.Holdings, Key: "p2/1/200/2000/2025-06-15"},

		// Transactions
		{Bucket: a.Buckets.Transactions, Key: "p1/1/100/3000/2025-06-12T00:14:37Z"},
		{Bucket: a.Buckets.Transactions, Key: "p1/1/100/3000/2025-06-12T02:48:09Z"},
		{Bucket: a.Buckets.Transactions, Key: "p2/1/100/3000/2025-06-13T02:48:09Z"},
		{Bucket: a.Buckets.Transactions, Key: "p2/1/200/2000/2025-06-14T07:06:18Z"},
	} {
		_, err := a.S3.PutObject(t.Context(), &s3.PutObjectInput{
			Bucket: record.Bucket.String(),
			Key:    aws.String(record.Key),
			Body:   bytes.NewReader([]byte("test")),
		})
		require.NoError(t, err)
	}
}
