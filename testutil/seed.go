package testutil

import (
	"bytes"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"testing"

	"yodleeops/infra"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"
)

func SeedS3Buckets(t *testing.T, s3Client *s3.Client) {
	// clear all objects already in the bucket
	for _, bucket := range []infra.Bucket{
		infra.CnctBucket,
		infra.AcctBucket,
		infra.HoldBucket,
		infra.TxnBucket,
	} {
		paginator := s3.NewListObjectVersionsPaginator(s3Client, &s3.ListObjectVersionsInput{
			Bucket: bucket.String(),
		})

		for paginator.HasMorePages() {
			output, err := paginator.NextPage(t.Context())
			require.NoError(t, err)

			// Collect object identifiers (key and version ID) for deletion
			var objectIdentifiers []types.ObjectIdentifier
			for _, object := range output.Versions {
				objectIdentifiers = append(objectIdentifiers, types.ObjectIdentifier{
					Key:       object.Key,
					VersionId: object.VersionId,
				})
			}
			for _, marker := range output.DeleteMarkers {
				objectIdentifiers = append(objectIdentifiers, types.ObjectIdentifier{
					Key:       marker.Key,
					VersionId: marker.VersionId,
				})
			}
			if len(objectIdentifiers) == 0 {
				continue
			}

			deleteInput := &s3.DeleteObjectsInput{
				Bucket: bucket.String(),
				Delete: &types.Delete{
					Objects: objectIdentifiers,
					Quiet:   aws.Bool(true), // Set to true to only return errors in the response
				},
			}
			_, err = s3Client.DeleteObjects(t.Context(), deleteInput)
			require.NoError(t, err)
		}
	}

	// seed the bucket with data to test deletions AND to ensure that inserts handle existing keys properly
	// "body" can be anything because insertion does not look at this information
	for _, record := range []struct {
		Bucket infra.Bucket
		Key    string
	}{
		{Bucket: infra.CnctBucket, Key: "p1/1/10/2025-06-12"},
		{Bucket: infra.CnctBucket, Key: "p1/1/10/2025-06-13"},
		{Bucket: infra.CnctBucket, Key: "p1/1/20/2025-06-14"},
		{Bucket: infra.CnctBucket, Key: "p1/1/30/2025-06-15"},

		// Accounts
		{Bucket: infra.AcctBucket, Key: "p1/1/10/100/2025-06-12"},
		{Bucket: infra.AcctBucket, Key: "p1/1/10/100/2025-06-13"},
		{Bucket: infra.AcctBucket, Key: "p2/1/20/200/2025-06-14"},
		{Bucket: infra.AcctBucket, Key: "p2/1/30/400/2025-06-15"},

		// Holdings
		{Bucket: infra.HoldBucket, Key: "p1/1/100/1000/2025-06-12"},
		{Bucket: infra.HoldBucket, Key: "p1/1/100/1000/2025-06-13"},
		{Bucket: infra.HoldBucket, Key: "p2/1/100/1000/2025-06-14"},
		{Bucket: infra.HoldBucket, Key: "p2/1/200/2000/2025-06-15"},

		// Transactions
		{Bucket: infra.TxnBucket, Key: "p1/1/100/3000/2025-06-12T00:14:37Z"},
		{Bucket: infra.TxnBucket, Key: "p1/1/100/3000/2025-06-12T02:48:09Z"},
		{Bucket: infra.TxnBucket, Key: "p2/1/100/3000/2025-06-13T02:48:09Z"},
		{Bucket: infra.TxnBucket, Key: "p2/1/200/2000/2025-06-14T07:06:18Z"},
	} {
		_, err := s3Client.PutObject(t.Context(), &s3.PutObjectInput{
			Bucket: record.Bucket.String(),
			Key:    aws.String(record.Key),
			Body:   bytes.NewReader([]byte("test")),
		})
		require.NoError(t, err)
	}
}
