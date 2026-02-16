package testutil

import (
	"context"
	"encoding/json"
	"io"
	"testing"
	"yodleeops/infra"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/testing/protocmp"
)

func Equal[T any](t *testing.T, expected, actual T, opts ...cmp.Option) {
	t.Helper()
	if diff := cmp.Diff(expected, actual, opts...); diff != "" {
		t.Errorf("\n%s", diff)
	}
}

type WantObject[JSON any] struct {
	Bucket string
	Key    string
	Value  JSON
}

func AssertObjects[JSON any](t *testing.T, awsClient *infra.AwsClient, objects []WantObject[JSON], opts ...cmp.Option) {
	opts = append([]cmp.Option{protocmp.Transform()}, opts...)

	t.Helper()

	for _, object := range objects {
		func() {
			resp, err := awsClient.S3Client.GetObject(context.Background(), &s3.GetObjectInput{
				Bucket: aws.String(object.Bucket),
				Key:    aws.String(object.Key),
			})
			if err != nil {
				t.Fatalf("failed to get object %s/%s: %s", object.Bucket, object.Key, err)
			}
			defer resp.Body.Close()

			bodyBytes, err := io.ReadAll(resp.Body)
			require.NoError(t, err)

			t.Logf("got object %s/%s (%d bytes)", object.Bucket, object.Key, len(bodyBytes))

			var s3Value JSON
			//gzipReader, err := gzip.NewReader(bytes.NewReader(bodyBytes))
			//require.NoError(t, err)
			//defer gzipReader.Close()
			//
			//decompressed, err := io.ReadAll(gzipReader)
			//require.NoError(t, err)
			//require.NoError(t, json.Unmarshal(decompressed, &s3Value))
			require.NoError(t, json.Unmarshal(bodyBytes, &s3Value))

			Equal(t, object.Value, s3Value, opts...)
		}()
	}
}

type WantKey struct {
	Bucket string
	Key    string
}

func GetAllKeys(t *testing.T, awsClient *infra.AwsClient) []WantKey {
	var keys []WantKey

	for _, bucket := range []string{awsClient.CnctBucket, awsClient.AcctBucket, awsClient.HoldBucket, awsClient.TxnBucket} {
		paginator := s3.NewListObjectsV2Paginator(awsClient.S3Client, &s3.ListObjectsV2Input{Bucket: aws.String(bucket)})

		for paginator.HasMorePages() {
			page, err := paginator.NextPage(context.Background())
			require.NoError(t, err)
			for _, obj := range page.Contents {
				if obj.Key == nil {
					continue
				}
				keys = append(keys, WantKey{Bucket: bucket, Key: *obj.Key})
			}
		}
	}

	return keys
}
