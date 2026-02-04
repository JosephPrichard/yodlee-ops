package testutil

import (
	"context"
	cfg "filogger/config"
	"io"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
)

func Equal[T any](t *testing.T, expected, actual T, opts ...cmp.Option) {
	t.Helper()
	if diff := cmp.Diff(expected, actual, opts...); diff != "" {
		t.Errorf("\n%s", diff)
	}
}

type WantObject struct {
	Bucket string
	Key    string
	Value  proto.Message
}

func AssertProtoObjects(t *testing.T, awsClient *cfg.AwsClient, objects []WantObject, makeValue func() proto.Message) {
	t.Helper()

	for _, object := range objects {
		data, err := awsClient.S3Client.GetObject(context.Background(), &s3.GetObjectInput{
			Bucket: aws.String(object.Bucket),
			Key:    aws.String(object.Key),
		})
		if err != nil {
			t.Errorf("failed to get object %s/%s: %v", object.Bucket, object.Key, err)
			continue
		}

		body, err := io.ReadAll(data.Body)
		require.NoError(t, err)

		t.Logf("got object %s/%s: %s", object.Bucket, object.Key, string(body))

		s3Value := makeValue()
		require.NoError(t, proto.Unmarshal(body, s3Value))

		Equal(t, object.Value, s3Value, protocmp.Transform())
	}
}


func GetAllKeys(t *testing.T, awsClient *cfg.AwsClient) []string {
	var keys []string

	for _, bucket := range []string{awsClient.CnctBucket, awsClient.AcctBucket, awsClient.HoldBucket, awsClient.TxnBucket} {
		paginator := s3.NewListObjectsV2Paginator(awsClient.S3Client, &s3.ListObjectsV2Input{Bucket: aws.String(bucket)})

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