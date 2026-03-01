package testutil

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"google.golang.org/protobuf/testing/protocmp"
	"io"
	"net/http/httptest"
	"testing"

	"yodleeops/infra"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
)

func Equal[T any](t *testing.T, expected, actual T, opts ...cmp.Option) {
	t.Helper()
	if diff := cmp.Diff(expected, actual, opts...); diff != "" {
		t.Errorf("\n%s", diff)
	}
}

type WantObject[JSON any] struct {
	Bucket infra.Bucket
	Key    string
	Value  JSON
}

func DecodeGzipJSON[JSON any](r io.Reader, decoded *JSON) error {
	gzipReader, err := gzip.NewReader(r)
	if err != nil {
		return fmt.Errorf("make gzip reader: %w", err)
	}
	defer gzipReader.Close()
	if err := json.NewDecoder(gzipReader).Decode(decoded); err != nil {
		return fmt.Errorf("decode gzip json: %w", err)
	}
	return nil
}

func AssertObjects[JSON any](t *testing.T, awsClient *infra.AWS, objects []WantObject[JSON], opts ...cmp.Option) {
	opts = append([]cmp.Option{protocmp.Transform()}, opts...)

	t.Helper()

	for _, object := range objects {
		func() {
			resp, err := awsClient.S3.GetObject(context.Background(), &s3.GetObjectInput{
				Bucket: object.Bucket.String(),
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
			require.NoError(t, DecodeGzipJSON(bytes.NewReader(bodyBytes), &s3Value))
			//require.NoError(t, json.Unmarshal(bodyBytes, &s3Value))

			Equal(t, object.Value, s3Value, opts...)
		}()
	}
}

type WantKey struct {
	Bucket infra.Bucket
	Key    string
}

func GetAllKeys(t *testing.T, a infra.AWS) []WantKey {
	var keys []WantKey

	for _, bucket := range []infra.Bucket{
		a.Buckets.Connections,
		a.Buckets.Accounts,
		a.Buckets.Transactions,
		a.Buckets.Holdings,
	} {
		paginator := s3.NewListObjectsV2Paginator(a.S3, &s3.ListObjectsV2Input{Bucket: bucket.String()})

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

func AssertRespBody[V any](t *testing.T, wantBody V, w *httptest.ResponseRecorder, opts ...cmp.Option) {
	t.Helper()

	actualBody := GetRespBody[V](t, w)
	str := cmp.Diff(wantBody, actualBody, opts...)
	if str != "" {
		t.Error(str)
	}
}

func GetRespBody[V any](t *testing.T, w *httptest.ResponseRecorder) V {
	t.Helper()

	resp := w.Result()
	defer resp.Body.Close()

	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Error(err.Error())
	}

	var actualBody V
	if err = json.Unmarshal(b, &actualBody); err != nil {
		t.Errorf("failed to unmarshal response body: %s: %s", string(b), err.Error())
	}
	return actualBody
}
