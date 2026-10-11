package testutil

import (
	"context"
	"fmt"
	"testing"
	"time"
	"yodleeops/storage"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	LocalstackContTag  = "localstack/localstack:3.0.2"
	LocalStackContPort = "4566/tcp"
)

var localstackCont testcontainers.Container

var AWSConfig = storage.Config{
	AwsDefaultRegion: "us-east-1",
	IsLocal:          true,
	CnctBucket:       "cnct-bucket",
	AcctBucket:       "acct-bucket",
	HoldBucket:       "hold-bucket",
	TxnBucket:        "txn-bucket",
}

func NewVirtualS3Client(t *testing.T) storage.S3Client {
	ctx := context.Background()

	config := AWSConfig
	config.AwsEndpoint = startLocalstackCont(ctx, t)

	s3Sdk := storage.NewS3SDK(config)
	s3Client := storage.NewS3Client(config, s3Sdk)

	for _, bucket := range []storage.Bucket{
		s3Client.CnctBucket,
		s3Client.AcctBucket,
		s3Client.TxnBucket,
		s3Client.HoldBucket,
	} {
		if _, err := s3Sdk.CreateBucket(context.Background(), &s3.CreateBucketInput{
			Bucket: bucket.String(),
		}); err != nil {
			t.Fatalf("failed to create bucket %s: %v", bucket, err)
		}
	}

	SeedS3Buckets(t, s3Sdk, s3Client.Buckets)

	return s3Client
}

func startLocalstackCont(ctx context.Context, t *testing.T) string {
	if localstackCont == nil {
		start := time.Now()

		cont, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
			Started: true,
			ContainerRequest: testcontainers.ContainerRequest{
				Image:        LocalstackContTag,
				ExposedPorts: []string{LocalStackContPort},
				WaitingFor:   wait.ForListeningPort(LocalStackContPort),
			},
		})
		if err != nil {
			t.Fatalf("failed to start localstack: %v", err)
		}

		localstackCont = cont
		t.Logf("started localstack in %v", time.Since(start))
	}

	host, _ := localstackCont.Host(ctx)
	port, _ := localstackCont.MappedPort(ctx, LocalStackContPort)

	return fmt.Sprintf("http://%s:%s", host, port.Port())
}
