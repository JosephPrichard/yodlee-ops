package testutil

import (
	"context"
	"fmt"
	"testing"
	"time"

	"yodleeops/infra"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	LocalstackContTag  = "localstack/localstack:3.0.2"
	LocalStackContPort = "4566/tcp"
)

var localstackCont testcontainers.Container

func SetupITest(t *testing.T) infra.AWS {
	ctx := context.Background()

	config := infra.Config{
		AwsDefaultRegion: "us-east-1",
		IsLocal:          true,
	}
	config.AwsEndpoint = startLocalstackCont(ctx, t)

	return initTestAWS(t, config)
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

func initTestAWS(t *testing.T, config infra.Config) infra.AWS {
	s3Client := infra.MakeS3Client(config)
	aws := infra.MakeAWS(s3Client)

	for _, bucket := range []infra.Bucket{
		infra.CnctBucket,
		infra.AcctBucket,
		infra.HoldBucket,
		infra.TxnBucket,
	} {
		if _, err := s3Client.CreateBucket(context.Background(), &s3.CreateBucketInput{
			Bucket: bucket.String(),
		}); err != nil {
			t.Fatalf("failed to create bucket %s: %v", bucket, err)
		}
	}

	SeedS3Buckets(t, s3Client)

	return aws
}
