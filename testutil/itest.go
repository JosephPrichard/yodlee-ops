package testutil

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"yodleeops/infra"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	LocalstackContTag  = "localstack/localstack:3.0.2"
	LocalStackContPort = "4566/tcp"
)

var setupAppMu sync.Mutex

var localstackCont testcontainers.Container

func SetupAwsITest(t *testing.T) infra.AWS {
	// global lock for the entire initialization phase.
	// this prevents multiple containers for the same infra from being spawned
	setupAppMu.Lock()
	defer setupAppMu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if localstackCont == nil {
		start := time.Now()
		cont, err := testcontainers.GenericContainer(t.Context(), testcontainers.GenericContainerRequest{
			Started: true,
			ContainerRequest: testcontainers.ContainerRequest{
				Image:        LocalstackContTag,
				ExposedPorts: []string{LocalStackContPort},
				WaitingFor:   wait.ForListeningPort(LocalStackContPort),
			},
		})
		if err != nil {
			t.Fatalf("failed to start localstack container: %v", err)
		}
		localstackCont = cont
		t.Logf("finished starting localstack container in %v", time.Since(start))
	}

	host, _ := localstackCont.Host(ctx)
	port, _ := localstackCont.MappedPort(ctx, LocalStackContPort)
	awsEndpoint := fmt.Sprintf("http://%s:%s", host, port.Port())

	cfg := infra.Config{
		AwsDefaultRegion: "us-east-1",
		IsLocal:          true,
		AwsEndpoint:      awsEndpoint,
	}

	client := infra.MakeAwsClient(infra.MakeS3Client(cfg))

	// mock the bucket data for each itest.
	client.Buckets.Connections = unique(client.Buckets.Connections)
	client.Buckets.Accounts = unique(client.Buckets.Accounts)
	client.Buckets.Holdings = unique(client.Buckets.Holdings)
	client.Buckets.Transactions = unique(client.Buckets.Transactions)

	createBuckets(ctx, t, cfg, client)
	return client
}

func createBuckets(ctx context.Context, t *testing.T, cfg infra.Config, client infra.AWS) {
	awsCfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(cfg.AwsDefaultRegion),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("testing", "testing", "")),
	)
	if err != nil {
		t.Fatalf("failed to load S3 config: %s", err)
	}
	s3Client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(cfg.AwsEndpoint)
		o.UsePathStyle = true
	})
	for _, bucket := range []infra.Bucket{
		client.Buckets.Connections,
		client.Buckets.Accounts,
		client.Buckets.Holdings,
		client.Buckets.Transactions,
	} {
		if _, err := s3Client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: bucket.String()}); err != nil {
			t.Fatalf("failed to create bucket %s: %s", bucket, err)
		}
	}
}

func unique(bucket infra.Bucket) infra.Bucket {
	return infra.Bucket(string(bucket) + "-" + uuid.NewString())
}
