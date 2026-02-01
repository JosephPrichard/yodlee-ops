package svc

import (
	"context"
	flog "filogger"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/go-cmp/cmp"
	"github.com/google/uuid"
	"github.com/testcontainers/testcontainers-go"
	testcontainerskafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	"github.com/testcontainers/testcontainers-go/wait"
	"golang.org/x/sync/errgroup"
	"sync"
	"testing"
	"time"
)

const (
	LocalstackContTag  = "localstack/localstack:3.0.2"
	LocalStackContPort = "4566/tcp"
)

var setupAppMu sync.Mutex

var localstackCont testcontainers.Container
var kafkaCont *testcontainerskafka.KafkaContainer

func SetupAppTest(t *testing.T) {
	// global lock for the entire initialization phase.
	// this prevents multiple containers for the same infra from being spawned and guarantees all tests share the same "App" instance
	setupAppMu.Lock()
	defer setupAppMu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	eg, egCtx := errgroup.WithContext(ctx)

	if localstackCont == nil {
		eg.Go(func() error {
			start := time.Now()
			cont, err := testcontainers.GenericContainer(egCtx, testcontainers.GenericContainerRequest{
				Started: true,
				ContainerRequest: testcontainers.ContainerRequest{
					Image:        LocalstackContTag,
					ExposedPorts: []string{LocalStackContPort},
					WaitingFor:   wait.ForListeningPort(LocalStackContPort),
				},
			})
			if err != nil {
				return fmt.Errorf("failed to start localstack container: %w", err)
			}
			localstackCont = cont
			t.Logf("finished starting localstack container in %v", time.Since(start))
			return nil
		})
	}
	if kafkaCont == nil {
		eg.Go(func() error {
			start := time.Now()
			cont, err := testcontainerskafka.Run(egCtx, "confluentinc/confluent-local:7.5.0")
			if err != nil {
				return fmt.Errorf("failed to start kafka container: %w", err)
			}
			kafkaCont = cont
			t.Logf("finished starting kafka container in %v", time.Since(start))
			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		t.Fatalf("failed to start containers: %s", err)
	}

	host, _ := localstackCont.Host(ctx)
	port, _ := localstackCont.MappedPort(ctx, LocalStackContPort)
	endpoint := fmt.Sprintf("http://%s:%s", host, port.Port())

	brokers, err := kafkaCont.Brokers(ctx)
	if err != nil {
		t.Fatalf("failed to get kafka brokers: %s", err)
	}

	InitApp(ctx, flog.Config{
		AwsRegion:    "us-east-1",
		AwsSecretKey: "testing",
		AwsSecretID:  "testing",
		Endpoint:     endpoint,

		KafkaBrokers: brokers,
		GroupID:      "test-group-id",

		ErrorLogBucket: "error-log-" + uuid.NewString(),
		CnctBucket:     "cncts-" + uuid.NewString(),
		AcctBucket:     "accts-" + uuid.NewString(),
		HoldBucket:     "holds-" + uuid.NewString(),
		TxnBucket:      "txns-" + uuid.NewString(),

		ErrorLogTopic:       "error-log" + uuid.NewString(),
		CnctRefreshTopic:    "cnct-refresh" + uuid.NewString(),
		AcctRefreshTopic:    "acct-refresh" + uuid.NewString(),
		TxnRefreshTopic:     "txn-refresh" + uuid.NewString(),
		HoldRefreshTopic:    "hold-refresh" + uuid.NewString(),
		CnctEnrichmentTopic: "cnct-enrichment" + uuid.NewString(),
		AcctEnrichmentTopic: "acct-enrichment" + uuid.NewString(),
		TxnEnrichmentTopic:  "txn-enrichment" + uuid.NewString(),
		HoldEnrichmentTopic: "hold-enrichment" + uuid.NewString(),
	})

	for _, bucket := range []string{
		app.ErrorLogBucket,
		app.CnctBucket,
		app.AcctBucket,
		app.HoldBucket,
		app.TxnBucket,
	} {
		if _, err := app.S3Client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
			t.Fatalf("failed to create bucket %s: %s", bucket, err)
		}
	}
}

func Equal[T any](t *testing.T, expected, actual T, opts ...cmp.Option) {
	t.Helper()
	if diff := cmp.Diff(expected, actual, opts...); diff != "" {
		t.Errorf("\n%s", diff)
	}
}
