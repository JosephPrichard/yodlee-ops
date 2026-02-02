package svc

import (
	"context"
	flog "filogger"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/go-cmp/cmp"
	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
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
	KafkaContTag       = "confluentinc/confluent-local:7.4.0"
	LocalStackContPort = "4566/tcp"
	TestGroupID        = "test-group"
)

var setupAppMu sync.Mutex

var localstackCont testcontainers.Container
var kafkaCont *testcontainerskafka.KafkaContainer

func SetupAppTest(t *testing.T) *App {
	// global lock for the entire initialization phase.
	// this prevents multiple containers for the same infra from being spawned and guarantees all tests share the same "App" instance
	setupAppMu.Lock()
	defer setupAppMu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// start all containers concurrently.
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
			cont, err := testcontainerskafka.Run(egCtx, KafkaContTag, testcontainerskafka.WithClusterID(TestGroupID))
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

	// parse connection data from the localstack container and kafka container.
	host, _ := localstackCont.Host(ctx)
	port, _ := localstackCont.MappedPort(ctx, LocalStackContPort)
	endpoint := fmt.Sprintf("http://%s:%s", host, port.Port())
	t.Logf("aws endpoint: %s", endpoint)

	brokers, err := kafkaCont.Brokers(ctx)
	if err != nil {
		t.Fatalf("failed to get kafka brokers: %s", err)
	}
	t.Logf("kafka brokers: %v", brokers)

	conn, err := kafka.Dial("tcp", brokers[0])
	if err != nil {
		t.Fatalf("failed to connect to kafka: %s", err)
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		t.Fatalf("failed to get kafka controller: %s", err)
	}
	controllerEndpoint := fmt.Sprintf("%s:%d", controller.Host, controller.Port)
	controllerConn, err := kafka.Dial("tcp", controllerEndpoint)
	if err != nil {
		t.Fatalf("failed to connect to kafka controller: %s", err)
	}
	defer controllerConn.Close()

	t.Logf("kafka controller endpoint: %+v", controllerEndpoint)

	// initialize app with config.
	InitLoggers(nil)
	app := MakeApp(ctx, flog.Config{
		AwsRegion:    "us-east-1",
		AwsSecretKey: "testing",
		AwsSecretID:  "testing",
		Endpoint:     endpoint,

		KafkaBrokers: brokers,
		GroupID:      TestGroupID,

		CnctBucket: "cncts-" + uuid.NewString(),
		AcctBucket: "accts-" + uuid.NewString(),
		HoldBucket: "holds-" + uuid.NewString(),
		TxnBucket:  "txns-" + uuid.NewString(),

		CnctRefreshTopic:    "cnct-refresh-" + uuid.NewString(),
		AcctRefreshTopic:    "acct-refresh-" + uuid.NewString(),
		TxnRefreshTopic:     "txn-refresh-" + uuid.NewString(),
		HoldRefreshTopic:    "hold-refresh-" + uuid.NewString(),
		CnctEnrichmentTopic: "cnct-enrichment-" + uuid.NewString(),
		AcctEnrichmentTopic: "acct-enrichment-" + uuid.NewString(),
		TxnEnrichmentTopic:  "txn-enrichment-" + uuid.NewString(),
		HoldEnrichmentTopic: "hold-enrichment-" + uuid.NewString(),
	})

	// create s3 buckets and kafka topics.
	for _, bucket := range []string{
		app.CnctBucket,
		app.AcctBucket,
		app.HoldBucket,
		app.TxnBucket,
	} {
		if _, err := app.S3Client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
			t.Fatalf("failed to create bucket %s: %s", bucket, err)
		}
	}

	for _, topic := range []string{
		app.CnctRefreshTopic,
		app.AcctRefreshTopic,
		app.HoldRefreshTopic,
		app.TxnRefreshTopic,
		app.CnctEnrichmentTopic,
		app.AcctEnrichmentTopic,
		app.TxnEnrichmentTopic,
		app.HoldEnrichmentTopic,
	} {
		if err = conn.CreateTopics(kafka.TopicConfig{
			Topic:             topic,
			NumPartitions:     3,
			ReplicationFactor: 1,
		}); err != nil {
			t.Fatalf("failed to create topic %s: %s", topic, err)
		}
	}

	return app
}

func Equal[T any](t *testing.T, expected, actual T, opts ...cmp.Option) {
	t.Helper()
	if diff := cmp.Diff(expected, actual, opts...); diff != "" {
		t.Errorf("\n%s", diff)
	}
}
