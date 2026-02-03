package svc

import (
	"bytes"
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/go-cmp/cmp"
	"github.com/google/uuid"
	"github.com/testcontainers/testcontainers-go"
	testcontainerskafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	"github.com/testcontainers/testcontainers-go/wait"
	"golang.org/x/sync/errgroup"
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
	awsEndpoint := fmt.Sprintf("http://%s:%s", host, port.Port())
	t.Logf("aws endpoint: %s", awsEndpoint)

	brokers, err := kafkaCont.Brokers(ctx)
	if err != nil {
		t.Fatalf("failed to get kafka brokers: %s", err)
	}
	t.Logf("kafka brokers: %v", brokers)

	// test configuration for infra.
	cfg := Config{
		AwsDefaultRegion: "us-east-1",
		AwsEndpoint:      awsEndpoint,
		IsUnitTest:       true,

		KafkaBrokers: brokers,
		GroupID:      TestGroupID,

		CnctBucket: unique("cncts"),
		AcctBucket: unique("accts"),
		HoldBucket: unique("holds"),
		TxnBucket:  unique("txns"),

		CnctRefreshTopic:    unique("cnct-refresh"),
		AcctRefreshTopic:    unique("acct-refresh"),
		TxnRefreshTopic:     unique("txn-refresh"),
		HoldRefreshTopic:    unique("hold-refresh"),
		CnctEnrichmentTopic: unique("cnct-enrichment"),
		AcctEnrichmentTopic: unique("acct-enrichment"),
		TxnEnrichmentTopic:  unique("txn-enrichment"),
		HoldEnrichmentTopic: unique("hold-enrichment"),
		DeleteRecoveryTopic: unique("delete-retry"),
	}

	// create s3 buckets and kafka topics required for testing.
	createBuckets(ctx, t, cfg)
	createTopics(t, cfg)

	// make the app state and connect to the infra.
	return MakeApp(ctx, cfg)
}

func createBuckets(ctx context.Context, t *testing.T, cfg Config) {
	awsCfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(cfg.AwsDefaultRegion),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("testing", "testing", "")),
	)
	if err != nil {
		t.Fatalf("failed to load AWS config: %s", err)
	}
	s3Client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(cfg.AwsEndpoint)
		o.UsePathStyle = true
	})
	for _, bucket := range []string{
		cfg.CnctBucket,
		cfg.AcctBucket,
		cfg.HoldBucket,
		cfg.TxnBucket,
	} {
		if _, err := s3Client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
			t.Fatalf("failed to create bucket %s: %s", bucket, err)
		}
	}
}

func createTopics(t *testing.T, cfg Config) {
	conn, err := kafka.Dial("tcp", cfg.KafkaBrokers[0])
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

	topics := []string{
		cfg.CnctRefreshTopic,
		cfg.AcctRefreshTopic,
		cfg.HoldRefreshTopic,
		cfg.TxnRefreshTopic,
		cfg.CnctEnrichmentTopic,
		cfg.AcctEnrichmentTopic,
		cfg.TxnEnrichmentTopic,
		cfg.HoldEnrichmentTopic,
		cfg.DeleteRecoveryTopic,
	}
	for _, topic := range topics {
		if err = conn.CreateTopics(kafka.TopicConfig{
			Topic:             topic,
			NumPartitions:     1,
			ReplicationFactor: 1,
		}); err != nil {
			t.Fatalf("failed to create topic '%s': %s", topic, err)
		}
	}
	t.Logf("created topics: %+v", topics)
}

func unique(str string) string {
	return str + "-" + uuid.NewString()
}

func Equal[T any](t *testing.T, expected, actual T, opts ...cmp.Option) {
	t.Helper()
	if diff := cmp.Diff(expected, actual, opts...); diff != "" {
		t.Errorf("\n%s", diff)
	}
}

func seedS3Buckets(t *testing.T, app *App) {
	// seed the bucket with data to test deletions AND to ensure that inserts handle existing keys properly
	// "body" can be anything because insertion does not look at this information
	for _, record := range []struct {
		Bucket string
		Key    string
	}{
		{Bucket: app.CnctBucket, Key: "p1/Y/c1/2025-06-12"},
		{Bucket: app.CnctBucket, Key: "p1/Y/c1/2025-06-13"},
		{Bucket: app.CnctBucket, Key: "p1/Y/c2/2025-06-14"},
		{Bucket: app.CnctBucket, Key: "p1/Y/c3/2025-06-15"},

		{Bucket: app.AcctBucket, Key: "p1/Y/c1/a1/2025-06-12"},
		{Bucket: app.AcctBucket, Key: "p1/Y/c1/a1/2025-06-13"},
		{Bucket: app.AcctBucket, Key: "p2/Y/c2/a2/2025-06-14"},
		{Bucket: app.AcctBucket, Key: "p2/Y/c3/a3/2025-06-15"},

		{Bucket: app.HoldBucket, Key: "p1/Y/a1/h1/2025-06-12"},
		{Bucket: app.HoldBucket, Key: "p1/Y/a1/h1/2025-06-13"},
		{Bucket: app.HoldBucket, Key: "p2/Y/a1/h1/2025-06-14"},
		{Bucket: app.HoldBucket, Key: "p2/Y/a2/h2/2025-06-15"},

		{Bucket: app.TxnBucket, Key: "p1/Y/a1/t1/2025-06-12T00:14:37Z"},
		{Bucket: app.TxnBucket, Key: "p1/Y/a1/t1/2025-06-12T02:48:09Z"},
		{Bucket: app.TxnBucket, Key: "p2/Y/a1/t1/2025-06-13T02:48:09Z"},
		{Bucket: app.TxnBucket, Key: "p2/Y/a2/t2/2025-06-14T07:06:18Z"},
	} {
		_, err := app.S3Client.PutObject(t.Context(), &s3.PutObjectInput{
			Bucket: aws.String(record.Bucket),
			Key:    aws.String(record.Key),
			Body:   bytes.NewReader([]byte("test")),
		})
		require.NoError(t, err)
	}
}

type wantObject struct {
	bucket string
	key    string
	value  proto.Message
}

func assertProtoObjects(t *testing.T, app *App, objects []wantObject, makeValue func() proto.Message) {
	t.Helper()

	for _, object := range objects {
		data, err := app.S3Client.GetObject(context.Background(), &s3.GetObjectInput{
			Bucket: aws.String(object.bucket),
			Key:    aws.String(object.key),
		})
		if err != nil {
			t.Errorf("failed to get object %s/%s: %v", object.bucket, object.key, err)
			continue
		}

		body, err := io.ReadAll(data.Body)
		require.NoError(t, err)

		t.Logf("got object %s/%s: %s", object.bucket, object.key, string(body))

		s3Value := makeValue()
		require.NoError(t, proto.Unmarshal(body, s3Value))

		Equal(t, object.value, s3Value, protocmp.Transform())
	}
}
