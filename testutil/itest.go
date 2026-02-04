package testutil

import (
	"context"
	cfg "filogger/config"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/segmentio/kafka-go"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
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

type TestFlag int

const (
	Aws TestFlag = iota
	Kafka
)

var setupAppMu sync.Mutex

var localstackCont testcontainers.Container
var kafkaCont *testcontainerskafka.KafkaContainer

func SetupITest(t *testing.T, flags ...TestFlag) cfg.Config {
	withKafka := slices.Contains(flags, Kafka)
	withAws := slices.Contains(flags, Aws)

	// global lock for the entire initialization phase.
	// this prevents multiple containers for the same infra from being spawned and guarantees all tests share the same "App" instance
	setupAppMu.Lock()
	defer setupAppMu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// start all containers concurrently.
	eg, egCtx := errgroup.WithContext(ctx)

	var appCfg cfg.Config

	if localstackCont == nil && withAws {
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
	if kafkaCont == nil && withKafka {
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

	if withAws {
		host, _ := localstackCont.Host(ctx)
		port, _ := localstackCont.MappedPort(ctx, LocalStackContPort)
		awsEndpoint := fmt.Sprintf("http://%s:%s", host, port.Port())
		t.Logf("aws endpoint: %s", awsEndpoint)

		appCfg.AwsConfig = cfg.AwsConfig{
			AwsDefaultRegion: "us-east-1",
			AwsEndpoint:      awsEndpoint,
			IsUnitTest:       true,

			CnctBucket: unique("cncts"),
			AcctBucket: unique("accts"),
			HoldBucket: unique("holds"),
			TxnBucket:  unique("txns"),
		}
		createBuckets(ctx, t, appCfg.AwsConfig)
	}

	if withKafka {
		brokers, err := kafkaCont.Brokers(ctx)
		if err != nil {
			t.Fatalf("failed to get kafka brokers: %s", err)
		}
		t.Logf("kafka brokers: %v", brokers)
		appCfg.KafkaConfig = cfg.KafkaConfig{
			KafkaBrokers: brokers,
			GroupID:      TestGroupID,

			CnctRefreshTopic:    unique("cnct-refresh"),
			AcctRefreshTopic:    unique("acct-refresh"),
			TxnRefreshTopic:     unique("txn-refresh"),
			HoldRefreshTopic:    unique("hold-refresh"),
			CnctEnrichmentTopic: unique("cnct-enrichment"),
			AcctEnrichmentTopic: unique("acct-enrichment"),
			TxnEnrichmentTopic:  unique("txn-enrichment"),
			HoldEnrichmentTopic: unique("hold-enrichment"),
			DeleteRecoveryTopic: unique("delete-retry"),

			// consumers
			CommitInterval: time.Microsecond,
			MaxWait:        time.Millisecond * 5,
			MinBytes:       1,
			MaxBytes:       1e6,
			// producers
			BatchTimeout: time.Millisecond * 5,
			BatchSize:    1,
		}
		createTopics(t, appCfg.KafkaConfig)
	}

	return appCfg
}

func createBuckets(ctx context.Context, t *testing.T, cfg cfg.AwsConfig) {
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

func createTopics(t *testing.T, cfg cfg.KafkaConfig) {
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
