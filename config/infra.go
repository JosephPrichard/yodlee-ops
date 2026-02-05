package cfg

import (
	"context"
	"errors"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/segmentio/kafka-go"
)

type Config struct {
	AwsConfig
	KafkaConfig
}

func ReadEnv() map[string]string {
	envMap := make(map[string]string)
	for _, env := range os.Environ() {
		kv := strings.SplitN(env, "=", 2)
		if len(kv) != 2 {
			continue
		}
		envMap[kv[0]] = kv[1]
	}
	return envMap
}

func MakeConfig(env map[string]string) Config {
	expectInt := func(v string) int {
		i, err := strconv.Atoi(v)
		if err != nil {
			log.Fatalf("invalid int in configuration: '%s'", v)
		}
		return i
	}

	expectDuration := func(v string) time.Duration {
		duration, err := time.ParseDuration(v)
		if err != nil {
			log.Fatalf("invalid duration in configuration: '%s'", v)
		}
		return duration
	}

	return Config{
		AwsConfig: AwsConfig{
			AwsEndpoint:      env["AWS_ENDPOINT"],
			AwsDefaultRegion: env["AWS_DEFAULT_REGION"],

			CnctBucket: env["CNCT_BUCKET"],
			AcctBucket: env["ACCT_BUCKET"],
			HoldBucket: env["HOLD_BUCKET"],
			TxnBucket:  env["TXN_BUCKET"],
		},
		KafkaConfig: KafkaConfig{
			KafkaBrokers:        strings.Split(env["KAFKA_BROKERS"], ","),
			CnctRefreshTopic:    env["CNCT_REFRESH_TOPIC"],
			AcctRefreshTopic:    env["ACCT_REFRESH_TOPIC"],
			HoldRefreshTopic:    env["HOLD_REFRESH_TOPIC"],
			TxnRefreshTopic:     env["TXN_REFRESH_TOPIC"],
			CnctEnrichmentTopic: env["CNCT_ENRICHMENT_TOPIC"],
			AcctEnrichmentTopic: env["ACCT_ENRICHMENT_TOPIC"],
			HoldEnrichmentTopic: env["HOLD_ENRICHMENT_TOPIC"],
			TxnEnrichmentTopic:  env["TXN_ENRICHMENT_TOPIC"],
			DeleteRecoveryTopic: env["DELETE_RETRY_TOPIC"],

			MaxWait:      expectDuration(env["CONSUMER_MAX_WAIT"]),
			MinBytes:     expectInt(env["CONSUMER_MIN_BYTES"]),
			MaxBytes:     expectInt(env["CONSUMER_MAX_BYTES"]),
			Concurrency:  expectInt(env["CONSUMER_CONCURRENCY"]),
			BatchTimeout: expectDuration(env["PRODUCER_BATCH_TIMEOUT"]),
			BatchSize:    expectInt(env["PRODUCER_BATCH_SIZE"]),
		},
	}
}

type AwsConfig struct {
	AwsEndpoint      string
	AwsDefaultRegion string
	IsLocal          bool // a special flag to tell the app to use hardcoded credentials when connecting to local infra.

	CnctBucket string
	AcctBucket string
	HoldBucket string
	TxnBucket  string
	PageLength *int32
}

type KafkaConfig struct {
	KafkaBrokers []string
	GroupID      string

	CnctRefreshTopic    string
	AcctRefreshTopic    string
	HoldRefreshTopic    string
	TxnRefreshTopic     string
	CnctEnrichmentTopic string
	AcctEnrichmentTopic string
	HoldEnrichmentTopic string
	TxnEnrichmentTopic  string
	DeleteRecoveryTopic string

	// consumers
	CommitInterval time.Duration
	MaxWait        time.Duration
	MinBytes       int
	MaxBytes       int
	Concurrency    int
	// producers
	BatchTimeout time.Duration
	BatchSize    int
}

func MakeKafka(config KafkaConfig) KafkaClient {
	producers := &kafka.Writer{
		Addr:         kafka.TCP(config.KafkaBrokers...),
		BatchSize:    config.BatchSize,
		BatchTimeout: config.BatchTimeout,
	}

	makeConsumer := func(topic string) *kafka.Reader {
		return kafka.NewReader(kafka.ReaderConfig{
			Brokers:        config.KafkaBrokers,
			GroupID:        config.GroupID,
			Topic:          topic,
			CommitInterval: config.CommitInterval,
			MaxWait:        config.MaxWait,
			MinBytes:       config.MinBytes,
			MaxBytes:       config.MaxBytes,
		})
	}
	kafkaClient := KafkaClient{
		KafkaBrokers:           config.KafkaBrokers,
		CnctRefreshTopic:       config.CnctRefreshTopic,
		AcctRefreshTopic:       config.AcctRefreshTopic,
		HoldRefreshTopic:       config.HoldRefreshTopic,
		TxnRefreshTopic:        config.TxnRefreshTopic,
		CnctEnrichmentTopic:    config.CnctEnrichmentTopic,
		AcctEnrichmentTopic:    config.AcctEnrichmentTopic,
		HoldEnrichmentTopic:    config.HoldEnrichmentTopic,
		TxnEnrichmentTopic:     config.TxnEnrichmentTopic,
		DeleteRecoveryTopic:    config.DeleteRecoveryTopic,
		Producer:               producers,
		CnctEnrichmentConsumer: makeConsumer(config.CnctEnrichmentTopic),
		AcctEnrichmentConsumer: makeConsumer(config.AcctEnrichmentTopic),
		TxnEnrichmentConsumer:  makeConsumer(config.TxnEnrichmentTopic),
		HoldEnrichmentConsumer: makeConsumer(config.HoldEnrichmentTopic),
		CnctRefreshConsumer:    makeConsumer(config.CnctRefreshTopic),
		AcctRefreshConsumer:    makeConsumer(config.AcctRefreshTopic),
		HoldRefreshConsumer:    makeConsumer(config.HoldRefreshTopic),
		TxnRefreshConsumer:     makeConsumer(config.TxnRefreshTopic),
		DeleteRecoveryConsumer: makeConsumer(config.DeleteRecoveryTopic),
	}

	return kafkaClient
}

func (k *KafkaClient) Close() error {
	shutdown := func(r io.Closer) error {
		if r == nil {
			return nil
		}
		return r.Close()
	}

	return errors.Join(
		shutdown(k.CnctEnrichmentConsumer),
		shutdown(k.AcctEnrichmentConsumer),
		shutdown(k.HoldEnrichmentConsumer),
		shutdown(k.TxnEnrichmentConsumer),
		shutdown(k.CnctRefreshConsumer),
		shutdown(k.AcctRefreshConsumer),
		shutdown(k.HoldRefreshConsumer),
		shutdown(k.TxnRefreshConsumer),
	)
}

type KafkaClient struct {
	KafkaBrokers           []string
	CnctRefreshTopic       string
	AcctRefreshTopic       string
	HoldRefreshTopic       string
	TxnRefreshTopic        string
	CnctEnrichmentTopic    string
	AcctEnrichmentTopic    string
	HoldEnrichmentTopic    string
	TxnEnrichmentTopic     string
	DeleteRecoveryTopic    string
	Producer               *kafka.Writer
	CnctEnrichmentConsumer *kafka.Reader
	AcctEnrichmentConsumer *kafka.Reader
	HoldEnrichmentConsumer *kafka.Reader
	TxnEnrichmentConsumer  *kafka.Reader
	CnctRefreshConsumer    *kafka.Reader
	AcctRefreshConsumer    *kafka.Reader
	HoldRefreshConsumer    *kafka.Reader
	TxnRefreshConsumer     *kafka.Reader
	DeleteRecoveryConsumer *kafka.Reader
}

type S3Client interface {
	PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	ListObjectsV2(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
	DeleteObjects(ctx context.Context, params *s3.DeleteObjectsInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectsOutput, error)
}

type AwsClient struct {
	S3Client   S3Client
	PageLength *int32
	CnctBucket string
	AcctBucket string
	HoldBucket string
	TxnBucket  string
}

func MakeAwsClient(cfg AwsConfig) AwsClient {
	opts := []func(*config.LoadOptions) error{
		config.WithRegion(cfg.AwsDefaultRegion),
	}
	if cfg.IsLocal {
		opts = append(opts, config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("testing", "testing", "")))
	}

	ctx := context.TODO()
	awsCfg, err := config.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		log.Fatalf("failed to load AWS config: %v", err)
	}

	return AwsClient{
		S3Client: s3.NewFromConfig(awsCfg, func(o *s3.Options) {
			if cfg.AwsEndpoint != "" {
				o.BaseEndpoint = aws.String(cfg.AwsEndpoint)
				o.UsePathStyle = true
			}
		}),
		PageLength: cfg.PageLength,
		CnctBucket: cfg.CnctBucket,
		AcctBucket: cfg.AcctBucket,
		HoldBucket: cfg.HoldBucket,
		TxnBucket:  cfg.TxnBucket,
	}
}
