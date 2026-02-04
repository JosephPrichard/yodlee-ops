package cfg

import (
	"context"
	"errors"
	"io"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/segmentio/kafka-go"
)

type Config struct {
	AwsEndpoint      string
	AwsDefaultRegion string
	IsUnitTest       bool // a special flag to tell app to use hardcoded credentials when connecting to local infra.

	CnctBucket string
	AcctBucket string
	HoldBucket string
	TxnBucket  string
	PageLength *int32

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
}

func MakeKafka(config Config) KafkaClient {
	producers := &kafka.Writer{
		Addr: kafka.TCP(config.KafkaBrokers...),
	}

	makeConsumer := func(topic string) *kafka.Reader {
		return kafka.NewReader(kafka.ReaderConfig{
			Brokers:        config.KafkaBrokers,
			GroupID:        config.GroupID,
			Topic:          topic,
			CommitInterval: time.Second, // auto-commit
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

func MakeAwsClient(cfg Config) AwsClient {
	opts := []func(*config.LoadOptions) error{
		config.WithRegion(cfg.AwsDefaultRegion),
	}
	if cfg.IsUnitTest {
		opts = append(opts, config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("testing", "testing", "")))
	}

	ctx := context.TODO()
	awsCfg, err := config.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		Fatal(ctx, "failed to load AWS config: %v", err)
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
