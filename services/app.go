package svc

import (
	"context"
	"errors"
	"fmt"
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

type App struct {
	Aws
	Kafka
}

func MakeApp(ctx context.Context, config Config) *App {
	awsClient, err := MakeAwsClient(ctx, config)
	if err != nil {
		Fatal(ctx, "failed to make AWS client", err)
	}

	producers := &kafka.Writer{
		Addr: kafka.TCP(config.KafkaBrokers...),
	}
	makeReader := func(topic string) *kafka.Reader {
		return kafka.NewReader(kafka.ReaderConfig{
			Brokers:        config.KafkaBrokers,
			GroupID:        config.GroupID,
			Topic:          topic,
			CommitInterval: time.Second, // auto-commit
		})
	}

	kafkaClient := Kafka{
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
		CnctEnrichmentConsumer: makeReader(config.CnctEnrichmentTopic),
		AcctEnrichmentConsumer: makeReader(config.AcctEnrichmentTopic),
		TxnEnrichmentConsumer:  makeReader(config.TxnEnrichmentTopic),
		HoldEnrichmentConsumer: makeReader(config.HoldEnrichmentTopic),
		CnctRefreshConsumer:    makeReader(config.CnctRefreshTopic),
		AcctRefreshConsumer:    makeReader(config.AcctRefreshTopic),
		HoldRefreshConsumer:    makeReader(config.HoldRefreshTopic),
		TxnRefreshConsumer:     makeReader(config.TxnRefreshTopic),
		DeleteRcoveryConsumer:  makeReader(config.DeleteRecoveryTopic),
	}
	return &App{Kafka: kafkaClient, Aws: awsClient}
}

func (app *App) Close() error {
	close := func(r io.Closer) error {
		if r == nil {
			return nil
		}
		return r.Close()
	}

	return errors.Join(
		close(app.CnctEnrichmentConsumer),
		close(app.AcctEnrichmentConsumer),
		close(app.HoldEnrichmentConsumer),
		close(app.TxnEnrichmentConsumer),
		close(app.CnctRefreshConsumer),
		close(app.AcctRefreshConsumer),
		close(app.HoldRefreshConsumer),
		close(app.TxnRefreshConsumer),
	)
}

type Kafka struct {
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
	DeleteRcoveryConsumer  *kafka.Reader
}

type Aws struct {
	S3Client   *s3.Client
	PageLength *int32
	CnctBucket string
	AcctBucket string
	HoldBucket string
	TxnBucket  string
}

func MakeAwsClient(ctx context.Context, cfg Config) (Aws, error) {
	opts := []func(*config.LoadOptions) error{
		config.WithRegion(cfg.AwsDefaultRegion),
	}
	if cfg.IsUnitTest {
		opts = append(opts, config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("testing", "testing", "")))
	}

	awsCfg, err := config.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return Aws{}, fmt.Errorf("failed to load AWS config: %w", err)
	}

	return Aws{
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
	}, nil
}
