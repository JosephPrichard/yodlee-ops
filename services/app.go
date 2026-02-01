package svc

import (
	"context"
	flog "filogger"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/segmentio/kafka-go"
)

type App struct {
	Aws
	Kafka
}

var app *App

func InitApp(ctx context.Context, config flog.Config) {
	if app != nil {
		return
	}

	producers := &kafka.Writer{
		Addr: kafka.TCP(config.KafkaBrokers...),
	}
	consumers := MakeConsumers(config)
	awsClient, err := MakeAwsClient(ctx, config)
	if err != nil {
		Fatal(ctx, "failed to make AWS client", err)
	}

	app = &App{
		Kafka: Kafka{
			Producer:  producers,
			Consumers: consumers,
		},
		Aws: awsClient,
	}
}

type Kafka struct {
	ErrorLogTopic       string
	CnctRefreshTopic    string
	AcctRefreshTopic    string
	HoldRefreshTopic    string
	TxnRefreshTopic     string
	CnctEnrichmentTopic string
	AcctEnrichmentTopic string
	HoldEnrichmentTopic string
	TxnEnrichmentTopic  string
	Consumers
	Producer *kafka.Writer
}

type Aws struct {
	S3Client       *s3.Client
	PageLength     *int32
	ErrorLogBucket string
	CnctBucket     string
	AcctBucket     string
	HoldBucket     string
	TxnBucket      string
}

func MakeAwsClient(ctx context.Context, cfg flog.Config) (Aws, error) {
	awsCfg, err := config.LoadDefaultConfig(
		ctx,
		config.WithRegion(cfg.AwsRegion),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(cfg.AwsSecretKey, cfg.AwsSecretID, "")),
	)
	if err != nil {
		return Aws{}, fmt.Errorf("failed to load AWS config, %w", err)
	}

	return Aws{
		S3Client: s3.NewFromConfig(awsCfg, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
			o.UsePathStyle = true
		}),
		ErrorLogBucket: cfg.ErrorLogBucket,
		CnctBucket:     cfg.CnctBucket,
		AcctBucket:     cfg.AcctBucket,
		HoldBucket:     cfg.HoldBucket,
		TxnBucket:      cfg.TxnBucket,
	}, nil
}
