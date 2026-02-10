package infra

import (
	"context"
	"errors"
	"io"
	"log"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/segmentio/kafka-go"
)

const (
	CnctRefreshTopic    = "cnct-refreshes"
	AcctRefreshTopic    = "acct-refreshes"
	HoldRefreshTopic    = "hold-refreshes"
	TxnRefreshTopic     = "txn-refreshes"
	CnctResponseTopic   = "cnct-responses"
	AcctResponseTopic   = "acct-responses"
	HoldResponseTopic   = "hold-responses"
	TxnResponseTopic    = "txn-responses"
	DeleteRecoveryTopic = "delete-recovery"
	BroadcastTopic      = "broadcast"
	CnctBucket          = "yodlee-cncts"
	AcctBucket          = "yodlee-accts"
	HoldBucket          = "yodlee-holds"
	TxnBucket           = "yodlee-txns"
)

type Config struct {
	AwsEndpoint      string
	AwsDefaultRegion string
	IsLocal          bool // a special flag to tell the app to use hardcoded credentials when connecting to local infra.
	KafkaBrokers     []string
}

type Clients struct {
	*KafkaClient
	*AwsClient
}

type KafkaClient struct {
	KafkaBrokers []string

	Producer Producer

	CnctResponseConsumer Consumer
	AcctResponseConsumer Consumer
	HoldResponseConsumer Consumer
	TxnResponseConsumer  Consumer
	CnctRefreshConsumer  Consumer
	AcctRefreshConsumer  Consumer
	HoldRefreshConsumer  Consumer
	TxnRefreshConsumer   Consumer
	DeleteRetryConsumer  Consumer
	BroadcastConsumer    Consumer
}

type Producer interface {
	io.Closer
	WriteMessages(context.Context, ...kafka.Message) error
}

type Consumer interface {
	io.Closer
	Config() kafka.ReaderConfig
	ReadMessage(context.Context) (kafka.Message, error)
	CommitMessages(context.Context, ...kafka.Message) error
}

func MakeKafkaData(brokers []string) *KafkaClient {
	return &KafkaClient{
		KafkaBrokers: brokers,
	}
}

func MakeKafkaProducers(config Config) *KafkaClient {
	producers := &kafka.Writer{
		Addr: kafka.TCP(config.KafkaBrokers...),
	}
	client := MakeKafkaData(config.KafkaBrokers)
	client.Producer = producers
	return client
}

func MakeKafkaConsumerProducer(config Config) *KafkaClient {
	producers := &kafka.Writer{
		Addr: kafka.TCP(config.KafkaBrokers...),
	}
	log.Printf("created kafka producer with config: %+v", producers)

	compute := func(topic string) *kafka.Reader {
		cfg := kafka.ReaderConfig{
			GroupID: "compute-consumer-group-id",
			Brokers: config.KafkaBrokers,
			Topic:   topic,
		}
		log.Printf("creating kafka compute consumer with config: %+v", cfg)
		return kafka.NewReader(cfg)
	}

	broadcast := func(topic string) *kafka.Reader {
		cfg := kafka.ReaderConfig{
			Brokers: config.KafkaBrokers,
			Topic:   topic,
		}
		log.Printf("created kafka broadcast consumer with config: %+v", cfg)
		return kafka.NewReader(cfg)
	}

	client := MakeKafkaData(config.KafkaBrokers)

	client.Producer = producers
	client.CnctResponseConsumer = compute(CnctResponseTopic)
	client.AcctResponseConsumer = compute(AcctResponseTopic)
	client.TxnResponseConsumer = compute(TxnResponseTopic)
	client.HoldResponseConsumer = compute(HoldResponseTopic)
	client.CnctRefreshConsumer = compute(CnctRefreshTopic)
	client.AcctRefreshConsumer = compute(AcctRefreshTopic)
	client.HoldRefreshConsumer = compute(HoldRefreshTopic)
	client.TxnRefreshConsumer = compute(TxnRefreshTopic)
	client.DeleteRetryConsumer = compute(DeleteRecoveryTopic)
	client.BroadcastConsumer = broadcast(BroadcastTopic)

	return client
}

func (k *KafkaClient) Close() error {
	shutdown := func(r io.Closer) error {
		if r == nil {
			return nil
		}
		return r.Close()
	}

	return errors.Join(
		shutdown(k.CnctResponseConsumer),
		shutdown(k.AcctResponseConsumer),
		shutdown(k.HoldResponseConsumer),
		shutdown(k.TxnResponseConsumer),
		shutdown(k.CnctRefreshConsumer),
		shutdown(k.AcctRefreshConsumer),
		shutdown(k.HoldRefreshConsumer),
		shutdown(k.TxnRefreshConsumer),
		shutdown(k.DeleteRetryConsumer),
		shutdown(k.BroadcastConsumer),
		shutdown(k.Producer),
	)
}

type AwsClient struct {
	S3Client   S3Client
	PageLength *int32
	CnctBucket string
	AcctBucket string
	HoldBucket string
	TxnBucket  string
}

type S3Client interface {
	PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	ListObjectsV2(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
	DeleteObjects(ctx context.Context, params *s3.DeleteObjectsInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectsOutput, error)
}

func MakeAwsClient(cfg Config) *AwsClient {
	opts := []func(*config.LoadOptions) error{
		config.WithRegion(cfg.AwsDefaultRegion),
	}
	if cfg.IsLocal {
		opts = append(opts, config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("testing", "testing", "")))
	}

	awsCfg, err := config.LoadDefaultConfig(context.Background(), opts...)
	if err != nil {
		log.Fatalf("failed to load AWS config: %v", err)
	}

	return &AwsClient{
		S3Client: s3.NewFromConfig(awsCfg, func(o *s3.Options) {
			if cfg.AwsEndpoint != "" {
				o.BaseEndpoint = aws.String(cfg.AwsEndpoint)
				o.UsePathStyle = true
			}
		}),
		PageLength: nil,
		CnctBucket: CnctBucket,
		AcctBucket: AcctBucket,
		HoldBucket: HoldBucket,
		TxnBucket:  TxnBucket,
	}
}
