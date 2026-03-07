package infra

import (
	"context"
	"github.com/IBM/sarama"
	"log"
	"log/slog"
	"os"
	"strings"

	mskiam "github.com/aws/aws-msk-iam-sasl-signer-go/signer"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type Config struct {
	AwsEndpoint      string
	AwsDefaultRegion string
	IsLocal          bool // a special flag to tell the app to use hardcoded credentials when connecting to local
	KafkaBrokers     []string
}

func MakeConfig() Config {
	envMap := make(map[string]string)
	for _, env := range os.Environ() {
		kv := strings.SplitN(env, "=", 2)
		if len(kv) != 2 {
			continue
		}
		envMap[kv[0]] = kv[1]
	}

	return Config{
		AwsEndpoint:      envMap["AWS_ENDPOINT"],
		AwsDefaultRegion: envMap["AWS_DEFAULT_REGION"],
		KafkaBrokers:     strings.Split(envMap["KAFKA_BROKERS"], ","),
	}
}

type AWS struct {
	S3            S3
	PaginationLen *int32
}

type S3 interface {
	PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	ListObjectsV2(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
	DeleteObjects(ctx context.Context, params *s3.DeleteObjectsInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectsOutput, error)
}

func MakeS3Client(cfg Config) *s3.Client {
	awsOpts := []func(*config.LoadOptions) error{
		config.WithRegion(cfg.AwsDefaultRegion),
	}
	if cfg.IsLocal {
		staticCreds := credentials.NewStaticCredentialsProvider("testing", "testing", "")
		awsOpts = append(awsOpts, config.WithCredentialsProvider(staticCreds))
		slog.Info("configured static localstack credentials provider", "credentials", staticCreds)
	} else {
		slog.Info("configured AWS IAM auth provider for s3 client")
	}

	awsCfg, err := config.LoadDefaultConfig(context.Background(), awsOpts...)
	if err != nil {
		log.Fatalf("failed to load S3 config: %v", err)
	}

	var s3Opts []func(*s3.Options)

	if cfg.AwsEndpoint != "" {
		s3Opts = append(s3Opts, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(cfg.AwsEndpoint)
			o.UsePathStyle = true
		})
	}

	return s3.NewFromConfig(awsCfg, s3Opts...)
}

func MakeAWS(s3Client *s3.Client) AWS {
	return AWS{S3: s3Client, PaginationLen: nil}
}

func MakeSaramaConfig(config Config) *sarama.Config {
	kafkaConfig := sarama.NewConfig()
	kafkaConfig.Version = sarama.V3_6_0_0
	kafkaConfig.Producer.Return.Errors = true

	if config.IsLocal {
		slog.Info("configured kafka client without credentials")
	} else {
		kafkaConfig.Net.SASL.Enable = true
		kafkaConfig.Net.SASL.Mechanism = sarama.SASLTypeOAuth
		kafkaConfig.Net.SASL.TokenProvider = &IAMTokenProvider{config.AwsDefaultRegion}
		kafkaConfig.Net.TLS.Enable = true
		slog.Info("configured kafka client with AWS OAUTH IAM token provider")
	}

	return kafkaConfig
}

func MakeSaramaProducer(kafkaBrokers []string, kafkaConfig *sarama.Config) sarama.AsyncProducer {
	producer, err := sarama.NewAsyncProducer(kafkaBrokers, kafkaConfig)
	if err != nil {
		log.Fatalf("failed to create kafka producer: %v", err)
	}
	go func() {
		for err := range producer.Errors() {
			slog.Error("failed to produce message", "err", err)
		}
	}()
	return producer
}

type IAMTokenProvider struct {
	region string
}

func (p *IAMTokenProvider) Token() (*sarama.AccessToken, error) {
	token, _, err := mskiam.GenerateAuthToken(context.Background(), p.region)
	if err != nil {
		return nil, err
	}
	return &sarama.AccessToken{Token: token}, nil
}
