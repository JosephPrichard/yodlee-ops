package infra

import (
	"context"
	"log"
	"log/slog"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type Config struct {
	AwsEndpoint      string
	AwsDefaultRegion string
	IsLocal          bool // a special flag to tell the app to use hardcoded credentials when connecting to local infra.
	KafkaBrokers     []string
	AllowOrigins     string
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
		AllowOrigins:     envMap["ALLOW_ORIGINS"],
	}
}

type AWS struct {
	S3            S3
	PaginationLen *int32
	Buckets       Buckets
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
		slog.Warn("using static localstack credentials provider", "credentials", staticCreds)
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
	return AWS{
		S3:            s3Client,
		PaginationLen: nil,
		Buckets: Buckets{
			Connections:  CnctBucket,
			Accounts:     AcctBucket,
			Holdings:     HoldBucket,
			Transactions: TxnBucket,
		},
	}
}
