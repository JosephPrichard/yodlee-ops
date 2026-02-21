package infrastub

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"reflect"
	"yodleeops/internal/infra"
)

type BadS3Client struct {
	GoodS3Client infra.S3Client
	BadS3ClientCfg
}

type BadS3ClientCfg struct {
	FailPutKey     string
	FailGetKey     string
	FailListPrefix map[infra.Bucket]string
	FailDeleteKeys map[string]bool
}

func MakeBadS3Client(awsClient *infra.AwsClient, cfg BadS3ClientCfg) {
	goodS3Client := awsClient.S3Client
	awsClient.S3Client = &BadS3Client{GoodS3Client: goodS3Client, BadS3ClientCfg: cfg}
}

func (s *BadS3Client) PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	if s.FailPutKey == *params.Key {
		return nil, fmt.Errorf("stub: failed to put object: %s", *params.Key)
	} else {
		return s.GoodS3Client.PutObject(ctx, params, optFns...)
	}
}

func (s *BadS3Client) GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	if s.FailGetKey == *params.Key {
		return nil, fmt.Errorf("stub: failed to get object: %s", *params.Key)
	} else {
		return s.GoodS3Client.GetObject(ctx, params, optFns...)
	}
}

func (s *BadS3Client) ListObjectsV2(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	prefixFailList := s.FailListPrefix[infra.Bucket(*params.Bucket)]
	if prefixFailList == *params.Prefix {
		return nil, fmt.Errorf("stub: failed to list objects: %s", *params.Prefix)
	} else {
		return s.GoodS3Client.ListObjectsV2(ctx, params, optFns...)
	}
}

func (s *BadS3Client) DeleteObjects(ctx context.Context, params *s3.DeleteObjectsInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectsOutput, error) {
	deleteOids := make(map[string]bool)
	for _, oid := range params.Delete.Objects {
		deleteOids[*oid.Key] = true
	}
	if reflect.DeepEqual(s.FailDeleteKeys, deleteOids) {
		return nil, fmt.Errorf("stub: failed to delete objects: %+v", params.Delete.Objects)
	} else {
		return s.GoodS3Client.DeleteObjects(ctx, params, optFns...)
	}
}
