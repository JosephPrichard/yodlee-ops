package fakes

import (
	"context"
	"fmt"
	"reflect"

	"yodleeops/infra"

	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type BadS3 struct {
	infra.S3
	BadS3Config
}

type BadS3Config struct {
	FailPutKey     string
	FailGetKey     string
	FailListPrefix map[infra.Bucket]string
	FailDeleteKeys map[string]bool
}

func MakeBadS3Client(awsClient *infra.AWS, cfg BadS3Config) {
	goodS3Client := awsClient.S3
	awsClient.S3 = &BadS3{S3: goodS3Client, BadS3Config: cfg}
}

func (s *BadS3) PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	if s.FailPutKey == *params.Key {
		return nil, fmt.Errorf("stub: failed to put object: %s", *params.Key)
	} else {
		return s.S3.PutObject(ctx, params, optFns...)
	}
}

func (s *BadS3) GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	if s.FailGetKey == *params.Key {
		return nil, fmt.Errorf("stub: failed to get object: %s", *params.Key)
	} else {
		return s.S3.GetObject(ctx, params, optFns...)
	}
}

func (s *BadS3) ListObjectsV2(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	prefixFailList := s.FailListPrefix[infra.Bucket(*params.Bucket)]
	if prefixFailList == *params.Prefix {
		return nil, fmt.Errorf("stub: failed to list objects: %s", *params.Prefix)
	} else {
		return s.S3.ListObjectsV2(ctx, params, optFns...)
	}
}

func (s *BadS3) DeleteObjects(ctx context.Context, params *s3.DeleteObjectsInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectsOutput, error) {
	deleteOids := make(map[string]bool)
	for _, oid := range params.Delete.Objects {
		deleteOids[*oid.Key] = true
	}
	if reflect.DeepEqual(s.FailDeleteKeys, deleteOids) {
		return nil, fmt.Errorf("stub: failed to delete objects: %+v", params.Delete.Objects)
	} else {
		return s.S3.DeleteObjects(ctx, params, optFns...)
	}
}
