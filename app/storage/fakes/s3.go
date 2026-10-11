package fakes

import (
	"context"
	"fmt"
	"reflect"
	"yodleeops/storage"

	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type BadS3 struct {
	storage.S3API
	BadS3Config
}

type BadS3Config struct {
	FailPutKey     string
	FailGetKey     string
	FailListPrefix map[storage.Bucket]string
	FailDeleteKeys map[string]bool
}

func SetBadS3Client(s3Client *storage.S3Client, cfg BadS3Config) {
	goodS3Client := s3Client.S3API
	s3Client.S3API = &BadS3{S3API: goodS3Client, BadS3Config: cfg}
}

func (s *BadS3) PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	if s.FailPutKey == *params.Key {
		return nil, fmt.Errorf("stub: failed to put object: %s", *params.Key)
	}
	return s.S3API.PutObject(ctx, params, optFns...)
}

func (s *BadS3) GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	if s.FailGetKey == *params.Key {
		return nil, fmt.Errorf("stub: failed to get object: %s", *params.Key)
	}
	return s.S3API.GetObject(ctx, params, optFns...)
}

func (s *BadS3) ListObjectsV2(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	prefixFailList := s.FailListPrefix[storage.Bucket(*params.Bucket)]
	if prefixFailList == *params.Prefix {
		return nil, fmt.Errorf("stub: failed to list objects: %s", *params.Prefix)
	}
	return s.S3API.ListObjectsV2(ctx, params, optFns...)
}

func (s *BadS3) DeleteObjects(ctx context.Context, params *s3.DeleteObjectsInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectsOutput, error) {
	deleteOids := make(map[string]bool)
	for _, oid := range params.Delete.Objects {
		deleteOids[*oid.Key] = true
	}
	if reflect.DeepEqual(s.FailDeleteKeys, deleteOids) {
		return nil, fmt.Errorf("stub: failed to delete objects: %+v", params.Delete.Objects)
	}
	return s.S3API.DeleteObjects(ctx, params, optFns...)
}
