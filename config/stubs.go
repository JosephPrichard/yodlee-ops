package cfg

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"slices"
)

type StubUnstableS3Client struct {
	StableS3Client S3Client
	FailPutKey     string
	FailGetKey     string
	FailListPrefix map[string]string
	FailDeleteKeys []string
}

func (m *StubUnstableS3Client) PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	if m.FailPutKey == *params.Key {
		return nil, fmt.Errorf("mock: failed to put object: %s", *params.Key)
	}
	return m.StableS3Client.PutObject(ctx, params, optFns...)
}

func (m *StubUnstableS3Client) GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	if m.FailGetKey == *params.Key {
		return nil, fmt.Errorf("mock: failed to get object: %s", *params.Key)
	}
	return m.StableS3Client.GetObject(ctx, params, optFns...)
}

func (m *StubUnstableS3Client) ListObjectsV2(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	prefixFailList := m.FailListPrefix[*params.Bucket]
	if prefixFailList == *params.Prefix {
		return nil, fmt.Errorf("mock: failed to list objects: %s", *params.Prefix)
	}
	return m.StableS3Client.ListObjectsV2(ctx, params, optFns...)
}

func (m *StubUnstableS3Client) DeleteObjects(ctx context.Context, params *s3.DeleteObjectsInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectsOutput, error) {
	objectIdsToStrList := func(oids []s3types.ObjectIdentifier) []string {
		var strKeys []string
		for _, oid := range oids {
			if oid.Key != nil {
				strKeys = append(strKeys, *oid.Key)
			}
		}
		return strKeys
	}

	inputIds := objectIdsToStrList(params.Delete.Objects)
	slices.Sort(inputIds)

	deleteKeysSorted := slices.Clone(m.FailDeleteKeys)
	slices.Sort(deleteKeysSorted)

	if slices.Equal(deleteKeysSorted, inputIds) {
		return nil, fmt.Errorf("mock: failed to delete objects: %+v", params.Delete.Objects)
	}
	return m.StableS3Client.DeleteObjects(ctx, params, optFns...)
}
