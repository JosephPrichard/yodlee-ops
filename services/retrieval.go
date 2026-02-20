package svc

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/go-faster/jx"
	"golang.org/x/sync/errgroup"
	"log/slog"
	"slices"
	"sync"
)

type OpsFiGeneric struct {
	OpsFiMessage
	Data map[string]jx.Raw `json:"data"`
}

type ProfileIDCursorPair struct {
	ProfileID string
	Cursor    string
}

type ListFiMetadataResult struct {
	OpsFiMetadata []OpsFiMetadata   `json:"opsFiMetadata"`
	Cursors       map[string]string `json:"cursors"`
}

func ListFiMetadataByProfileIDs(appCtx AppContext, bucket string, pairs []ProfileIDCursorPair) (results ListFiMetadataResult, err error) {
	nestedOpsFiMetadata := make([][]OpsFiMetadata, len(pairs))

	var cursorsLock sync.Mutex
	cursors := make(map[string]string, len(pairs))

	eg, egCtx := errgroup.WithContext(appCtx)
	appEgCtx := AppContext{Context: egCtx, App: appCtx.App}
	for i, pair := range pairs {
		eg.Go(func() error {
			opsFiMetadata, nextCursor, err := ListFiMetadataByProfileID(appEgCtx, bucket, pair.ProfileID, pair.Cursor)
			if err != nil {
				return fmt.Errorf("list metadata job index=%d: %w", i, err)
			}

			nestedOpsFiMetadata[i] = opsFiMetadata

			cursorsLock.Lock()
			if nextCursor != "" {
				cursors[pair.ProfileID] = nextCursor
			}
			cursorsLock.Unlock()

			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return results, err
	}

	opsFiMetadata := make([]OpsFiMetadata, 0)
	for _, nested := range nestedOpsFiMetadata {
		opsFiMetadata = append(opsFiMetadata, nested...)
	}
	slices.SortFunc(opsFiMetadata, func(left, right OpsFiMetadata) int {
		return right.LastUpdated.Compare(left.LastUpdated)
	})

	slog.InfoContext(appCtx, "retrieved metadata records", "bucket", bucket, "pairs", pairs, "opsFiMetadata", opsFiMetadata)

	return ListFiMetadataResult{OpsFiMetadata: opsFiMetadata, Cursors: cursors}, nil
}

func ListFiMetadataByProfileID(ctx AppContext, bucket string, profileID string, cursor string) ([]OpsFiMetadata, string, error) {
	var continuationToken *string
	if cursor != "" {
		continuationToken = aws.String(cursor)
	}
	output, err := ctx.S3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket:            aws.String(bucket),
		Prefix:            aws.String(profileID),
		ContinuationToken: continuationToken,
		MaxKeys:           ctx.AwsClient.PageLength,
	})
	if err != nil {
		return nil, "", fmt.Errorf("list objects by profileID=%s, cursor=%s and bucket=%s: %w", profileID, cursor, bucket, err)
	}

	opsFiMetadata := make([]OpsFiMetadata, 0)
	for _, obj := range output.Contents {
		if obj.Key == nil || obj.LastModified == nil {
			continue
		}
		metadataRecord := OpsFiMetadata{
			Key:          *obj.Key,
			LastModified: *obj.LastModified,
		}
		if err := metadataRecord.ParseOpsFiMetadata(ctx.S3Buckets, bucket, *obj.Key); err != nil {
			slog.ErrorContext(ctx, "failed to parse ops fi metadata record", "Key", *obj.Key, "err", err)
		} else {
			opsFiMetadata = append(opsFiMetadata, metadataRecord)
		}
	}

	hasMore := output.IsTruncated != nil && *output.IsTruncated
	nextCursor := ""
	if hasMore && output.NextContinuationToken != nil {
		nextCursor = *output.NextContinuationToken
	}

	slog.InfoContext(ctx, "retrieved metadata records", "bucket", bucket, "profileID", profileID, "opsFiMetadata", opsFiMetadata, "nextCursor", nextCursor)
	return opsFiMetadata, nextCursor, nil
}

var ErrKeyNotFound = errors.New("key not found")

func GetFiObject(ctx AppContext, bucket string, key string) (OpsFiGeneric, error) {
	object, err := ctx.S3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		var nsk *types.NoSuchKey
		if errors.As(err, &nsk) {
			return OpsFiGeneric{}, ErrKeyNotFound
		} else {
			return OpsFiGeneric{}, fmt.Errorf("get object %s/%s: %w", bucket, key, err)
		}
	}
	defer object.Body.Close()

	slog.InfoContext(ctx, "retrieved object", "bucket", bucket, "key", key)

	// todo: add gzip decompression

	var fiMessage OpsFiGeneric
	if err := json.NewDecoder(object.Body).Decode(&fiMessage); err != nil {
		return OpsFiGeneric{}, fmt.Errorf("parse object: %w", err)
	}
	return fiMessage, nil
}
