package svc

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"golang.org/x/sync/errgroup"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"yodleeops/internal/infra"
	"yodleeops/internal/jsonutil"
)

type OpsFiGeneric struct {
	OpsFiMessage
	Data map[string]json.RawMessage `json:"data"` // ensures that `data` is a JSON object.
}

type ListFiMetadataQuery struct {
	ProfileID         string
	ContinuationToken string
}

func FlattenNestedOpsFiMetadata(nestedOpsFiMetadata [][]OpsFiMetadata) []OpsFiMetadata {
	opsFiMetadata := make([]OpsFiMetadata, 0)
	for _, nested := range nestedOpsFiMetadata {
		opsFiMetadata = append(opsFiMetadata, nested...)
	}
	slices.SortFunc(opsFiMetadata, func(left, right OpsFiMetadata) int {
		return right.LastUpdated.Compare(left.LastUpdated)
	})
	return opsFiMetadata
}

const NilContinuationToken = ""

func MakeReturnCursor(profileIDContinuationTokenMap map[string]string, queries []ListFiMetadataQuery) string {
	cursorArray := make([]string, 0, len(queries))
	for _, pair := range queries {
		continuationToken := profileIDContinuationTokenMap[pair.ProfileID]
		cursorArray = append(cursorArray, continuationToken)
	}
	isAllContinuationTokenNil := true
	for _, continuationToken := range cursorArray {
		if continuationToken != NilContinuationToken {
			isAllContinuationTokenNil = false
		}
	}
	if isAllContinuationTokenNil {
		return "" // no more continuation tokens means = return empty cursor
	}
	return strings.Join(cursorArray, ",")
}

func MakeFirstCursor(profileIDCount int) []string {
	var cursorsInput []string
	for range profileIDCount {
		cursorsInput = append(cursorsInput, "")
	}
	return cursorsInput
}

type ListFiMetadataResult struct {
	OpsFiMetadata []OpsFiMetadata `json:"opsFiMetadata"`
	Cursor        string          `json:"cursor"`
}

func ListFiMetadataByProfileIDs(appCtx AppContext, bucket infra.Bucket, queries []ListFiMetadataQuery) (results ListFiMetadataResult, err error) {
	nestedOpsFiMetadata := make([][]OpsFiMetadata, len(queries))

	var cursorsLock sync.Mutex
	cursors := make(map[string]string, len(queries))

	eg, egCtx := errgroup.WithContext(appCtx)
	appEgCtx := AppContext{Context: egCtx, App: appCtx.App}
	for i, pair := range queries {
		eg.Go(func() error {
			opsFiMetadata, nextCursor, err := ListFiMetadataByProfileID(appEgCtx, bucket, pair.ProfileID, pair.ContinuationToken)
			if err != nil {
				return fmt.Errorf("list metadata job index=%d: %w", i, err)
			}
			nestedOpsFiMetadata[i] = opsFiMetadata

			cursorsLock.Lock()
			cursors[pair.ProfileID] = nextCursor
			cursorsLock.Unlock()

			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return results, err
	}

	opsFiMetadata := FlattenNestedOpsFiMetadata(nestedOpsFiMetadata)
	cursor := MakeReturnCursor(cursors, queries)

	slog.InfoContext(appCtx, "retrieved metadata records", "bucket", bucket, "queries", queries, "opsFiMetadata", opsFiMetadata, "cursor", cursor)

	return ListFiMetadataResult{OpsFiMetadata: opsFiMetadata, Cursor: cursor}, nil
}

func ListFiMetadataByProfileID(ctx AppContext, bucket infra.Bucket, profileID string, cursor string) ([]OpsFiMetadata, string, error) {
	var continuationToken *string
	if cursor != "" {
		continuationToken = aws.String(cursor)
	}
	slog.InfoContext(ctx, "listing metadata records", "bucket", bucket, "profileID", profileID, "continuationToken", continuationToken)

	output, err := ctx.S3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket:            aws.String(string(bucket)),
		Prefix:            aws.String(profileID),
		ContinuationToken: continuationToken,
		MaxKeys:           ctx.AwsClient.PageLength,
	})
	if err != nil {
		return nil, NilContinuationToken, fmt.Errorf("list objects by profileID=%s, cursor=%s and bucket=%s: %w", profileID, cursor, bucket, err)
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
	nextCursor := NilContinuationToken
	if hasMore && output.NextContinuationToken != nil {
		nextCursor = *output.NextContinuationToken
	}

	slog.InfoContext(ctx, "retrieved metadata records", "bucket", bucket, "profileID", profileID, "opsFiMetadata", opsFiMetadata, "nextCursor", nextCursor)
	return opsFiMetadata, nextCursor, nil
}

var ErrKeyNotFound = errors.New("key not found")

func GetFiObject(ctx AppContext, bucket infra.Bucket, key string) (OpsFiGeneric, error) {
	object, err := ctx.S3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(string(bucket)),
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

	var fiObject OpsFiGeneric
	if err := jsonutil.DecodeGzipJSON(object.Body, &fiObject); err != nil {
		return OpsFiGeneric{}, fmt.Errorf("decode gzip json: %w", err)
	}
	return fiObject, nil
}
