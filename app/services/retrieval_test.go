package svc

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"
	"yodleeops/storage"
	"yodleeops/testutil"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupRetrievalTest(t *testing.T) *State {
	s3Client := testutil.NewVirtualS3Client(t)

	state := &State{S3Client: s3Client}
	state.S3Client.PaginationLen = aws.Int32(1) // testing ListObjectsV2 pagination.

	return state
}

var opsFiMetadataCmpOpts = []cmp.Option{
	cmpopts.IgnoreFields(OpsFiMetadata{}, "LastUpdated", "LastModified"),
	cmpopts.IgnoreFields(ListFiMetadataResult{}, "Cursor"),
}

func TestGetListFiMetadataByProfileIDs(t *testing.T) {
	state := setupRetrievalTest(t)

	for _, test := range []struct {
		name           string
		queries        []ListFiMetadataQuery
		bucket         storage.Bucket
		wantOpsGeneric []OpsFiMetadata
	}{
		{
			name:    "selecting without cursor",
			queries: []ListFiMetadataQuery{{ProfileID: "p1/1/10"}},
			bucket:  state.S3Client.AcctBucket,
			wantOpsGeneric: []OpsFiMetadata{
				{
					Key:               "p1/1/10/100/2025-06-12",
					LastModified:      time.Date(2026, 2, 26, 4, 7, 44, 0, time.UTC),
					ProfileID:         "p1",
					ProviderAccountID: "1",
					PartyIDTypeCd:     "10",
					AccountID:         "100",
				},
			},
		},
		{
			name:    "selecting without cursor",
			queries: []ListFiMetadataQuery{{ProfileID: "p1"}, {ProfileID: "p2"}},
			bucket:  state.S3Client.AcctBucket,
			wantOpsGeneric: []OpsFiMetadata{
				{
					Key:               "p2/1/20/200/2025-06-14",
					ProfileID:         "p2",
					ProviderAccountID: "1",
					PartyIDTypeCd:     "20",
					AccountID:         "200",
				},
				{
					Key:               "p1/1/10/100/2025-06-12",
					ProfileID:         "p1",
					ProviderAccountID: "1",
					PartyIDTypeCd:     "10",
					AccountID:         "100",
				},
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := Context{Context: t.Context(), State: state}

			result, err := ListFiMetadataByProfileIDs(ctx, test.bucket, test.queries)
			require.NoError(t, err)

			testutil.Equal(t, test.wantOpsGeneric, result.OpsFiMetadata, opsFiMetadataCmpOpts...)
			assert.NotEmpty(t, result.Cursor)
		})
	}
}

func TestListFiMetadataByProfileIDs_Pagination(t *testing.T) {
	s3Client := testutil.NewVirtualS3Client(t)

	state := &State{S3Client: s3Client}
	state.S3Client.PaginationLen = aws.Int32(1) // testing ListObjectsV2 pagination.

	ctx := Context{Context: t.Context(), State: state}

	const MaxPages = 10 // hard cap to prevent infinite loops for logic errors.
	cursor := []string{"", ""}

	var results []ListFiMetadataResult

	for range MaxPages {
		queries := []ListFiMetadataQuery{{ProfileID: "p1", ContinuationToken: cursor[0]}, {ProfileID: "p2", ContinuationToken: cursor[1]}}

		result, err := ListFiMetadataByProfileIDs(ctx, s3Client.AcctBucket, queries)
		require.NoError(t, err)

		results = append(results, result)

		cursor = result.Cursor
		if len(cursor) == 0 {
			break
		}
	}

	// assert: only 2 pages, and each page has the data we expect.
	wantOpsGenerics := []ListFiMetadataResult{
		{OpsFiMetadata: []OpsFiMetadata{
			{
				Key:               "p2/1/20/200/2025-06-14",
				ProfileID:         "p2",
				ProviderAccountID: "1",
				PartyIDTypeCd:     "20",
				AccountID:         "200",
			},
			{
				Key:               "p1/1/10/100/2025-06-12",
				ProfileID:         "p1",
				ProviderAccountID: "1",
				PartyIDTypeCd:     "10",
				AccountID:         "100",
			},
		}},
		{OpsFiMetadata: []OpsFiMetadata{
			{
				Key:               "p2/1/30/400/2025-06-15",
				ProfileID:         "p2",
				ProviderAccountID: "1",
				PartyIDTypeCd:     "30",
				AccountID:         "400",
			},
			{
				Key:               "p1/1/10/100/2025-06-13",
				ProfileID:         "p1",
				ProviderAccountID: "1",
				PartyIDTypeCd:     "10",
				AccountID:         "100",
			},
		}},
	}
	testutil.Equal(t, wantOpsGenerics, results, opsFiMetadataCmpOpts...)
}

func TestGetFiObject(t *testing.T) {
	testKey := "p1/1/100/3000/2025-06-12T00:14:37Z"
	testBody := OpsFiGeneric{
		OpsFiMessage: OpsFiMessage{
			ProfileId:   "p1",
			Timestamp:   time.Date(2025, 6, 12, 0, 15, 00, 0, time.UTC),
			OriginTopic: storage.TxnResponseTopic,
		},
		Data: map[string]json.RawMessage{
			"key": json.RawMessage(`"value"`),
		},
	}

	s3Client := testutil.NewVirtualS3Client(t)

	state := &State{S3Client: s3Client}
	state.S3Client.PaginationLen = aws.Int32(1) // testing ListObjectsV2 pagination.

	ctx := Context{Context: t.Context(), State: state}

	_, err := s3Client.PutObject(t.Context(), &s3.PutObjectInput{
		Bucket: s3Client.TxnBucket.String(),
		Key:    aws.String(testKey),
		Body:   bytes.NewReader(MustEncodeJson(t, testBody)),
	})
	require.NoError(t, err)

	object, err := GetFiObject(ctx, state.S3Client.TxnBucket, testKey)
	require.NoError(t, err)

	wantOpsFiGeneric := OpsFiGeneric{
		OpsFiMessage: OpsFiMessage{
			ProfileId:   "p1",
			Timestamp:   time.Date(2025, 6, 12, 0, 15, 00, 0, time.UTC),
			OriginTopic: storage.TxnResponseTopic,
		},
		Data: map[string]json.RawMessage{
			"key": json.RawMessage(`"value"`),
		},
	}
	testutil.Equal(t, wantOpsFiGeneric, object)
}
