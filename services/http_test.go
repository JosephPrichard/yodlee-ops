package svc

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/go-faster/jx"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"
	"yodleeops/internal/infra"
	infrastub "yodleeops/internal/infra/stubs"
	"yodleeops/internal/jsonutil"
	"yodleeops/internal/testutil"
	openapi "yodleeops/openapi/sources"
)

const TestAuthorizationToken = "Bearer <TOKEN>"

func scanEvents(resp *http.Response, wantEvents int) []string {
	var events []string
	if wantEvents == 0 {
		return events
	}

	event := ""
	count := 0

	scan := bufio.NewScanner(resp.Body)
	for scan.Scan() {
		line := scan.Text()
		if line == "" {
			continue
		}
		if event == "" {
			event = line + "\n"
		} else {
			event += line + "\n"
			events = append(events, event)
			count++
			event = ""
		}
		if count >= wantEvents {
			break
		}
	}

	return events
}

func TestStreamFiObjectLogs(t *testing.T) {
	app := &App{
		FiMessageBroadcaster: &FiMessageBroadcaster{},
	}

	testServer := httptest.NewServer(MakeRoot(app, ""))
	defer testServer.Close()

	// optimistic timeout in case of a deadlock.
	ctx, cancel := context.WithTimeout(t.Context(), 1*time.Second)
	defer cancel()

	// when
	query := fmt.Sprintf("%s/events/taillog?prefix=profile1,profile2,profile3&subjects=%s,%s",
		testServer.URL+ApiUrl, openapi.FiSubjectHoldings, openapi.FiSubjectTransactions)
	r, err := http.NewRequestWithContext(ctx, http.MethodGet, query, nil)
	require.NoError(t, err)
	r.Header.Set("Authorization", TestAuthorizationToken)

	resp, err := http.DefaultClient.Do(r)
	require.NoError(t, err)
	defer resp.Body.Close()

	go func() {
		app.FiMessageBroadcaster.Broadcast("profile1", infra.HoldRefreshTopic, "event1")
		app.FiMessageBroadcaster.Broadcast("profile2", infra.HoldResponseTopic, "event2")
		app.FiMessageBroadcaster.Broadcast("profile3", infra.TxnRefreshTopic, "event3")
		app.FiMessageBroadcaster.Broadcast("profile1", infra.TxnResponseTopic, "event4")
		app.FiMessageBroadcaster.Broadcast("profile4", infra.CnctRefreshTopic, "event5")
	}()

	// then
	wantEvents := []string{
		fmt.Sprintf("event: %s\ndata: %s\n", "meta", "init"),
		fmt.Sprintf("event: %s\ndata: %s\n", "log", "event1"),
		fmt.Sprintf("event: %s\ndata: %s\n", "log", "event2"),
		fmt.Sprintf("event: %s\ndata: %s\n", "log", "event3"),
		fmt.Sprintf("event: %s\ndata: %s\n", "log", "event4"),
	}
	assert.Equal(t, wantEvents, scanEvents(resp, len(wantEvents)))
}

var errorRespCmpOpts = []cmp.Option{cmpopts.IgnoreFields(openapi.ErrorResp{}, "ErrorDesc")}
var opsGenericsCmpOpts = []cmp.Option{
	cmpopts.IgnoreFields(openapi.OpsFiMetadata{}, "LastModified"),
	cmpopts.IgnoreFields(openapi.ListFiMetadataResponse{}, "Cursor"),
}

func TestHandleListFiMessages(t *testing.T) {
	// given
	awsClient := testutil.SetupAwsITest(t)

	goodApp := &App{AwsClient: awsClient}
	goodApp.AwsClient.PageLength = aws.Int32(1) // testing ListObjectsV2 pagination.

	badAppPrefix := &App{AwsClient: awsClient}
	infrastub.MakeBadS3Client(&badAppPrefix.AwsClient, infrastub.BadS3ClientCfg{
		FailListPrefix: map[infra.Bucket]string{awsClient.AcctBucket: "p1/1/10"},
	})

	badAppProfiles := &App{AwsClient: awsClient}
	infrastub.MakeBadS3Client(&badAppProfiles.AwsClient, infrastub.BadS3ClientCfg{
		FailListPrefix: map[infra.Bucket]string{awsClient.AcctBucket: "p1"},
	})

	testutil.SeedS3Buckets(t, awsClient)

	for _, test := range []struct {
		app            *App
		name           string
		url            string
		wantOpsGeneric openapi.ListFiMetadataResponse
		wantErrorResp  openapi.ErrorResp
		wantStatusCode int
	}{
		{
			app:  goodApp,
			name: "selecting without cursor",
			url:  fmt.Sprintf("prefix?prefix=%s&subject=%s", url.QueryEscape("p1/1/10"), string(openapi.FiSubjectAccounts)),
			wantOpsGeneric: openapi.ListFiMetadataResponse{OpsFiMetadata: []openapi.OpsFiMetadata{
				{
					Subject:           "accounts",
					Key:               "p1/1/10/100/2025-06-12",
					LastModified:      time.Date(2026, 2, 26, 4, 7, 44, 0, time.UTC),
					ProfileID:         "p1",
					ProviderAccountID: "1",
					PartyIDTypeCd:     "10",
					AccountID:         "100",
					LastUpdated:       time.Date(2025, 6, 12, 0, 0, 0, 0, time.UTC),
				},
			}},
			wantStatusCode: http.StatusOK,
		},
		{
			app:  badAppPrefix,
			name: "failed to list fi metadata by prefix",
			url:  fmt.Sprintf("prefix?prefix=%s&subject=%s", url.QueryEscape("p1/1/10"), string(openapi.FiSubjectAccounts)),
			wantErrorResp: openapi.ErrorResp{
				ErrorCode: openapi.ErrorCodeFATALERROR,
			},
			wantStatusCode: http.StatusInternalServerError,
		},
		{
			app:  goodApp,
			name: "selecting without cursor",
			url:  fmt.Sprintf("profiles?profileIDs=%s&subject=%s&cursor=", url.QueryEscape("p1,p2"), string(openapi.FiSubjectAccounts)),
			wantOpsGeneric: openapi.ListFiMetadataResponse{OpsFiMetadata: []openapi.OpsFiMetadata{
				{
					Subject:           openapi.FiSubjectAccounts,
					Key:               "p2/1/20/200/2025-06-14",
					ProfileID:         "p2",
					ProviderAccountID: "1",
					PartyIDTypeCd:     "20",
					AccountID:         "200",
					LastUpdated:       time.Date(2025, 6, 14, 0, 0, 0, 0, time.UTC),
				},
				{
					Subject:           openapi.FiSubjectAccounts,
					Key:               "p1/1/10/100/2025-06-12",
					ProfileID:         "p1",
					ProviderAccountID: "1",
					PartyIDTypeCd:     "10",
					AccountID:         "100",
					LastUpdated:       time.Date(2025, 6, 12, 0, 0, 0, 0, time.UTC),
				},
			}},
			wantStatusCode: http.StatusOK,
		},
		{
			app:  goodApp,
			name: "prefix length != cursor length",
			url:  fmt.Sprintf("profiles?profileIDs=%s&subject=%s&cursor=cursor1", url.QueryEscape("p1,p2"), string(openapi.FiSubjectAccounts)),
			wantErrorResp: openapi.ErrorResp{
				ErrorCode: openapi.ErrorCodePROFILEIDCURSORLENGTH,
			},
			wantStatusCode: http.StatusBadRequest,
		},
		{
			app:  badAppProfiles,
			name: "failed to list fi metadata by profile",
			url:  fmt.Sprintf("profiles?profileIDs=%s&subject=%s&cursor=", url.QueryEscape("p1,p2"), string(openapi.FiSubjectAccounts)),
			wantErrorResp: openapi.ErrorResp{
				ErrorCode: openapi.ErrorCodeFATALERROR,
			},
			wantStatusCode: http.StatusInternalServerError,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			// when
			endpoint := fmt.Sprintf("%s/fimetadata/%s", ApiUrl, test.url)
			r := httptest.NewRequest(http.MethodGet, endpoint, nil)
			r.Header.Set("Authorization", TestAuthorizationToken)
			w := httptest.NewRecorder()

			hander := MakeRoot(test.app, "")
			hander.ServeHTTP(w, r)

			// then
			assert.Equal(t, test.wantStatusCode, w.Code)
			if test.wantStatusCode == http.StatusOK {
				testutil.AssertRespBody(t, test.wantOpsGeneric, w, opsGenericsCmpOpts...)
			} else {
				testutil.AssertRespBody(t, test.wantErrorResp, w, errorRespCmpOpts...)
			}
		})
	}
}

func TestHandleListFiMessages_Pagination(t *testing.T) {
	// given
	awsClient := testutil.SetupAwsITest(t)

	app := &App{AwsClient: awsClient}
	app.AwsClient.PageLength = aws.Int32(1) // testing ListObjectsV2 pagination.

	testutil.SeedS3Buckets(t, awsClient)

	MaxPages := 10 // hard cap to prevent infinite loops for logic errors.
	cursor := ""

	var statuses []int
	var responses []openapi.ListFiMetadataResponse

	// when
	for range MaxPages {
		r := httptest.NewRequest(http.MethodGet,
			fmt.Sprintf("%s/fimetadata/profiles?profileIDs=p1,p2&cursor=%s&subject=accounts",
				ApiUrl,
				url.QueryEscape(cursor),
			),
			nil)
		r.Header.Set("Authorization", TestAuthorizationToken)
		w := httptest.NewRecorder()

		hander := MakeRoot(app, "")
		hander.ServeHTTP(w, r)

		listFiMetadataResponse := testutil.GetRespBody[openapi.ListFiMetadataResponse](t, w)

		statuses = append(statuses, w.Code)
		responses = append(responses, listFiMetadataResponse)

		cursor = listFiMetadataResponse.Cursor
		if cursor == "" {
			// endpoint returns an empty string when there are no more pages to read.
			break
		}
	}

	// then

	// assert: only 2 pages, and each page has the data we expect.
	wantOpsGenerics := []openapi.ListFiMetadataResponse{
		{OpsFiMetadata: []openapi.OpsFiMetadata{
			{
				Subject:           openapi.FiSubjectAccounts,
				Key:               "p2/1/20/200/2025-06-14",
				ProfileID:         "p2",
				ProviderAccountID: "1",
				PartyIDTypeCd:     "20",
				AccountID:         "200",
				LastUpdated:       time.Date(2025, 6, 14, 0, 0, 0, 0, time.UTC),
			},
			{
				Subject:           openapi.FiSubjectAccounts,
				Key:               "p1/1/10/100/2025-06-12",
				ProfileID:         "p1",
				ProviderAccountID: "1",
				PartyIDTypeCd:     "10",
				AccountID:         "100",
				LastUpdated:       time.Date(2025, 6, 12, 0, 0, 0, 0, time.UTC),
			},
		}},
		{OpsFiMetadata: []openapi.OpsFiMetadata{
			{
				Subject:           openapi.FiSubjectAccounts,
				Key:               "p2/1/30/400/2025-06-15",
				ProfileID:         "p2",
				ProviderAccountID: "1",
				PartyIDTypeCd:     "30",
				AccountID:         "400",
				LastUpdated:       time.Date(2025, 6, 15, 0, 0, 0, 0, time.UTC),
			},
			{
				Subject:           openapi.FiSubjectAccounts,
				Key:               "p1/1/10/100/2025-06-13",
				ProfileID:         "p1",
				ProviderAccountID: "1",
				PartyIDTypeCd:     "10",
				AccountID:         "100",
				LastUpdated:       time.Date(2025, 6, 13, 0, 0, 0, 0, time.UTC),
			},
		}},
	}

	assert.Equal(t, []int{http.StatusOK, http.StatusOK}, statuses)
	testutil.Equal(t, wantOpsGenerics, responses, opsGenericsCmpOpts...)
}

func TestHandleGetFiObject(t *testing.T) {
	// given
	testKey := "p1/1/100/3000/2025-06-12T00:14:37Z"
	testBody := OpsFiGeneric{
		OpsFiMessage: OpsFiMessage{
			ProfileId:   "p1",
			Timestamp:   time.Date(2025, 6, 12, 0, 15, 00, 0, time.UTC),
			OriginTopic: infra.TxnResponseTopic,
		},
		Data: map[string]json.RawMessage{
			"key": json.RawMessage(`"value"`),
		},
	}

	awsClient := testutil.SetupAwsITest(t)

	goodApp := &App{AwsClient: awsClient}
	badApp := &App{AwsClient: awsClient}
	infrastub.MakeBadS3Client(&badApp.AwsClient, infrastub.BadS3ClientCfg{
		FailGetKey: testKey,
	})

	_, err := awsClient.S3Client.PutObject(t.Context(), &s3.PutObjectInput{
		Bucket: aws.String(string(awsClient.S3Buckets.TxnBucket)),
		Key:    aws.String(testKey),
		Body:   bytes.NewReader(jsonutil.MustEncodeJson(t, testBody)),
	})
	require.NoError(t, err)

	for _, test := range []struct {
		app            *App
		name           string
		key            string
		subject        string
		wantOpsGeneric openapi.FiObject
		wantErrorResp  openapi.ErrorResp
		wantStatusCode int
	}{
		{
			app:     goodApp,
			name:    "valid key",
			key:     testKey,
			subject: string(openapi.FiSubjectTransactions),
			wantOpsGeneric: openapi.FiObject{
				ProfileId:   "p1",
				Timestamp:   time.Date(2025, 6, 12, 0, 15, 00, 0, time.UTC),
				OriginTopic: string(infra.TxnResponseTopic),
				Data:        map[string]jx.Raw{"key": jx.Raw("\"value\"")},
			},
			wantStatusCode: http.StatusOK,
		},
		{
			app:            goodApp,
			name:           "invalid key",
			key:            "p1/invalid-key",
			subject:        string(openapi.FiSubjectTransactions),
			wantErrorResp:  openapi.ErrorResp{ErrorCode: openapi.ErrorCodeKEYNOTFOUND},
			wantStatusCode: http.StatusNotFound,
		},
		{
			app:            badApp,
			name:           "failed to get fi object",
			key:            testKey,
			subject:        string(openapi.FiSubjectTransactions),
			wantErrorResp:  openapi.ErrorResp{ErrorCode: openapi.ErrorCodeFATALERROR},
			wantStatusCode: http.StatusInternalServerError,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			// when
			r := httptest.NewRequest(http.MethodGet,
				fmt.Sprintf("%s/fiobject?key=%s&subject=%s",
					ApiUrl,
					url.QueryEscape(test.key),
					url.QueryEscape(test.subject),
				),
				nil)
			r.Header.Set("Authorization", TestAuthorizationToken)
			w := httptest.NewRecorder()

			hander := MakeRoot(test.app, "")
			hander.ServeHTTP(w, r)

			// then
			assert.Equal(t, test.wantStatusCode, w.Code)
			if test.wantStatusCode == http.StatusOK {
				testutil.AssertRespBody(t, test.wantOpsGeneric, w)
			} else {
				testutil.AssertRespBody(t, test.wantErrorResp, w, errorRespCmpOpts...)
			}
		})
	}
}
