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
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"
	"yodleeops/infra"
	infrastub "yodleeops/infra/stubs"
	openapi "yodleeops/openapi/sources"
	"yodleeops/testutil"
)

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
	query := fmt.Sprintf("%s/events/taillog?profileIDs=profile1,profile2,profile3&subjects=%s,%s",
		testServer.URL+ApiUrl, openapi.FiSubjectHoldings, openapi.FiSubjectTransactions)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, query, nil)
	require.NoError(t, err)

	resp, err := http.DefaultClient.Do(req)
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

func TestHandleListFiMessages(t *testing.T) {
	// given
	awsClient := testutil.SetupAwsITest(t)

	goodApp := &App{AwsClient: awsClient}
	goodApp.AwsClient.PageLength = aws.Int32(1) // testing ListObjectsV2 pagination.

	badApp := &App{AwsClient: awsClient}
	infrastub.MakeBadS3Client(&badApp.AwsClient, infrastub.BadS3ClientCfg{
		FailListPrefix: map[infra.Bucket]string{awsClient.AcctBucket: "p1"},
	})

	testutil.SeedS3Buckets(t, awsClient)

	for _, test := range []struct {
		app            *App
		name           string
		profileIDs     string
		cursors        string
		subject        string
		wantOpsGeneric openapi.ListFiMetadataResponse
		wantErrorResp  openapi.ErrorResp
		wantStatusCode int
	}{
		{
			app:        goodApp,
			name:       "selecting without cursors",
			profileIDs: "p1,p2",
			cursors:    ",", // no cursors.
			subject:    string(openapi.FiSubjectAccounts),
			wantOpsGeneric: openapi.ListFiMetadataResponse{OpsFiMetadata: []openapi.OpsFiMetadata{
				{
					Key:               "p2/1/20/200/2025-06-14",
					ProfileID:         "p2",
					ProviderAccountID: "1",
					PartyIDTypeCd:     "20",
					AccountID:         "200",
					LastUpdated:       time.Date(2025, 6, 14, 0, 0, 0, 0, time.UTC),
				},
				{
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
			app:        goodApp,
			name:       "profileIDs length != cursors length",
			profileIDs: "p1,p2",
			cursors:    "cursor1",
			subject:    string(openapi.FiSubjectAccounts),
			wantErrorResp: openapi.ErrorResp{
				ErrorCode: openapi.ErrorCodePROFILEIDCURSORLENGTH,
			},
			wantStatusCode: http.StatusBadRequest,
		},
		{
			app:        badApp,
			name:       "failed to list fi metadata",
			profileIDs: "p1,p2",
			cursors:    ",",
			subject:    string(openapi.FiSubjectAccounts),
			wantErrorResp: openapi.ErrorResp{
				ErrorCode: openapi.ErrorCodeFATALERROR,
			},
			wantStatusCode: http.StatusInternalServerError,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			// when
			r := httptest.NewRequest(http.MethodGet,
				fmt.Sprintf("%s/fimetadata?profileIDs=%s&cursors=%s&subject=%s",
					ApiUrl,
					url.QueryEscape(test.profileIDs),
					url.QueryEscape(test.cursors),
					url.QueryEscape(test.subject),
				),
				nil)
			w := httptest.NewRecorder()

			hander := MakeRoot(test.app, "")
			hander.ServeHTTP(w, r)

			// then
			assert.Equal(t, test.wantStatusCode, w.Code)
			if test.wantStatusCode == http.StatusOK {
				AssertRespBody(t, test.wantOpsGeneric, w,
					cmpopts.IgnoreFields(openapi.OpsFiMetadata{}, "LastModified"),
					cmpopts.IgnoreFields(openapi.ListFiMetadataResponse{}, "Cursors"))
			} else {
				AssertRespBody(t, test.wantErrorResp, w, errorRespCmpOpts...)
			}
		})
	}
}

func TestHandleGetFiObject(t *testing.T) {
	// given
	testKey := "p1/1/100/3000/2025-06-12T00:14:37Z"
	testBody := fmt.Sprintf(`{"profileId":"p1","timestamp":"2025-06-12T00:15:00Z","originTopic":"%s", "data": {"key": "value"}}`, infra.TxnResponseTopic)

	awsClient := testutil.SetupAwsITest(t)

	goodApp := &App{AwsClient: awsClient}
	badApp := &App{AwsClient: awsClient}
	infrastub.MakeBadS3Client(&badApp.AwsClient, infrastub.BadS3ClientCfg{
		FailGetKey: testKey,
	})

	_, err := awsClient.S3Client.PutObject(t.Context(), &s3.PutObjectInput{
		Bucket: aws.String(string(awsClient.S3Buckets.TxnBucket)),
		Key:    aws.String(testKey),
		Body:   bytes.NewReader([]byte(testBody)),
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
			w := httptest.NewRecorder()

			hander := MakeRoot(test.app, "")
			hander.ServeHTTP(w, r)

			// then
			assert.Equal(t, test.wantStatusCode, w.Code)
			if test.wantStatusCode == http.StatusOK {
				AssertRespBody(t, test.wantOpsGeneric, w)
			} else {
				AssertRespBody(t, test.wantErrorResp, w, errorRespCmpOpts...)
			}
		})
	}
}

func AssertRespBody[V any](t *testing.T, wantBody V, w *httptest.ResponseRecorder, opts ...cmp.Option) {
	t.Helper()

	resp := w.Result()
	defer resp.Body.Close()

	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Error(err.Error())
	}

	var actualBody V
	if err = json.Unmarshal(b, &actualBody); err != nil {
		t.Errorf("failed to unmarshal response body: %s: %s", string(b), err.Error())
	}
	str := cmp.Diff(wantBody, actualBody, opts...)
	if str != "" {
		t.Error(str)
	}
}
