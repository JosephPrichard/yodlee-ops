package svc

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
	"yodleeops/infra"
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

func TestHandleTailLogSSE(t *testing.T) {
	app := &App{
		FiMessageBroadcaster: &FiMessageBroadcaster{},
	}

	testServer := httptest.NewServer(MakeRoot(app, ""))
	defer testServer.Close()

	// optimistic timeout in case of a deadlock.
	ctx, cancel := context.WithTimeout(t.Context(), 1*time.Second)
	defer cancel()

	// when
	query := fmt.Sprintf("%s/taillog?profileIDs=profile1,profile2,profile3&topics=%s,%s", BaseURL+testServer.URL, HoldingsInput, TransactionsInput)
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

func setupS3ApiTest(t *testing.T) *App {
	awsClient := testutil.SetupAwsITest(t)

	app := &App{AwsClient: awsClient}
	app.AwsClient.PageLength = aws.Int32(1) // testing ListObjectsV2 pagination.

	testutil.SeedS3Buckets(t, app.AwsClient)
	return app
}

var errorRespCmpOpts = []cmp.Option{cmpopts.IgnoreFields(ErrorResp{}, "ErrorDesc")}

func TestHandleListFiMessages(t *testing.T) {
	// given
	app := setupS3ApiTest(t)

	for _, test := range []struct {
		name           string
		profileIDs     string
		cursors        string
		subject        string
		wantOpsGeneric ListFiMetadataResult
		wantErrorResp  ErrorResp
		wantStatusCode int
	}{
		{
			name:       "selecting without cursors",
			profileIDs: "p1,p2",
			cursors:    ",", // no cursors.
			subject:    AccountsInput,
			wantOpsGeneric: ListFiMetadataResult{OpsFiMetadata: []OpsFiMetadata{
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
			name:       "profileIDs length != cursors length",
			profileIDs: "p1,p2",
			cursors:    "cursor1",
			subject:    AccountsInput,
			wantErrorResp: ErrorResp{
				ErrorCode: ErrHttpProfileIDsCursorLength.Error(),
			},
			wantStatusCode: http.StatusBadRequest,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			// when
			r := httptest.NewRequest(http.MethodGet,
				fmt.Sprintf("%s/fimetadata?profileIDs=%s&cursors=%s&subject=%s", BaseURL, test.profileIDs, test.cursors, test.subject), nil)
			w := httptest.NewRecorder()

			hander := MakeRoot(app, "")
			hander.ServeHTTP(w, r)

			// then
			assert.Equal(t, test.wantStatusCode, w.Code)
			if test.wantStatusCode == http.StatusOK {
				AssertRespBody(t, test.wantOpsGeneric, w,
					cmpopts.IgnoreFields(OpsFiMetadata{}, "LastModified"), cmpopts.IgnoreFields(ListFiMetadataResult{}, "Cursors"))
			} else {
				AssertRespBody(t, test.wantErrorResp, w, errorRespCmpOpts...)
			}
		})
	}
}

func TestHandleGetFiMessage(t *testing.T) {
	// given
	awsClient := testutil.SetupAwsITest(t)
	app := &App{AwsClient: awsClient}

	inputKey := "p1/1/100/3000/2025-06-12T00:14:37Z"
	inputBody := fmt.Sprintf(`{"profileId":"p1","timestamp":"2025-06-12T00:15:00Z","originTopic":"%s"}`, infra.TxnResponseTopic)

	_, err := app.S3Client.PutObject(t.Context(), &s3.PutObjectInput{
		Bucket: aws.String(app.AwsClient.TxnBucket),
		Key:    aws.String(inputKey),
		Body:   bytes.NewReader([]byte(inputBody)),
	})
	require.NoError(t, err)

	for _, test := range []struct {
		name           string
		key            string
		subject        string
		wantOpsGeneric OpsFiGeneric
		wantErrorResp  ErrorResp
		wantStatusCode int
	}{
		{
			name:    "valid key",
			key:     inputKey,
			subject: TransactionsInput,
			wantOpsGeneric: OpsFiGeneric{
				OpsFiMessage: OpsFiMessage{
					ProfileId:   "p1",
					Timestamp:   time.Date(2025, 6, 12, 0, 15, 00, 0, time.UTC),
					OriginTopic: infra.TxnResponseTopic,
				},
				Data: json.RawMessage("null"),
			},
			wantStatusCode: http.StatusOK,
		},
		{
			name:           "invalid key",
			key:            "p1/invalid-key",
			subject:        TransactionsInput,
			wantErrorResp:  ErrorResp{ErrorCode: ErrHttpKeyNotFound.Error()},
			wantStatusCode: http.StatusNotFound,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			// when
			r := httptest.NewRequest(http.MethodGet,
				fmt.Sprintf("%s/fiobject?key=%s&subject=%s", BaseURL, test.key, test.subject), nil)
			w := httptest.NewRecorder()

			hander := MakeRoot(app, "")
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
		t.Error(err.Error())
	}
	str := cmp.Diff(wantBody, actualBody, opts...)
	if str != "" {
		t.Error(str)
	}
}
