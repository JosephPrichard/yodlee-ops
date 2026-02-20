package svc

import (
	"context"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
	"yodleeops/infra"
	openapi "yodleeops/openapi/sources"
)

const BaseURL = "/yodlee-ops/api/v1"

func RootMiddleware(allowedOrigins string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		trace := r.Header.Get("X-trace")
		if trace == "" {
			trace = uuid.NewString()
		}
		r = r.WithContext(context.WithValue(r.Context(), "trace", trace))

		w.Header().Set("Access-Control-Allow-Origin", allowedOrigins)
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization, X-trace")
		w.Header().Set("Access-Control-Allow-Credentials", "true")

		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func MakeRoot(app *App, allowOrigins string) http.Handler {
	mux := http.NewServeMux()

	oGenServer, err := openapi.NewServer(&FiObjectHandler{App: app})
	if err != nil {
		panic(fmt.Sprintf("failed to make root: %v", err))
	}

	mux.Handle(BaseURL+"/taillog", RootMiddleware(allowOrigins, http.HandlerFunc(app.HandleSSELogs)))
	// a trailing slash is needed for prefix matching. strip prefix allows ogen to do exact path matching
	mux.Handle(BaseURL+"/", http.StripPrefix(BaseURL, RootMiddleware(allowOrigins, oGenServer)))

	distDir := "./frontend/dist"
	fs := http.FileServer(http.Dir(distDir))
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		// allows defaulting to index page if the file requested does not exist.
		path := filepath.Join(distDir, r.URL.Path)
		if info, err := os.Stat(path); err == nil && !info.IsDir() {
			fs.ServeHTTP(w, r)
			return
		}
		http.ServeFile(w, r, filepath.Join(distDir, "index.html"))
	})

	return mux
}

func stripQuery(elements []string) []string {
	for i, element := range elements {
		if element == "" {
			elements = append(elements[:i], elements[i+1:]...)
		}
	}
	return elements
}

const (
	ConnectionsInput  = "connections"
	AccountsInput     = "accounts"
	TransactionsInput = "transactions"
	HoldingsInput     = "holdings"
)

func FFormatEvent(w io.Writer, topic string, msg string) {
	fmt.Fprintf(w, "event: %s\ndata: %s\n\n", topic, msg)
}

func MakeTopicSubscription(topicsInput []string) []string {
	var topics []string
	for _, topicInput := range topicsInput {
		switch strings.ToLower(topicInput) {
		case ConnectionsInput:
			topics = append(topics, infra.CnctRefreshTopic, infra.CnctResponseTopic)
		case AccountsInput:
			topics = append(topics, infra.AcctRefreshTopic, infra.AcctResponseTopic)
		case TransactionsInput:
			topics = append(topics, infra.TxnRefreshTopic, infra.TxnResponseTopic)
		case HoldingsInput:
			topics = append(topics, infra.HoldRefreshTopic, infra.HoldResponseTopic)
		}
	}
	return topics
}

func (app *App) HandleSSELogs(w http.ResponseWriter, r *http.Request) {
	values := r.URL.Query()
	profileIDs := stripQuery(strings.Split(values.Get("profileIDs"), ","))
	topicsInput := stripQuery(strings.Split(values.Get("topics"), ","))

	topics := MakeTopicSubscription(topicsInput)

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	slog.InfoContext(r.Context(), "begin log tail streaming", "topicsInput", topicsInput, "topics", topics, "profileIDs", profileIDs)

	f, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming unsupported", http.StatusInternalServerError)
		return
	}

	FFormatEvent(w, "meta", "init")
	f.Flush()

	subChan := app.FiMessageBroadcaster.Subscribe(SubscriberFilter{Topics: topics, ProfileIDs: profileIDs})

	go func() {
		defer app.FiMessageBroadcaster.Unsubscribe(subChan)
		<-r.Context().Done()
	}()

	ticker := time.NewTicker(15 * time.Second)
	for {
		select {
		case msg, ok := <-subChan:
			if !ok {
				return
			}
			FFormatEvent(w, "log", msg)
		case <-ticker.C:
			FFormatEvent(w, "meta", "ping")
		}
		f.Flush()
	}
}

func BucketFromSubject(buckets infra.S3Buckets, subject openapi.FiSubject) string {
	var bucket string
	switch subject {
	case openapi.FiSubjectConnections:
		bucket = buckets.CnctBucket
	case openapi.FiSubjectAccounts:
		bucket = buckets.AcctBucket
	case openapi.FiSubjectTransactions:
		bucket = buckets.TxnBucket
	case openapi.FiSubjectHoldings:
		bucket = buckets.HoldBucket
	default:
		bucket = buckets.CnctBucket
	}
	return bucket
}

type FiObjectHandler struct {
	*App
}

func (h *FiObjectHandler) NewError(ctx context.Context, err error) *openapi.ErrorRespStatusCode {
	slog.ErrorContext(ctx, "error handling request", "err", err)

	return &openapi.ErrorRespStatusCode{
		StatusCode: http.StatusInternalServerError,
		Response: openapi.ErrorResp{
			ErrorCode: openapi.ErrorCodeFATALERROR,
			ErrorDesc: "internal server error",
		},
	}
}

var _ openapi.Handler = (*FiObjectHandler)(nil)

func (h *FiObjectHandler) GetFiMetadata(ctx context.Context, params openapi.GetFiMetadataParams) (openapi.GetFiMetadataRes, error) {
	appCtx := AppContext{Context: ctx, App: h.App}

	profileIDsInput := strings.Split(params.ProfileIDs, ",")
	cursorsInput := strings.Split(params.Cursors, ",")
	bucketInput := BucketFromSubject(h.S3Buckets, params.Subject.Value)

	if len(profileIDsInput) != len(cursorsInput) {
		return &openapi.ErrorResp{
			ErrorCode: openapi.ErrorCodePROFILEIDCURSORLENGTH,
			ErrorDesc: "profileIDs and cursors must be the same length",
		}, nil
	}
	var pairs []ProfileIDCursorPair
	for i, profileID := range profileIDsInput {
		pairs = append(pairs, ProfileIDCursorPair{ProfileID: profileID, Cursor: cursorsInput[i]})
	}

	fiMetadataResults, err := ListFiMetadataByProfileIDs(appCtx, bucketInput, pairs)
	if err != nil {
		return nil, fmt.Errorf("list objects by profile IDs: %w", err)
	}

	opsFiMetadata := make([]openapi.OpsFiMetadata, 0)
	for _, metadata := range fiMetadataResults.OpsFiMetadata {
		opsFiMetadata = append(opsFiMetadata, openapi.OpsFiMetadata(metadata))
	}
	cursors := fiMetadataResults.Cursors

	return &openapi.ListFiMetadataResponse{
		OpsFiMetadata: opsFiMetadata,
		Cursors:       cursors,
	}, nil
}

func (h *FiObjectHandler) GetFiObject(ctx context.Context, params openapi.GetFiObjectParams) (openapi.GetFiObjectRes, error) {
	appCtx := AppContext{Context: ctx, App: h.App}

	keyInput := params.Key
	bucketInput := BucketFromSubject(h.S3Buckets, params.Subject.Value)

	opsFiObject, err := GetFiObject(appCtx, bucketInput, keyInput)
	if errors.Is(err, ErrKeyNotFound) {
		return &openapi.ErrorResp{
			ErrorCode: openapi.ErrorCodeKEYNOTFOUND,
			ErrorDesc: err.Error(),
		}, nil
	} else if err != nil {
		return nil, fmt.Errorf("retrieve fi object: %w", err)
	}
	slog.InfoContext(appCtx, "retrieved fi message object", "key", keyInput, "bucket", bucketInput)

	return &openapi.GetFiObjectOK{
		ProfileId:   opsFiObject.ProfileId,
		Timestamp:   opsFiObject.Timestamp,
		OriginTopic: opsFiObject.OriginTopic,
		Data:        opsFiObject.Data,
	}, nil
}
