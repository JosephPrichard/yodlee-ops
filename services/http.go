package svc

import (
	"context"
	"encoding/json"
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
)

func RouteMiddleware(allowedOrigins string) func(handlerFunc http.HandlerFunc) http.HandlerFunc {
	return func(next http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
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
		}
	}
}

func writeJSON(w http.ResponseWriter, v any) {
	b, err := json.Marshal(v)
	if err != nil {
		slog.Error("failed to marshal response", "err", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}
	if _, err := w.Write(b); err != nil {
		slog.Error("failed to marshal response", "err", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
	}
}

type ErrorResp struct {
	ErrorCode string `json:"errorcode"`
	ErrorDesc string `json:"errorDesc"`
}

var ErrHttpInternalServer = errors.New("INTERNAL_SERVER_ERROR")
var ErrHttpProfileIDsCursorLength = errors.New("PROFILE_IDS_AND_CURSORS_LENGTH_MISMATCH")
var ErrHttpKeyNotFound = errors.New("KEY_NOT_FOUND")

func errorHandler(handler func(w http.ResponseWriter, r *http.Request) error) http.HandlerFunc {
	writeErrorMsg := func(w http.ResponseWriter, resp ErrorResp, status int) {
		b, err := json.Marshal(resp)
		if err != nil {
			slog.Error("failed to marshal error response", "err", err)
			http.Error(w, "internal server error", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(status)
		w.Header().Set("Content-Type", "application/json")
		if _, err := w.Write(b); err != nil {
			slog.Error("failed to marshal error response", "err", err)
			http.Error(w, "internal server error", http.StatusInternalServerError)
			return
		}
	}

	return func(w http.ResponseWriter, r *http.Request) {
		if err := handler(w, r); err != nil {
			slog.ErrorContext(r.Context(), "failed to handle request", "err", err)

			var errorCode error
			var statusCode int
			var errorDesc string

			switch {
			case errors.Is(err, ErrKeyNotFound):
				errorCode = ErrHttpKeyNotFound
				statusCode = http.StatusNotFound
				errorDesc = err.Error()
			case errorsAsType[ProfileIDsCursorLengthError](err):
				errorCode = ErrHttpProfileIDsCursorLength
				statusCode = http.StatusBadRequest
				errorDesc = err.Error()
			default:
				errorCode = ErrHttpInternalServer
				statusCode = http.StatusInternalServerError
				errorDesc = "internal server error"
			}

			writeErrorMsg(w, ErrorResp{ErrorCode: errorCode.Error(), ErrorDesc: errorDesc}, statusCode)
		}
	}
}

const BaseURL = "/yodlee-ops/api/v1"

func MakeRoot(app *App, allowOrigins string) http.Handler {
	mux := http.NewServeMux()

	routeMiddleware := RouteMiddleware(allowOrigins)

	mux.Handle(BaseURL+"/taillog", routeMiddleware(app.HandleSSELogs))
	mux.Handle(BaseURL+"/fimetadata", routeMiddleware(errorHandler(app.HandleListFiMetadata)))
	mux.Handle(BaseURL+"/fiobject", routeMiddleware(errorHandler(app.HandleGetFiObject)))

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

func (app *App) InputCodeToBucket(inputCode string) string {
	switch inputCode {
	case ConnectionsInput:
		return app.CnctBucket
	case AccountsInput:
		return app.AcctBucket
	case TransactionsInput:
		return app.TxnBucket
	case HoldingsInput:
		return app.HoldBucket
	default:
		return app.CnctBucket
	}
}

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

type ProfileIDsCursorLengthError struct {
	ProfileIDsLength int
	CursorsLength    int
}

func (e ProfileIDsCursorLengthError) Error() string {
	return fmt.Sprintf("profileIDs and cursors must have the same length, got %d and %d", e.ProfileIDsLength, e.CursorsLength)
}

func (app *App) HandleListFiMetadata(w http.ResponseWriter, r *http.Request) error {
	appCtx := AppContext{Context: r.Context(), App: app}

	values := r.URL.Query()
	profileIDs := strings.Split(values.Get("profileIDs"), ",")
	cursors := strings.Split(values.Get("cursors"), ",")
	bucket := app.InputCodeToBucket(values.Get("subject"))

	if len(profileIDs) != len(cursors) {
		return ProfileIDsCursorLengthError{ProfileIDsLength: len(profileIDs), CursorsLength: len(cursors)}
	}
	var pairs []ProfileIDCursorPair
	for i, profileID := range profileIDs {
		pairs = append(pairs, ProfileIDCursorPair{ProfileID: profileID, Cursor: cursors[i]})
	}

	result, err := ListFiMetadataByProfileIDs(appCtx, bucket, pairs)
	if err != nil {
		return fmt.Errorf("list objects by profile IDs: %w", err)
	}
	writeJSON(w, result)
	return nil
}

func (app *App) HandleGetFiObject(w http.ResponseWriter, r *http.Request) error {
	appCtx := AppContext{Context: r.Context(), App: app}

	values := r.URL.Query()
	key := values.Get("key")
	bucket := app.InputCodeToBucket(values.Get("subject"))

	opsFiObject, err := GetFiObject(appCtx, bucket, key)
	if err != nil {
		return fmt.Errorf("get fi message object: %w", err)
	}
	slog.InfoContext(appCtx, "retrieved fi message object", "key", key)

	writeJSON(w, opsFiObject)
	return nil
}
