package svc

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"
	"yodleeops/infra"
)

func RouteMiddleware(allowedOrigins string) func(handlerFunc http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
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
}

func MakeRoot(app *App, allowOrigins string) http.Handler {
	mux := http.NewServeMux()

	routeMiddleware := RouteMiddleware(allowOrigins)

	mux.Handle("/taillog", routeMiddleware(http.HandlerFunc(app.HandleSSELogs)))

	return mux
}

func parseTailLogQuery(r *http.Request) ([]string, []string) {
	values := r.URL.Query()

	filter := func(elements []string) []string {
		for i, element := range elements {
			if element == "" {
				elements = append(elements[:i], elements[i+1:]...)
			}
		}
		return elements
	}

	profileIds := strings.Split(values.Get("profileIDs"), ",")
	topics := strings.Split(values.Get("topics"), ",")

	return filter(profileIds), filter(topics)
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
	profileIds, topicsInput := parseTailLogQuery(r)

	topics := MakeTopicSubscription(topicsInput)

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	slog.InfoContext(r.Context(), "begin log tail streaming", "topicsInput", topicsInput, "topics", topics, "profileIds", profileIds)

	f, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming unsupported", http.StatusInternalServerError)
		return
	}

	FFormatEvent(w, "meta", "init")
	f.Flush()

	subChan := app.FiMessageBroadcaster.Subscribe(SubscriberFilter{Topics: topics, ProfileIDs: profileIds})

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
