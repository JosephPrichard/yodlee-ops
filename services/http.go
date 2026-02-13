package svc

import (
	"context"
	"fmt"
	"github.com/a-h/templ"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"
	"yodleeops/infra"
	"yodleeops/templates"
)

var BasePathApi = "/yodlee-ops/servicing/v1"

func MakeRoot(app *App) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc(fmt.Sprintf("%s/taillog", BasePathApi), app.HandleTailLogSSE)
	mux.HandleFunc("/taillog", HandleTailLogPage)

	return mux
}

func parseTailLogQuery(r *http.Request) ([]string, []string) {
	values := r.URL.Query()
	profileIds := strings.Split(values.Get("profileIds"), ",")
	topics := strings.Split(values.Get("topics"), ",")
	return profileIds, topics
}

func HandleTailLogPage(w http.ResponseWriter, r *http.Request) {
	profileIds, topics := parseTailLogQuery(r)

	ctx := context.WithValue(r.Context(), "trace", r.Header.Get("trace"))

	slog.InfoContext(ctx, "rendering tail log page", "topics", topics, "profileIds", profileIds)

	component := templates.TailLog(profileIds, topics)
	templ.Handler(component).ServeHTTP(w, r)
}

var DefaultTailLogTopics = []string{
	infra.CnctResponseTopic,
	infra.AcctResponseTopic,
	infra.TxnResponseTopic,
	infra.HoldResponseTopic,
	infra.CnctRefreshTopic,
	infra.AcctRefreshTopic,
	infra.TxnRefreshTopic,
	infra.HoldRefreshTopic,
}

func FFormatEvent(w io.Writer, topic string, msg string) {
	fmt.Fprintf(w, "event: %s\ndata: %s\n\n", topic, msg)
}

func (app *App) HandleTailLogSSE(w http.ResponseWriter, r *http.Request) {
	profileIds, topics := parseTailLogQuery(r)
	if len(topics) == 0 {
		topics = DefaultTailLogTopics
	}

	ctx := context.WithValue(r.Context(), "trace", r.Header.Get("trace"))

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	slog.InfoContext(ctx, "tailing log", "topics", topics, "profileIds", profileIds)

	f, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming unsupported", http.StatusInternalServerError)
		return
	}

	FFormatEvent(w, "meta", "init")
	f.Flush()

	subChan := app.Subscribe(SubscriberFilter{Topics: topics, ProfileIDs: profileIds})

	go func() {
		defer app.Unsubscribe(subChan)
		<-r.Context().Done()
	}()

	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case msg, ok := <-subChan:
			slog.InfoContext(ctx, "received message", "msg", msg)
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
