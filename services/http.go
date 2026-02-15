package svc

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/a-h/templ"
	"github.com/google/uuid"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"
	"yodleeops/infra"
	"yodleeops/templates"
)

func MakeRoot(app *App) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("/taillog", app.HandleSSELogs)
	mux.HandleFunc("/", HandleIndexPage)
	mux.HandleFunc("/admin", HandleAdminPage)
	mux.HandleFunc("/logs", HandleLogsPage)
	mux.Handle("/", http.FileServer(http.Dir("./static")))

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

func HandleIndexPage(w http.ResponseWriter, r *http.Request) {

}

func HandleAdminPage(w http.ResponseWriter, r *http.Request) {

}

func HandleLogsPage(w http.ResponseWriter, r *http.Request) {
	profileIds, topics := parseTailLogQuery(r)

	if len(topics) == 0 {
		topics = DefautSubscribeTopics
	}

	slog.InfoContext(r.Context(), "rendering tail log page", "topics", topics, "profileIds", profileIds)

	component := templates.LogPage(templates.LogsPageArgs{
		ProfileIDs:        profileIds,
		Topics:            topics,
		InputTopicOptions: DefautSubscribeTopics,
	})
	templ.Handler(component).ServeHTTP(w, r)
}

const (
	ConnectionsInput  = "connections"
	AccountsInput     = "accounts"
	TransactionsInput = "transactions"
	HoldingsInput     = "holdings"
)

var DefautSubscribeTopics = []string{
	ConnectionsInput,
	AccountsInput,
	TransactionsInput,
	HoldingsInput,
}

func FFormatEvent(w io.Writer, topic string, msg string) {
	fmt.Fprintf(w, "event: %s\ndata: %s\n\n", topic, msg)
}

func RenderHTMLBroadcast(brdJson string) (string, error) {
	var html bytes.Buffer

	var broadcast struct {
		OpsFiMessage
		Data map[string]any `json:"data"`
	}
	if err := json.Unmarshal([]byte(brdJson), &broadcast); err != nil {
		return "", fmt.Errorf("failed to unmarshal broadcast message: %w", err)
	}
	rawData, err := json.Marshal(broadcast.Data)
	if err != nil {
		return "", fmt.Errorf("failed to marshal broadcast message data: %w", err)
	}

	component := templates.LogMsg(templates.LogArgs{
		ID:        uuid.NewString(),
		Topic:     broadcast.OriginTopic,
		Timestamp: broadcast.Timestamp.Format(time.RFC1123),
		Json:      string(rawData),
		JsonTree:  broadcast.Data,
	})
	if err := component.Render(context.Background(), &html); err != nil {
		return "", fmt.Errorf("failed to render log message: %w", err)
	}

	return html.String(), nil
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
	accept := r.Header.Get("Accept")

	var renderFn func(string) (string, error)
	switch accept {
	case "application/json", "text/plain":
		renderFn = func(json string) (string, error) { return json, nil }
	default:
		renderFn = RenderHTMLBroadcast
	}

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
			if !ok {
				return
			}
			str, err := renderFn(msg)
			if err != nil {
				slog.Error("failed to render log message", "err", err)
				continue
			}
			FFormatEvent(w, "log", str)
		case <-ticker.C:
			FFormatEvent(w, "meta", "ping")
		}
		f.Flush()
	}
}
