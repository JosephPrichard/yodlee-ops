package svc

import (
	"fmt"
	"net/http"
	"strings"
	"time"
	"yodleeops/infra"
)

func RegisterRoutes(app *App) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("/taillog", app.HandleTailLogEvents)

	return mux
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

func (app *App) HandleTailLogEvents(w http.ResponseWriter, r *http.Request) {
	values := r.URL.Query()

	topics := strings.Split(values.Get("topics"), ",")
	if len(topics) == 0 {
		topics = DefaultTailLogTopics
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming unsupported", http.StatusInternalServerError)
		return
	}
	flusher.Flush()

	subChan := app.Subscribe(topics)

	go func() {
		// user-initiated close. stop listening.
		defer app.Unsubscribe(subChan)
		<-r.Context().Done()
	}()

	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case msg, ok := <-subChan:
			if !ok {
				// stop listening when the channel is closed (this could be a system or user-initiated event)
				return
			}
			fmt.Fprintf(w, "event: %s\ndata: %s\n\n", "log", msg)
			flusher.Flush()
		case <-ticker.C:
			fmt.Fprintf(w, "event: %s\ndata: %s\n\n", "meta", "ping")
			flusher.Flush()
		}
	}
}
