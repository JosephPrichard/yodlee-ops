package svc

import (
	"fmt"
	"net/http"
	"time"
)

func (app *App) HandleTailLogEvents(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming unsupported", http.StatusInternalServerError)
		return
	}

	subChan := app.Subscribe()

	go func() {
		// client initiated close. stop listening.
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
