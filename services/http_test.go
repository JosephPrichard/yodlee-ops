package svc

import (
	"bufio"
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
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

	testServer := httptest.NewServer(MakeRoot(app))
	defer testServer.Close()

	// optimistic timeout in case of a deadlock.
	ctx, cancel := context.WithTimeout(t.Context(), 1*time.Second)
	defer cancel()

	// when
	query := fmt.Sprintf("%s/taillog?profileIds=profile1,profile2,profile3&topics=topic1,topic2", testServer.URL+BasePathApi)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, query, nil)
	require.NoError(t, err)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	go func() {
		app.Broadcast("profile1", "topic2", "event1")
		app.Broadcast("profile2", "topic2", "event2")
		app.Broadcast("profile3", "topic1", "event3")
		app.Broadcast("profile1", "topic1", "event4")
		app.Broadcast("profile4", "topic4", "event5")
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
