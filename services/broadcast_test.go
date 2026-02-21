package svc

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"yodleeops/internal/infra"
)

func TestFiMessageBroadcaster(t *testing.T) {
	b := FiMessageBroadcaster{}

	ch1 := b.Subscribe(SubscriberFilter{
		ProfileIDs: []string{"profile1"},
		Topics:     []infra.Topic{"topic1"},
	})
	ch2 := b.Subscribe(SubscriberFilter{
		ProfileIDs: []string{"profile2"},
		Topics:     []infra.Topic{"topic2"},
	})
	ch3 := b.Subscribe(SubscriberFilter{
		ProfileIDs: []string{"profile1"},
		Topics:     []infra.Topic{"topic1", "topic2"},
	})
	ch4 := b.Subscribe(SubscriberFilter{
		ProfileIDs: []string{"profile1"},
	})

	b.Broadcast("profile1", "topic1", "msg1")
	b.Broadcast("profile2", "topic2", "msg2")
	b.Broadcast("profile1", "topic2", "msg3")

	b.Unsubscribe(ch3)
	b.Broadcast("profile1", "topic2", "msg4")

	b.Unsubscribe(ch1)
	b.Unsubscribe(ch2)
	b.Unsubscribe(ch4)

	var msgTable [][]string
	for _, ch := range []chan string{ch1, ch2, ch3, ch4} {
		msgs := make([]string, 0)
		for msg := range ch {
			msgs = append(msgs, msg)
		}
		msgTable = append(msgTable, msgs)
	}

	wantMessages := [][]string{
		{"msg1"},
		{"msg2"},
		{"msg1", "msg3"},
		{"msg1", "msg3", "msg4"},
	}
	assert.Equal(t, wantMessages, msgTable)
}
