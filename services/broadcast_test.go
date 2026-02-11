package svc

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestFiMessageBroadcaster(t *testing.T) {
	b := FiMessageBroadcaster{}

	ch1 := b.Subscribe([]string{"topic1"})
	ch2 := b.Subscribe([]string{"topic2"})
	ch3 := b.Subscribe([]string{"topic1", "topic2"})
	ch4 := b.Subscribe([]string{})

	b.Broadcast("topic1", "msg1")
	b.Broadcast("topic2", "msg2")

	b.Unsubscribe(ch3)
	b.Broadcast("topic2", "msg3")

	b.Unsubscribe(ch1)
	b.Unsubscribe(ch2)
	b.Unsubscribe(ch4)

	var allMessages [][]string
	for _, ch := range []chan string{ch1, ch2, ch3, ch4} {
		chMessages := make([]string, 0)
		for msg := range ch {
			chMessages = append(chMessages, msg)
		}
		allMessages = append(allMessages, chMessages)
	}

	wantMessages := [][]string{
		{"msg1"},
		{"msg2", "msg3"},
		{"msg1", "msg2"},
		{},
	}
	assert.Equal(t, wantMessages, allMessages)
}
