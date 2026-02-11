package svc

import (
	"slices"
	"sync"
)

const MaxSubscribeMessages = 10

type FiMessageBroadcaster struct {
	lock        sync.Mutex
	subscribers []Subscriber
}

func (b *FiMessageBroadcaster) Broadcast(topic string, msg string) {
	b.lock.Lock()
	for _, subscriber := range b.subscribers {
		if !slices.Contains(subscriber.topics, topic) {
			continue
		}
		select {
		case subscriber.ch <- msg:
		default:
			// drop if the subscriber is behind by `MaxSubscribeMessages` messages
		}
	}
	b.lock.Unlock()
}

type Subscriber struct {
	ch     chan string
	topics []string
}

func (b *FiMessageBroadcaster) Subscribe(topics []string) chan string {
	ch := make(chan string, MaxSubscribeMessages)

	b.lock.Lock()
	b.subscribers = append(b.subscribers, Subscriber{ch: ch, topics: topics})
	b.lock.Unlock()

	return ch
}

func (b *FiMessageBroadcaster) Unsubscribe(removeCh chan string) {
	b.lock.Lock()
	for i, subscriber := range b.subscribers {
		if subscriber.ch == removeCh {
			// signify no more messages will be sent
			close(subscriber.ch)
			// copy back. this is pretty inexpensive when the number of subscribers is low (we expect it to be like 3 digits max)
			b.subscribers = append(b.subscribers[:i], b.subscribers[i+1:]...)
			break
		}
	}
	b.lock.Unlock()
}
