package svc

import (
	"hash/fnv"
	"slices"
	"sync"
	"yodleeops/internal/infra"
)

const MaxSubscribeMessages = 10

type FiMessageBroadcaster struct {
	lock        sync.Mutex
	subscribers []Subscriber
}

func hash(s string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(s))
	return h.Sum64()
}

func (b *FiMessageBroadcaster) Broadcast(profileID string, topic infra.Topic, msg string) {
	profileIDHash := hash(profileID)
	topicHash := hash(string(topic))

	b.lock.Lock()
	for _, subscriber := range b.subscribers {
		isTopicDisallowed := len(subscriber.topics) > 0 && !slices.Contains(subscriber.topics, topicHash)
		isProfileDisallowed := !slices.Contains(subscriber.profileIDs, profileIDHash)

		if isTopicDisallowed || isProfileDisallowed {
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

type SubscriberFilter struct {
	Topics     []infra.Topic // allowlist for topics. receive all if empty.
	ProfileIDs []string      // allowlist for profile ids.
}

type Subscriber struct {
	ch         chan string
	topics     []uint64
	profileIDs []uint64
}

func (b *FiMessageBroadcaster) Subscribe(filter SubscriberFilter) chan string {
	var topics []uint64
	var profileIDS []uint64

	for _, topic := range filter.Topics {
		if topic == "" {
			continue
		}
		topics = append(topics, hash(string(topic)))
	}
	for _, profileID := range filter.ProfileIDs {
		if profileID == "" {
			continue
		}
		profileIDS = append(profileIDS, hash(profileID))
	}

	ch := make(chan string, MaxSubscribeMessages)

	b.lock.Lock()
	b.subscribers = append(b.subscribers, Subscriber{ch: ch, topics: topics, profileIDs: profileIDS})
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
