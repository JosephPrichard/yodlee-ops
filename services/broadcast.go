package svc

import "sync"

const MaxSubscribeMessages = 10

type Broadcaster struct {
	lock        sync.Mutex
	subscribers []chan []byte
}

func (b *Broadcaster) Broadcast(msg []byte) {
	b.lock.Lock()
	for _, ch := range b.subscribers {
		select {
		case ch <- msg:
		default:
			// drop if the subscriber is behind by `MaxSubscribeMessages` messages
		}
	}
	b.lock.Unlock()
}

func (b *Broadcaster) Subscribe() chan []byte {
	ch := make(chan []byte, MaxSubscribeMessages)

	b.lock.Lock()
	b.subscribers = append(b.subscribers, ch)
	b.lock.Unlock()

	return ch
}

func (b *Broadcaster) Unsubscribe(ch chan []byte) {
	b.lock.Lock()
	for i, c := range b.subscribers {
		if c == ch {
			// signify no more messages will be sent
			close(c)
			// copy back. this is pretty inexpensive when the number of subscribers is low (we expect it to be like 3 digits max)
			b.subscribers = append(b.subscribers[:i], b.subscribers[i+1:]...)
			break
		}
	}
	b.lock.Unlock()
}
