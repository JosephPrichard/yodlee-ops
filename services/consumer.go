package svc

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/IBM/sarama"
	"github.com/google/uuid"
	"log/slog"
	"sync"
	"time"
)

var ConsumerConcurrency = 10

type ConsumerHandler[Value any] struct {
	OnMessage func(ctx context.Context, key string, value Value)
	Semaphore chan struct{}
}

func MakeStateConsumerHandler[Value any](state *State, onMessage func(ctx Context, key string, value Value)) *ConsumerHandler[Value] {
	return &ConsumerHandler[Value]{
		OnMessage: func(ctx context.Context, key string, value Value) {
			onMessage(Context{Context: ctx, State: state}, key, value)
		},
		Semaphore: make(chan struct{}, ConsumerConcurrency),
	}
}

func (*ConsumerHandler[Value]) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

func (*ConsumerHandler[Value]) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (consumer *ConsumerHandler[Value]) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	var sessionLock sync.Mutex

	for message := range claim.Messages() {
		ctx := context.WithValue(context.Background(), "trace", uuid.NewString())

		start := time.Now()
		slog.InfoContext(ctx, "read message from kafka topic", "message", message)

		var data Value
		if err := json.Unmarshal(message.Value, &data); err != nil {
			slog.ErrorContext(ctx, "failed to unmarshal message", "type", fmt.Sprintf("%T", data), "message", message)
			continue
		}

		consumer.Semaphore <- struct{}{}

		go func() {
			defer func() {
				<-consumer.Semaphore
				if r := recover(); r != nil {
					slog.ErrorContext(ctx, "recovering on message callback in consumer handler", "recover", r)
				}

				sessionLock.Lock()
				session.MarkMessage(message, "")
				sessionLock.Unlock()
			}()
			consumer.OnMessage(ctx, string(message.Key), data)

			slog.InfoContext(ctx, "consumed message from kafka topic", "message", message, "elapsed", time.Since(start))
		}()
	}
	return nil
}
