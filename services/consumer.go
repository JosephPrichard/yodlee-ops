package svc

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/IBM/sarama"
	"github.com/google/uuid"
	"log/slog"
	"time"
)

type ConsumerHandler[Value any] struct {
	OnMessage func(ctx context.Context, key string, value Value)
}

func MakeStateConsumerHandler[Value any](state *State, onMessage func(ctx Context, key string, value Value)) *ConsumerHandler[Value] {
	return &ConsumerHandler[Value]{
		OnMessage: func(ctx context.Context, key string, value Value) {
			onMessage(Context{Context: ctx, State: state}, key, value)
		},
	}
}

func (*ConsumerHandler[Value]) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

func (*ConsumerHandler[Value]) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (consumer *ConsumerHandler[Value]) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		strKey := string(message.Key)

		ctx := context.WithValue(context.Background(), "trace", uuid.NewString())

		start := time.Now()
		slog.InfoContext(ctx, "read message from kafka topic", "key", strKey)

		var data Value
		if err := json.Unmarshal(message.Value, &data); err != nil {
			slog.ErrorContext(ctx, "failed to unmarshal message", "type", fmt.Sprintf("%T", data), "key", strKey, "message", string(message.Value))
			continue
		}

		consumer.OnMessage(ctx, strKey, data)
		session.MarkMessage(message, "")

		slog.InfoContext(ctx, "consumed message from kafka topic", "key", strKey, "elapsed", time.Since(start))
	}
	return nil
}
