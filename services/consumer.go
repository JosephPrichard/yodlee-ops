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

func (*ConsumerHandler[Value]) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

func (*ConsumerHandler[Value]) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (consumer *ConsumerHandler[Value]) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		ctx := context.WithValue(context.Background(), "trace", uuid.NewString())

		start := time.Now()
		slog.InfoContext(ctx, "read message from kafka topic", "message", message)

		var data Value
		if err := json.Unmarshal(message.Value, &data); err != nil {
			slog.ErrorContext(ctx, "failed to unmarshal message", "type", fmt.Sprintf("%T", data), "message", message)
			continue
		}
		consumer.OnMessage(ctx, string(message.Key), data)

		session.MarkMessage(message, "")

		slog.InfoContext(ctx, "consumed message from kafka topic", "message", message, "elapsed", time.Since(start))
	}
	return nil
}
