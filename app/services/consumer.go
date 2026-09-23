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
	Topic     string
	OnMessage OnConsumerMessage[Value]
}

type OnConsumerMessage[Value any] = func(ctx context.Context, key string, value Value)

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
		slog.InfoContext(ctx, "read message from kafka topic", "topic", consumer.Topic, "key", strKey)

		var data Value
		if err := json.Unmarshal(message.Value, &data); err != nil {
			slog.ErrorContext(ctx, "failed to unmarshal message", "type", fmt.Sprintf("%T", data),
				"topic", consumer.Topic, "key", strKey, "message", string(message.Value))
			continue
		}

		consumer.OnMessage(ctx, strKey, data)
		session.MarkMessage(message, "")

		slog.InfoContext(ctx, "consumed message from kafka topic", "topic", consumer.Topic, "key", strKey, "elapsed", time.Since(start).String())
	}
	return nil
}
