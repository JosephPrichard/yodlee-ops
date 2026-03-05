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

type JSONConsumer[Value any] struct {
	App       *App
	OnMessage func(ctx Context, key string, value Value)
}

func MakeJSONConsumer[Value any](app *App, onMessage func(ctx Context, key string, value Value)) *JSONConsumer[Value] {
	return &JSONConsumer[Value]{
		App:       app,
		OnMessage: onMessage,
	}
}

func (*JSONConsumer[Value]) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

func (*JSONConsumer[Value]) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (consumer *JSONConsumer[Value]) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		session.MarkMessage(message, "")

		ctx := Context{
			Context: context.WithValue(context.Background(), "trace", uuid.NewString()),
			App:     consumer.App,
		}

		start := time.Now()
		slog.InfoContext(ctx, "read message from kafka topic", "message", message)

		var data Value
		if err := json.Unmarshal(message.Value, &data); err != nil {
			slog.ErrorContext(ctx, "failed to unmarshal message", "type", fmt.Sprintf("%T", data), "message", message)
			continue
		}
		consumer.OnMessage(ctx, string(message.Key), data)

		slog.InfoContext(ctx, "consumed message from kafka topic", "message", message, "elapsed", time.Since(start))
	}
	return nil
}
