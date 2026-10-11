package svc

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"
	"yodleeops/storage"

	"github.com/IBM/sarama"
	"github.com/google/uuid"
)

type ConsumerHandler[Value any] struct {
	groupID string
	topic   storage.Topic
	state   *State
	handler func(Context, string, Value)
}

func (consumer *ConsumerHandler[Value]) Topic() storage.Topic {
	return consumer.topic
}

func (consumer *ConsumerHandler[Value]) GroupID() string {
	return consumer.groupID
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
		slog.InfoContext(ctx, "read message from kafka topic", "topic", consumer.topic, "key", strKey)

		var data Value
		if err := json.Unmarshal(message.Value, &data); err != nil {
			slog.ErrorContext(ctx, "failed to unmarshal message", "type", fmt.Sprintf("%T", data),
				"topic", consumer.topic, "key", strKey, "message", string(message.Value))
			continue
		}

		serviceCtx := Context{Context: ctx, State: consumer.state}

		consumer.handler(serviceCtx, strKey, data)
		session.MarkMessage(message, "")

		slog.InfoContext(ctx, "consumed message from kafka topic", "topic", consumer.topic, "key", strKey, "elapsed", time.Since(start).String())
	}
	return nil
}
