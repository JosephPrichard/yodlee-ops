package svc

import (
	"context"
	"encoding/json"
	"github.com/segmentio/kafka-go"
	"log/slog"
)

func ProducePutErrors[Input any](ctx context.Context, app *App, topic string, putErrs []PutError[Input]) {
	var msgs []kafka.Message
	for _, putErr := range putErrs {
		inputBytes, inputErr := json.Marshal(putErr.Origin)

		if err := inputErr; err != nil {
			slog.ErrorContext(ctx, "failed to marshal messages for put errors", "err", err)
			continue
		}

		msgs = append(msgs, kafka.Message{
			Topic: topic,
			Value: inputBytes,
		})
	}
	if len(msgs) > 0 {
		if err := app.Producer.WriteMessages(ctx, msgs...); err != nil {
			// if we aren't able to put these messages, we need to drop them. this is already a "last line" of defense for errors that shouldn't have happened
			slog.ErrorContext(ctx, "failed to write put errors to kafka", "err", err)
		}
	}
}
