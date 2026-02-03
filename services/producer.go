package svc

import (
	"context"
	"encoding/json"
	"log/slog"

	"github.com/segmentio/kafka-go"
)

func (app *App) ProducePutErrors(ctx context.Context, topic string, putErrs []PutResult) {
	if len(putErrs) == 0 {
		return
	}

	var msgs []kafka.Message
	for _, putErr := range putErrs {
		inputBytes, inputErr := json.Marshal(putErr.Origin)

		if err := inputErr; err != nil {
			slog.ErrorContext(ctx, "failed to marshal messages for put errors", "err", err)
			continue
		}

		slog.InfoContext(ctx, "producing put error", "topic", topic, "bytes", len(inputBytes), "originalInput", putErr.Origin)

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

func (app *App) ProduceDeleteErrors(ctx context.Context, deleteErrs []DeleteResult) {
	if len(deleteErrs) == 0 {
		return
	}

	var msgs []kafka.Message
	for _, deleteErr := range deleteErrs {
		var kind string
		if deleteErr.Prefix != "" {
			kind = ListKind
		} else if len(deleteErr.Keys) > 0 {
			kind = DeleteKind
		} else {
			slog.WarnContext(ctx, "skipping an produce for delete result", "deleteErr", deleteErr)
			continue
		}

		deleteRetry := DeleteRetry{
			Kind: kind,
			Bucket: deleteErr.Bucket,
			Prefix: deleteErr.Prefix,
			Keys: deleteErr.Keys,
		}
		msgBytes, inputErr := json.Marshal(deleteRetry)

		if err := inputErr; err != nil {
			slog.ErrorContext(ctx, "failed to marshal messages for put errors", "err", err)
			continue
		}

		slog.InfoContext(ctx, "producing delete error", "topic", app.DeleteRecoveryTopic, "bytes", len(msgBytes), "deleteRetry", deleteRetry)

		msgs = append(msgs, kafka.Message{
			Topic: app.DeleteRecoveryTopic,
			Value: msgBytes,
		})
	}
	if len(msgs) > 0 {
		if err := app.Producer.WriteMessages(ctx, msgs...); err != nil {
			// if we aren't able to put these messages, we need to drop them. this is already a "last line" of defense for errors that shouldn't have happened
			slog.ErrorContext(ctx, "failed to write delete errors to kafka", "err", err)
		}
	}
}
