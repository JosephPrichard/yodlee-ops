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

	var dataArray []any

	for _, putErr := range putErrs {
		dataArray = append(dataArray, putErr.Origin)
	}

	if len(dataArray) > 0 {
		inputBytes, err := json.Marshal(dataArray)
		if err != nil {
			slog.ErrorContext(ctx, "failed to marshal messages for put errors", "err", err)
			return
		}
		slog.InfoContext(ctx, "producing put error", "topic", topic, "bytes", len(inputBytes), "dataArray", dataArray)

		msg := kafka.Message{
			Topic: topic,
			Value: inputBytes,
		}

		if err := app.Producer.WriteMessages(ctx, msg); err != nil {
			slog.ErrorContext(ctx, "failed to produce put errors to kafka", "err", err)
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
			slog.WarnContext(ctx, "skipping an empty delete result while producing", "deleteErr", deleteErr)
			continue
		}

		deleteRetry := DeleteRetry{
			Kind:   kind,
			Bucket: deleteErr.Bucket,
			Prefix: deleteErr.Prefix,
			Keys:   deleteErr.Keys,
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
			slog.ErrorContext(ctx, "failed to produce delete errors to kafka", "err", err)
		}
	}
}
