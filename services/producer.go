package svc

import (
	"context"
	"encoding/json"
	"github.com/segmentio/kafka-go"
	"log/slog"
	"yodleeops/infra"
)

func ProduceJsonMessage(ctx AppContext, topic infra.Topic, key string, fiMessage any) {
	inputBytes, err := json.Marshal(fiMessage)
	if err != nil {
		slog.ErrorContext(ctx, "failed to republish json messages", "err", err)
		return
	}
	slog.InfoContext(ctx, "producing json messages", "topic", topic, "size", len(inputBytes), "json", string(inputBytes))

	if err := ctx.Producer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(key),
		Topic: string(topic),
		Value: inputBytes,
	}); err != nil {
		slog.ErrorContext(ctx, "failed to json messages", "err", err)
	}
}

type DeleteErrorMsg struct {
	Key   string
	Topic infra.Topic
	Value any
}

func MakeDeleteErrorsMsgs(ctx context.Context, profileId string, deleteErrs []DeleteResult) []DeleteErrorMsg {
	if len(deleteErrs) == 0 {
		return nil
	}

	var msgs []DeleteErrorMsg
	for _, deleteErr := range deleteErrs {
		var kind string
		if deleteErr.Prefix != "" {
			kind = ListKind
		} else if len(deleteErr.Keys) > 0 {
			kind = DeleteKind
		} else {
			slog.WarnContext(ctx, "skipping an empty delete Result while producing", "deleteErr", deleteErr)
			continue
		}

		deleteRetry := DeleteRetry{
			Kind:   kind,
			Bucket: deleteErr.Bucket,
			Prefix: deleteErr.Prefix,
			Keys:   deleteErr.Keys,
		}
		slog.InfoContext(ctx, "producing delete error", "topic", infra.DeleteRecoveryTopic, "deleteRetry", deleteRetry)

		msgs = append(msgs, DeleteErrorMsg{
			Key:   profileId,
			Topic: infra.DeleteRecoveryTopic,
			Value: deleteRetry,
		})
	}
	return msgs
}

func ProduceDeleteErrors(ctx AppContext, profileId string, deleteErrs []DeleteResult) {
	deleteErrorMsgs := MakeDeleteErrorsMsgs(ctx, profileId, deleteErrs)

	var msgs []kafka.Message
	for _, msg := range deleteErrorMsgs {
		msgBytes, err := json.Marshal(msg)
		if err != nil {
			slog.ErrorContext(ctx, "failed to marshal messages for put errors", "err", err)
			continue
		}
		msgs = append(msgs, kafka.Message{
			Key:   []byte(msg.Key),
			Topic: string(msg.Topic),
			Value: msgBytes,
		})
	}

	if len(msgs) > 0 {
		if err := ctx.Producer.WriteMessages(ctx, msgs...); err != nil {
			slog.ErrorContext(ctx, "failed to produce delete errors to kafka", "err", err)
		}
	}
}
