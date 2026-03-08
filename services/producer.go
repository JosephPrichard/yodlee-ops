package svc

import (
	"context"
	"encoding/json"
	"github.com/IBM/sarama"
	"log/slog"

	"yodleeops/model"
)

type JsonMessage struct {
	Key   string
	Topic model.Topic
	Value any
}

func ProduceJsonMessage(ctx Context, message JsonMessage) {
	inputBytes, err := json.Marshal(message.Value)
	if err != nil {
		slog.ErrorContext(ctx, "failed to republish json messages", "err", err)
		return
	}
	slog.InfoContext(ctx, "producing json messages", "topic", message.Topic, "size", len(inputBytes), "json", string(inputBytes))

	messages := &sarama.ProducerMessage{
		Key:   sarama.ByteEncoder(message.Key),
		Topic: string(message.Topic),
		Value: sarama.ByteEncoder(inputBytes),
	}
	select {
	case ctx.Producer.Input() <- messages:
	case <-ctx.Done():
		return
	}
}

func MakeDeleteErrorsMsgs(ctx context.Context, profileId string, deleteErrs []DeleteResult) []JsonMessage {
	if len(deleteErrs) == 0 {
		return nil
	}

	var msgs []JsonMessage
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
		slog.InfoContext(ctx, "producing delete error", "Topic", model.DeleteRetryTopic, "deleteRetry", deleteRetry)

		msgs = append(msgs, JsonMessage{
			Key:   profileId,
			Topic: model.DeleteRetryTopic,
			Value: deleteRetry,
		})
	}
	return msgs
}

func ProduceDeleteErrors(ctx Context, profileId string, deleteErrs []DeleteResult) {
	deleteErrorMsgs := MakeDeleteErrorsMsgs(ctx, profileId, deleteErrs)
	for _, msg := range deleteErrorMsgs {
		ProduceJsonMessage(ctx, msg)
	}
}

type BroadcastInput[Wrap YodleeWrapper[Inner], Inner YodleeInput] struct {
	// content of the fi messages, data extracts, response, etc.
	FiMessages  []Wrap      `json:"messages"`
	OriginTopic model.Topic `json:"originTopic"`
}

func ProducePutResults[Wrap YodleeWrapper[Inner], Inner YodleeInput](
	ctx Context,
	topic model.Topic,
	key string,
	putResults []PutResult[Wrap],
	mapInputs func([]Inner) any,
) {
	var successUploads []Wrap
	var errInputs []Inner

	for _, putErr := range putResults {
		if putErr.Err == nil {
			successUploads = append(successUploads, putErr.Input)
		} else {
			errInputs = append(errInputs, putErr.Input.Inner())
		}
	}

	if len(successUploads) > 0 {
		// write success uploads with a small wrapper describing the Topic the upload originally came from (for broadcasting).
		ProduceJsonMessage(ctx, JsonMessage{
			Topic: model.BroadcastTopic,
			Value: BroadcastInput[Wrap, Inner]{FiMessages: successUploads, OriginTopic: topic},
		})
	}
	if len(errInputs) > 0 {
		// maps the failed inputs into a new datastructure or writes as is.
		// mapInputs guarantees that `writeErrInputs` is the same type that `e.OriginTopic` expects.
		var writeErrInputs any = errInputs
		if mapInputs != nil {
			writeErrInputs = mapInputs(errInputs)
		}
		ProduceJsonMessage(ctx, JsonMessage{
			Topic: topic,
			Key:   key,
			Value: writeErrInputs,
		})
	}
}
