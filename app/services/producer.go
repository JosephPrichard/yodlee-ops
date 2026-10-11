package svc

import (
	"encoding/json"
	"log/slog"
	"yodleeops/storage"

	"github.com/IBM/sarama"
)

type JsonMessage struct {
	Key   string
	Topic storage.Topic
	Value any
}

func ProduceDeleteErrors(ctx Context, profileId string, deleteErrs []DeleteResult) {
	if len(deleteErrs) == 0 {
		return
	}

	var deleteErrorMsgs []JsonMessage
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
		slog.InfoContext(ctx, "producing delete error", "Topic", storage.DeleteRetryTopic, "deleteRetry", deleteRetry)

		deleteErrorMsgs = append(deleteErrorMsgs, JsonMessage{
			Key:   profileId,
			Topic: storage.DeleteRetryTopic,
			Value: deleteRetry,
		})
	}

	for _, msg := range deleteErrorMsgs {
		produceJsonMessage(ctx, msg)
	}
}

type BroadcastInput[Wrap YodleeWrapper[Inner], Inner YodleeInput] struct {
	// content of the fi messages, data extracts, response, etc.
	FiMessages  []Wrap        `json:"messages"`
	OriginTopic storage.Topic `json:"originTopic"`
}

func ProducePutResults[Wrap YodleeWrapper[Inner], Inner YodleeInput](
	ctx Context,
	topic storage.Topic,
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
		produceJsonMessage(ctx, JsonMessage{
			Topic: storage.BroadcastTopic,
			Value: BroadcastInput[Wrap, Inner]{FiMessages: successUploads, OriginTopic: topic},
		})
	}
	if len(errInputs) > 0 {
		// maps the failed inputs into a new datastructures or writes as is
		// mapInputs guarantees that `writeErrInputs` is the same type that `e.OriginTopic` expects.
		var writeErrInputs any = errInputs
		if mapInputs != nil {
			writeErrInputs = mapInputs(errInputs)
		}
		produceJsonMessage(ctx, JsonMessage{
			Topic: topic,
			Key:   key,
			Value: writeErrInputs,
		})
	}
}

func produceJsonMessage(ctx Context, message JsonMessage) {
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
