package svc

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/IBM/sarama"
	"github.com/google/uuid"
	"log"
	"log/slog"
	"time"

	"yodleeops/infra"
	"yodleeops/yodlee"
)

func StartConsumers(ctx context.Context, app *App, kafkaBrokers []string) {
	consumerGroupID := func(t infra.Topic) string {
		return string(t) + "_group"
	}

	groups := []struct {
		ID      string
		Topic   infra.Topic
		Handler sarama.ConsumerGroupHandler
	}{
		// fi message topics have a staticly computed group id because only one node receives mesages
		{
			ID:      consumerGroupID(infra.CnctRefreshTopic),
			Topic:   infra.CnctRefreshTopic,
			Handler: MakeConsumer(app, ConsumeCnctRefreshMessage),
		},
		{
			ID:      consumerGroupID(infra.AcctRefreshTopic),
			Topic:   infra.AcctRefreshTopic,
			Handler: MakeConsumer(app, ConsumeAcctRefreshMessage),
		},
		{
			ID:      consumerGroupID(infra.HoldRefreshTopic),
			Topic:   infra.HoldRefreshTopic,
			Handler: MakeConsumer(app, ConsumeHoldRefreshMessage),
		},
		{
			ID:      consumerGroupID(infra.TxnRefreshTopic),
			Topic:   infra.TxnRefreshTopic,
			Handler: MakeConsumer(app, ConsumeTxnRefreshMessage),
		},
		{
			ID:      consumerGroupID(infra.CnctResponseTopic),
			Topic:   infra.CnctResponseTopic,
			Handler: MakeConsumer(app, ConsumeCnctResponseMessage),
		},
		{
			ID:      consumerGroupID(infra.AcctResponseTopic),
			Topic:   infra.AcctResponseTopic,
			Handler: MakeConsumer(app, ConsumeAcctResponseMessage),
		},
		{
			ID:      consumerGroupID(infra.HoldResponseTopic),
			Topic:   infra.HoldResponseTopic,
			Handler: MakeConsumer(app, ConsumeHoldResponseMessage),
		},
		{
			ID:      consumerGroupID(infra.TxnResponseTopic),
			Topic:   infra.TxnResponseTopic,
			Handler: MakeConsumer(app, ConsumeTxnResponseMessage),
		},
		{
			ID:      consumerGroupID(infra.DeleteRetryTopic),
			Topic:   infra.DeleteRetryTopic,
			Handler: MakeConsumer(app, ConsumeDeleteRetryMessage),
		},
		// the broadcast topic has a dynamically computed group id because each node receives the message
		{
			ID:      uuid.NewString(),
			Topic:   infra.BroadcastTopic,
			Handler: MakeConsumer(app, ConsumeBroadcastMessage),
		},
	}
	for _, group := range groups {
		consumerGroup, err := sarama.NewConsumerGroup(kafkaBrokers, group.ID, nil)
		if err != nil {
			log.Fatalf("failed to create kafka consumer: %v", err)
		}

		topics := []string{string(group.Topic)}
		go func() {
			for {
				if err := consumerGroup.Consume(ctx, topics, group.Handler); err != nil {
					slog.ErrorContext(ctx, "failed to start consumer group", "group", group, "err", err)
				}
				if ctx.Err() != nil {
					return
				}
			}
		}()
	}
}

type JSONConsumer[Value any] struct {
	App       *App
	OnMessage func(ctx Context, key string, value Value)
}

func MakeConsumer[Value any](app *App, onMessage func(ctx Context, key string, value Value)) *JSONConsumer[Value] {
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

func ConsumeCnctRefreshMessage(ctx Context, key string, cncts []yodlee.DataExtractsProviderAccount) {
	slog.InfoContext(ctx, "handling cnct refresh messages", "cncts", cncts)

	result := IngestCnctRefreshes(ctx, key, cncts)
	slog.InfoContext(ctx, "completed cnct refresh ingestion", "putResults", result.PutResults, "deleteErrs", result.DeleteErrors)

	ProducePutResults(ctx, infra.CnctRefreshTopic, key, result.PutResults, nil)
	ProduceDeleteErrors(ctx, key, result.DeleteErrors)
}

func ConsumeAcctRefreshMessage(ctx Context, key string, accts []yodlee.DataExtractsAccount) {
	slog.InfoContext(ctx, "handling acct refresh messages", "accts", accts)

	result := IngestAcctsRefreshes(ctx, key, accts)
	slog.InfoContext(ctx, "completed acct refresh ingestion", "putResults", result.PutResults, "deleteErrs", result.DeleteErrors)

	ProducePutResults(ctx, infra.AcctRefreshTopic, key, result.PutResults, nil)
	ProduceDeleteErrors(ctx, key, result.DeleteErrors)
}

func ConsumeTxnRefreshMessage(ctx Context, key string, txns []yodlee.DataExtractsTransaction) {
	slog.InfoContext(ctx, "handling txn refresh messages", "txns", txns)

	result := IngestTxnRefreshes(ctx, key, txns)
	slog.InfoContext(ctx, "completed txn refresh ingestion", "putResults", result.PutResults, "deleteErrs", result.DeleteErrors)

	ProducePutResults(ctx, infra.TxnRefreshTopic, key, result.PutResults, nil)
	ProduceDeleteErrors(ctx, key, result.DeleteErrors)
}

func ConsumeHoldRefreshMessage(ctx Context, key string, holds []yodlee.DataExtractsHolding) {
	slog.InfoContext(ctx, "handling hold refresh messages", "holds", holds)

	result := IngestHoldRefreshes(ctx, key, holds)
	slog.InfoContext(ctx, "completed hold refresh ingestion", "putResults", result.PutResults, "deleteErrs", result.DeleteErrors)

	ProducePutResults(ctx, infra.HoldRefreshTopic, key, result.PutResults, nil)
	ProduceDeleteErrors(ctx, key, result.DeleteErrors)
}

func ConsumeCnctResponseMessage(ctx Context, key string, cncts yodlee.ProviderAccountResponse) {
	slog.InfoContext(ctx, "handling cnct response messages", "cncts", cncts)

	putResults := IngestCnctResponses(ctx, key, cncts)

	ProducePutResults(ctx, infra.CnctResponseTopic, key, putResults, func(errInputs []yodlee.ProviderAccount) any {
		return yodlee.ProviderAccountResponse{ProviderAccount: errInputs}
	})
}

func ConsumeAcctResponseMessage(ctx Context, key string, accts yodlee.AccountResponse) {
	slog.InfoContext(ctx, "handling acct response messages", "accts", accts)

	putResults := IngestAcctResponses(ctx, key, accts)

	ProducePutResults(ctx, infra.AcctResponseTopic, key, putResults, func(errInputs []yodlee.Account) any {
		return yodlee.AccountResponse{Account: errInputs}
	})
}

func ConsumeTxnResponseMessage(ctx Context, key string, txns yodlee.TransactionResponse) {
	slog.InfoContext(ctx, "handling txn response messages", "txns", txns)

	putResults := IngestTxnResponses(ctx, key, txns)

	ProducePutResults(ctx, infra.TxnResponseTopic, key, putResults, func(errInputs []yodlee.TransactionWithDateTime) any {
		return yodlee.TransactionResponse{Transaction: errInputs}
	})
}

func ConsumeHoldResponseMessage(ctx Context, key string, holds yodlee.HoldingResponse) {
	slog.InfoContext(ctx, "handling hold response messages", "holds", holds)

	putResults := IngestHoldResponses(ctx, key, holds)

	ProducePutResults(ctx, infra.HoldResponseTopic, key, putResults, func(errInputs []yodlee.Holding) any {
		return yodlee.HoldingResponse{Holding: errInputs}
	})
}

func ConsumeDeleteRetryMessage(ctx Context, key string, deleteRetries []DeleteRetry) {
	slog.InfoContext(ctx, "handling delete recovery messages", "deleteRetries", deleteRetries)

	deleteErrors := IngestDeleteRetries(ctx, deleteRetries)
	ProduceDeleteErrors(ctx, key, deleteErrors)
}

type BroadcastOutput struct {
	OriginTopic string            `json:"origintopic"`
	FiMessages  []json.RawMessage `json:"messages"`
}

func ConsumeBroadcastMessage(ctx Context, _ string, broadcast BroadcastOutput) {
	for _, binaryMsg := range broadcast.FiMessages {
		var opsFiMessage OpsFiMessage
		if err := json.Unmarshal(binaryMsg, &opsFiMessage); err != nil {
			slog.ErrorContext(ctx, "failed to unmarshal broadcast message opsFiMessage", "err", err)
			continue
		}

		strMsg := string(binaryMsg)
		slog.InfoContext(ctx, "broadcasting message", "Topic", opsFiMessage.OriginTopic, "message", strMsg)
		ctx.FiMessageBroadcaster.Broadcast(opsFiMessage.ProfileId, opsFiMessage.OriginTopic, strMsg)
	}
}
