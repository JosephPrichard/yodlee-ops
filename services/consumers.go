package svc

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/IBM/sarama"
	"github.com/google/uuid"
	"log/slog"
	"yodleeops/infra"
	"yodleeops/yodlee"
)

type Consumer struct {
	GroupID string
	Topic   infra.Topic
	Handler sarama.ConsumerGroupHandler
}

func MakeConsumerHandler[Value any](state *State, onMessage func(ctx Context, key string, value Value)) *ConsumerHandler[Value] {
	return &ConsumerHandler[Value]{
		OnMessage: func(ctx context.Context, key string, value Value) {
			onMessage(Context{Context: ctx, State: state}, key, value)
		},
	}
}

func MakeConsumers(state *State) []Consumer {
	return []Consumer{
		// fi message topics have a statically computed group id because only one node receives messages
		{
			GroupID: infra.CnctRefreshTopicGroupID,
			Topic:   infra.CnctRefreshTopic,
			Handler: MakeConsumerHandler(state, ConsumeCnctRefreshMessage),
		},
		{
			GroupID: infra.AcctRefreshTopicGroupID,
			Topic:   infra.AcctRefreshTopic,
			Handler: MakeConsumerHandler(state, ConsumeAcctRefreshMessage),
		},
		{
			GroupID: infra.HoldRefreshTopicGroupID,
			Topic:   infra.HoldRefreshTopic,
			Handler: MakeConsumerHandler(state, ConsumeHoldRefreshMessage),
		},
		{
			GroupID: infra.TxnRefreshTopicGroupID,
			Topic:   infra.TxnRefreshTopic,
			Handler: MakeConsumerHandler(state, ConsumeTxnRefreshMessage),
		},
		{
			GroupID: infra.CnctResponseTopicGroupID,
			Topic:   infra.CnctResponseTopic,
			Handler: MakeConsumerHandler(state, ConsumeCnctResponseMessage),
		},
		{
			GroupID: infra.AcctResponseTopicGroupID,
			Topic:   infra.AcctResponseTopic,
			Handler: MakeConsumerHandler(state, ConsumeAcctResponseMessage),
		},
		{
			GroupID: infra.HoldResponseTopicGroupID,
			Topic:   infra.HoldResponseTopic,
			Handler: MakeConsumerHandler(state, ConsumeHoldResponseMessage),
		},
		{
			GroupID: infra.TxnResponseTopicGroupID,
			Topic:   infra.TxnResponseTopic,
			Handler: MakeConsumerHandler(state, ConsumeTxnResponseMessage),
		},
		{
			GroupID: infra.DeleteRetryTopicGroupID,
			Topic:   infra.DeleteRetryTopic,
			Handler: MakeConsumerHandler(state, ConsumeDeleteRetryMessage),
		},
		// the broadcast topic has a dynamically computed group id because each node receives the message
		{
			GroupID: uuid.NewString(),
			Topic:   infra.BroadcastTopic,
			Handler: MakeConsumerHandler(state, ConsumeBroadcastMessage),
		},
	}
}

func StartConsumers(ctx context.Context, kafkaBrokers []string, config *sarama.Config, app *State) error {
	consumers := MakeConsumers(app)
	for _, consumer := range consumers {
		consumerGroup, err := sarama.NewConsumerGroup(kafkaBrokers, consumer.GroupID, config)
		if err != nil {
			return fmt.Errorf("failed to create kafka consumer: %v", err)
		}
		// begin a comsumer consumer loop for each topic
		topics := []string{string(consumer.Topic)}
		go func() {
			for {
				if err := consumerGroup.Consume(ctx, topics, consumer.Handler); err != nil {
					slog.ErrorContext(ctx, "failed to start consumer consumer", "consumer", consumer, "err", err)
				}
				// stop consumer loop when context is canceled.
				if ctx.Err() != nil {
					return
				}
			}
		}()
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
