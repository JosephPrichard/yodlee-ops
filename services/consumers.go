package svc

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/IBM/sarama"
	"github.com/google/uuid"
	"log/slog"
	"yodleeops/model"
	"yodleeops/yodlee"
)

type Consumer struct {
	Topic   model.Topic
	GroupID string
	Handler sarama.ConsumerGroupHandler
}

type ConsumerHandlerFunc[Value any] = func(Context, string, Value)

type ConsumerConfig[Value any] struct {
	State   *State
	GroupID string
	Topic   model.Topic
	Handler ConsumerHandlerFunc[Value]
}

func MakeConsumer[Value any](cfg ConsumerConfig[Value]) Consumer {
	return Consumer{
		GroupID: cfg.GroupID,
		Topic:   cfg.Topic,
		Handler: &ConsumerHandler[Value]{
			Topic: string(cfg.Topic),
			OnMessage: func(ctx context.Context, key string, value Value) {
				cfg.Handler(Context{Context: ctx, State: cfg.State}, key, value)
			},
		},
	}
}

func MakeConsumers(state *State) map[model.Topic]Consumer {
	consumerList := []Consumer{
		// fi message topics have a statically computed group id because only one node receives messages
		MakeConsumer(ConsumerConfig[[]yodlee.DataExtractsProviderAccount]{
			Topic:   model.CnctRefreshTopic,
			State:   state,
			GroupID: model.CnctRefreshTopicGroupID,
			Handler: ConsumeCnctRefreshMessage,
		}),
		MakeConsumer(ConsumerConfig[[]yodlee.DataExtractsAccount]{
			Topic:   model.AcctRefreshTopic,
			State:   state,
			GroupID: model.AcctRefreshTopicGroupID,
			Handler: ConsumeAcctRefreshMessage,
		}),
		MakeConsumer(ConsumerConfig[[]yodlee.DataExtractsHolding]{
			Topic:   model.HoldRefreshTopic,
			State:   state,
			GroupID: model.HoldRefreshTopicGroupID,
			Handler: ConsumeHoldRefreshMessage,
		}),
		MakeConsumer(ConsumerConfig[[]yodlee.DataExtractsTransaction]{
			Topic:   model.TxnRefreshTopic,
			State:   state,
			GroupID: model.TxnRefreshTopicGroupID,
			Handler: ConsumeTxnRefreshMessage,
		}),
		MakeConsumer(ConsumerConfig[yodlee.ProviderAccountResponse]{
			Topic:   model.CnctResponseTopic,
			State:   state,
			GroupID: model.CnctResponseTopicGroupID,
			Handler: ConsumeCnctResponseMessage,
		}),
		MakeConsumer(ConsumerConfig[yodlee.AccountResponse]{
			Topic:   model.AcctResponseTopic,
			State:   state,
			GroupID: model.AcctResponseTopicGroupID,
			Handler: ConsumeAcctResponseMessage,
		}),
		MakeConsumer(ConsumerConfig[yodlee.HoldingResponse]{
			Topic:   model.HoldResponseTopic,
			State:   state,
			GroupID: model.HoldResponseTopicGroupID,
			Handler: ConsumeHoldResponseMessage,
		}),
		MakeConsumer(ConsumerConfig[yodlee.TransactionResponse]{
			Topic:   model.TxnResponseTopic,
			State:   state,
			GroupID: model.TxnResponseTopicGroupID,
			Handler: ConsumeTxnResponseMessage,
		}),
		MakeConsumer(ConsumerConfig[[]DeleteRetry]{
			Topic:   model.DeleteRetryTopic,
			State:   state,
			GroupID: model.DeleteRetryTopicGroupID,
			Handler: ConsumeDeleteRetryMessage,
		}),
		// the broadcast topic has a dynamically computed group id because each node receives the message
		MakeConsumer(ConsumerConfig[BroadcastOutput]{
			Topic:   model.BroadcastTopic,
			State:   state,
			GroupID: uuid.NewString(),
			Handler: ConsumeBroadcastMessage,
		}),
	}

	consumerMap := make(map[model.Topic]Consumer, len(consumerList))
	for _, c := range consumerList {
		consumerMap[c.Topic] = c
	}
	return consumerMap
}

func StartConsumers(ctx context.Context, kafkaBrokers []string, config *sarama.Config, app *State) error {
	consumers := MakeConsumers(app)
	for topic, consumer := range consumers {
		consumerGroup, err := sarama.NewConsumerGroup(kafkaBrokers, consumer.GroupID, config)
		if err != nil {
			return fmt.Errorf("starting yodlee ops, failed to create kafka consumer: %v", err)
		}
		// begin a comsumer consumer loop for each topic
		topics := []string{string(topic)}
		go func() {
			for {
				if err := consumerGroup.Consume(ctx, topics, consumer.Handler); err != nil {
					slog.ErrorContext(ctx, "failed to start consumer group", "consumer", fmt.Sprintf("+%v", consumer), "err", err)
				}
				// stop consumer loop when context is canceled.
				if ctx.Err() != nil {
					return
				}
			}
		}()
	}

	slog.Info("starting yodlee ops, started consumers", "consumers", fmt.Sprintf("+%v", consumers))
	return nil
}

func ConsumeCnctRefreshMessage(ctx Context, key string, cncts []yodlee.DataExtractsProviderAccount) {
	slog.InfoContext(ctx, "handling cnct refresh messages", "cncts", cncts)

	result := IngestCnctRefreshes(ctx, key, cncts)
	slog.InfoContext(ctx, "completed cnct refresh ingestion", "putResults", result.PutResults, "deleteErrs", result.DeleteErrors)

	ProducePutResults(ctx, model.CnctRefreshTopic, key, result.PutResults, nil)
	ProduceDeleteErrors(ctx, key, result.DeleteErrors)
}

func ConsumeAcctRefreshMessage(ctx Context, key string, accts []yodlee.DataExtractsAccount) {
	slog.InfoContext(ctx, "handling acct refresh messages", "accts", accts)

	result := IngestAcctsRefreshes(ctx, key, accts)
	slog.InfoContext(ctx, "completed acct refresh ingestion", "putResults", result.PutResults, "deleteErrs", result.DeleteErrors)

	ProducePutResults(ctx, model.AcctRefreshTopic, key, result.PutResults, nil)
	ProduceDeleteErrors(ctx, key, result.DeleteErrors)
}

func ConsumeTxnRefreshMessage(ctx Context, key string, txns []yodlee.DataExtractsTransaction) {
	slog.InfoContext(ctx, "handling txn refresh messages", "txns", txns)

	result := IngestTxnRefreshes(ctx, key, txns)
	slog.InfoContext(ctx, "completed txn refresh ingestion", "putResults", result.PutResults, "deleteErrs", result.DeleteErrors)

	ProducePutResults(ctx, model.TxnRefreshTopic, key, result.PutResults, nil)
	ProduceDeleteErrors(ctx, key, result.DeleteErrors)
}

func ConsumeHoldRefreshMessage(ctx Context, key string, holds []yodlee.DataExtractsHolding) {
	slog.InfoContext(ctx, "handling hold refresh messages", "holds", holds)

	result := IngestHoldRefreshes(ctx, key, holds)
	slog.InfoContext(ctx, "completed hold refresh ingestion", "putResults", result.PutResults, "deleteErrs", result.DeleteErrors)

	ProducePutResults(ctx, model.HoldRefreshTopic, key, result.PutResults, nil)
	ProduceDeleteErrors(ctx, key, result.DeleteErrors)
}

func ConsumeCnctResponseMessage(ctx Context, key string, cncts yodlee.ProviderAccountResponse) {
	slog.InfoContext(ctx, "handling cnct response messages", "cncts", cncts)

	putResults := IngestCnctResponses(ctx, key, cncts)

	ProducePutResults(ctx, model.CnctResponseTopic, key, putResults, func(errInputs []yodlee.ProviderAccount) any {
		return yodlee.ProviderAccountResponse{ProviderAccount: errInputs}
	})
}

func ConsumeAcctResponseMessage(ctx Context, key string, accts yodlee.AccountResponse) {
	slog.InfoContext(ctx, "handling acct response messages", "accts", accts)

	putResults := IngestAcctResponses(ctx, key, accts)

	ProducePutResults(ctx, model.AcctResponseTopic, key, putResults, func(errInputs []yodlee.Account) any {
		return yodlee.AccountResponse{Account: errInputs}
	})
}

func ConsumeTxnResponseMessage(ctx Context, key string, txns yodlee.TransactionResponse) {
	slog.InfoContext(ctx, "handling txn response messages", "txns", txns)

	putResults := IngestTxnResponses(ctx, key, txns)

	ProducePutResults(ctx, model.TxnResponseTopic, key, putResults, func(errInputs []yodlee.TransactionWithDateTime) any {
		return yodlee.TransactionResponse{Transaction: errInputs}
	})
}

func ConsumeHoldResponseMessage(ctx Context, key string, holds yodlee.HoldingResponse) {
	slog.InfoContext(ctx, "handling hold response messages", "holds", holds)

	putResults := IngestHoldResponses(ctx, key, holds)

	ProducePutResults(ctx, model.HoldResponseTopic, key, putResults, func(errInputs []yodlee.Holding) any {
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
