package svc

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"log/slog"
	"yodleeops/storage"

	"github.com/IBM/sarama"
	"github.com/google/uuid"

	"yodleeops/yodlee"
)

type ConsumerConfig[Value any] struct {
	State   *State
	GroupID string
	Topic   storage.Topic
	Handler func(Context, string, Value)
}

type Consumer interface {
	sarama.ConsumerGroupHandler
	Topic() storage.Topic
	GroupID() string
}

func MakeConsumerList(state *State) []Consumer {
	return []Consumer{
		// fi message topics have a statically computed group id because only one node receives messages
		&ConsumerHandler[[]yodlee.DataExtractsProviderAccount]{
			topic:   storage.CnctRefreshTopic,
			state:   state,
			groupID: storage.CnctRefreshTopicGroupID,
			handler: ConsumeCnctRefreshMessage,
		},
		&ConsumerHandler[[]yodlee.DataExtractsAccount]{
			topic:   storage.AcctRefreshTopic,
			state:   state,
			groupID: storage.AcctRefreshTopicGroupID,
			handler: ConsumeAcctRefreshMessage,
		},
		&ConsumerHandler[[]yodlee.DataExtractsHolding]{
			topic:   storage.HoldRefreshTopic,
			state:   state,
			groupID: storage.HoldRefreshTopicGroupID,
			handler: ConsumeHoldRefreshMessage,
		},
		&ConsumerHandler[[]yodlee.DataExtractsTransaction]{
			topic:   storage.TxnRefreshTopic,
			state:   state,
			groupID: storage.TxnRefreshTopicGroupID,
			handler: ConsumeTxnRefreshMessage,
		},
		&ConsumerHandler[yodlee.ProviderAccountResponse]{
			topic:   storage.CnctResponseTopic,
			state:   state,
			groupID: storage.CnctResponseTopicGroupID,
			handler: ConsumeCnctResponseMessage,
		},
		&ConsumerHandler[yodlee.AccountResponse]{
			topic:   storage.AcctResponseTopic,
			state:   state,
			groupID: storage.AcctResponseTopicGroupID,
			handler: ConsumeAcctResponseMessage,
		},
		&ConsumerHandler[yodlee.HoldingResponse]{
			topic:   storage.HoldResponseTopic,
			state:   state,
			groupID: storage.HoldResponseTopicGroupID,
			handler: ConsumeHoldResponseMessage,
		},
		&ConsumerHandler[yodlee.TransactionResponse]{
			topic:   storage.TxnResponseTopic,
			state:   state,
			groupID: storage.TxnResponseTopicGroupID,
			handler: ConsumeTxnResponseMessage,
		},
		&ConsumerHandler[[]DeleteRetry]{
			topic:   storage.DeleteRetryTopic,
			state:   state,
			groupID: storage.DeleteRetryTopicGroupID,
			handler: ConsumeDeleteRetryMessage,
		},
		// the broadcast topic has a dynamically computed group id because each node receives the message
		&ConsumerHandler[BroadcastOutput]{
			topic:   storage.BroadcastTopic,
			state:   state,
			groupID: uuid.NewString(),
			handler: ConsumeBroadcastMessage,
		},
	}
}

func StartConsumers(ctx context.Context, kafkaBrokers []string, config *sarama.Config, state *State) error {
	consumers := MakeConsumerList(state)

	for _, consumer := range consumers {
		go consumerLoop(ctx, kafkaBrokers, config, consumer)
	}

	slog.Info("starting yodlee ops, started consumers", "consumers", fmt.Sprintf("%+v", consumers))
	return nil
}

func consumerLoop(ctx context.Context, kafkaBrokers []string, config *sarama.Config, consumer Consumer) {
	consumerGroup, err := sarama.NewConsumerGroup(kafkaBrokers, consumer.GroupID(), config)
	if err != nil {
		log.Fatalf("failed to create consumer group: %v", err)
	}
	defer consumerGroup.Close()

	topics := []string{consumer.Topic().String()}
	for {
		if err := consumerGroup.Consume(ctx, topics, consumer); err != nil {
			slog.ErrorContext(ctx, "failed to start consumer group", "consumer", fmt.Sprintf("%+v", consumer), "err", err)
		}
		// stop consumer loop when context is canceled.
		if ctx.Err() != nil {
			return
		}
	}
}

func ConsumeCnctRefreshMessage(ctx Context, key string, cncts []yodlee.DataExtractsProviderAccount) {
	slog.InfoContext(ctx, "handling cnct refresh messages", "cncts", cncts)

	result := IngestCnctRefreshes(ctx, key, cncts)
	slog.InfoContext(ctx, "completed cnct refresh ingestion", "putResults", result.PutResults, "deleteErrs", result.DeleteErrors)

	ProducePutResults(ctx, storage.CnctRefreshTopic, key, result.PutResults, nil)
	ProduceDeleteErrors(ctx, key, result.DeleteErrors)
}

func ConsumeAcctRefreshMessage(ctx Context, key string, accts []yodlee.DataExtractsAccount) {
	slog.InfoContext(ctx, "handling acct refresh messages", "accts", accts)

	result := IngestAcctsRefreshes(ctx, key, accts)
	slog.InfoContext(ctx, "completed acct refresh ingestion", "putResults", result.PutResults, "deleteErrs", result.DeleteErrors)

	ProducePutResults(ctx, storage.AcctRefreshTopic, key, result.PutResults, nil)
	ProduceDeleteErrors(ctx, key, result.DeleteErrors)
}

func ConsumeTxnRefreshMessage(ctx Context, key string, txns []yodlee.DataExtractsTransaction) {
	slog.InfoContext(ctx, "handling txn refresh messages", "txns", txns)

	result := IngestTxnRefreshes(ctx, key, txns)
	slog.InfoContext(ctx, "completed txn refresh ingestion", "putResults", result.PutResults, "deleteErrs", result.DeleteErrors)

	ProducePutResults(ctx, storage.TxnRefreshTopic, key, result.PutResults, nil)
	ProduceDeleteErrors(ctx, key, result.DeleteErrors)
}

func ConsumeHoldRefreshMessage(ctx Context, key string, holds []yodlee.DataExtractsHolding) {
	slog.InfoContext(ctx, "handling hold refresh messages", "holds", holds)

	result := IngestHoldRefreshes(ctx, key, holds)
	slog.InfoContext(ctx, "completed hold refresh ingestion", "putResults", result.PutResults, "deleteErrs", result.DeleteErrors)

	ProducePutResults(ctx, storage.HoldRefreshTopic, key, result.PutResults, nil)
	ProduceDeleteErrors(ctx, key, result.DeleteErrors)
}

func ConsumeCnctResponseMessage(ctx Context, key string, cncts yodlee.ProviderAccountResponse) {
	slog.InfoContext(ctx, "handling cnct response messages", "cncts", cncts)

	putResults := IngestCnctResponses(ctx, key, cncts)

	ProducePutResults(ctx, storage.CnctResponseTopic, key, putResults, func(errInputs []yodlee.ProviderAccount) any {
		return yodlee.ProviderAccountResponse{ProviderAccount: errInputs}
	})
}

func ConsumeAcctResponseMessage(ctx Context, key string, accts yodlee.AccountResponse) {
	slog.InfoContext(ctx, "handling acct response messages", "accts", accts)

	putResults := IngestAcctResponses(ctx, key, accts)

	ProducePutResults(ctx, storage.AcctResponseTopic, key, putResults, func(errInputs []yodlee.Account) any {
		return yodlee.AccountResponse{Account: errInputs}
	})
}

func ConsumeTxnResponseMessage(ctx Context, key string, txns yodlee.TransactionResponse) {
	slog.InfoContext(ctx, "handling txn response messages", "txns", txns)

	putResults := IngestTxnResponses(ctx, key, txns)

	ProducePutResults(ctx, storage.TxnResponseTopic, key, putResults, func(errInputs []yodlee.TransactionWithDateTime) any {
		return yodlee.TransactionResponse{Transaction: errInputs}
	})
}

func ConsumeHoldResponseMessage(ctx Context, key string, holds yodlee.HoldingResponse) {
	slog.InfoContext(ctx, "handling hold response messages", "holds", holds)

	putResults := IngestHoldResponses(ctx, key, holds)

	ProducePutResults(ctx, storage.HoldResponseTopic, key, putResults, func(errInputs []yodlee.Holding) any {
		return yodlee.HoldingResponse{Holding: errInputs}
	})
}

func ConsumeDeleteRetryMessage(ctx Context, key string, deleteRetries []DeleteRetry) {
	slog.InfoContext(ctx, "handling delete recovery messages", "deleteRetries", deleteRetries)

	deleteErrors := IngestDeleteRetries(ctx, deleteRetries)
	ProduceDeleteErrors(ctx, key, deleteErrors)
}

type BroadcastOutput struct {
	OriginTopic string            `json:"originTopic"`
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
