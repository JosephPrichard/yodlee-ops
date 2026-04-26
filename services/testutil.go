package svc

import (
	"encoding/json"
	"fmt"
	"github.com/IBM/sarama"
	saramaMocks "github.com/IBM/sarama/mocks"
	"yodleeops/storage"

	"yodleeops/yodlee"
)

func decodeProduceMsg[JSON any](msg *sarama.ProducerMessage) any {
	v, err := msg.Value.Encode()
	if err != nil {
		return err.Error()
	}
	var value JSON
	if err := json.Unmarshal(v, &value); err != nil {
		return err.Error()
	}
	return value
}

func decodeBroadcast(msg *sarama.ProducerMessage) any {
	v, err := msg.Value.Encode()
	if err != nil {
		return err.Error()
	}
	var brdcast struct {
		OriginTopic storage.Topic `json:"origintopic"`
	}
	if err := json.Unmarshal(v, &brdcast); err != nil {
		return fmt.Errorf("unmarshal broadcast message: %w", err)
	}

	switch brdcast.OriginTopic {
	case storage.CnctResponseTopic:
		return decodeProduceMsg[BroadcastInput[OpsProviderAccount, yodlee.ProviderAccount]](msg)
	case storage.AcctResponseTopic:
		return decodeProduceMsg[BroadcastInput[OpsAccount, yodlee.Account]](msg)
	case storage.HoldResponseTopic:
		return decodeProduceMsg[BroadcastInput[OpsHolding, yodlee.Holding]](msg)
	case storage.TxnResponseTopic:
		return decodeProduceMsg[BroadcastInput[OpsTransaction, yodlee.TransactionWithDateTime]](msg)
	case storage.CnctRefreshTopic:
		return decodeProduceMsg[BroadcastInput[OpsProviderAccountRefresh, yodlee.DataExtractsProviderAccount]](msg)
	case storage.AcctRefreshTopic:
		return decodeProduceMsg[BroadcastInput[OpsAccountRefresh, yodlee.DataExtractsAccount]](msg)
	case storage.HoldRefreshTopic:
		return decodeProduceMsg[BroadcastInput[OpsHoldingRefresh, yodlee.DataExtractsHolding]](msg)
	case storage.TxnRefreshTopic:
		return decodeProduceMsg[BroadcastInput[OpsTransactionRefresh, yodlee.DataExtractsTransaction]](msg)
	default:
		return fmt.Sprintf("unexpected broadcast origin Topic: %s", msg.Topic)
	}
}

func decodeProducerMessage(msg *sarama.ProducerMessage) any {
	switch storage.Topic(msg.Topic) {
	case storage.CnctResponseTopic:
		return decodeProduceMsg[yodlee.ProviderAccountResponse](msg)
	case storage.AcctResponseTopic:
		return decodeProduceMsg[yodlee.AccountResponse](msg)
	case storage.HoldResponseTopic:
		return decodeProduceMsg[yodlee.HoldingResponse](msg)
	case storage.TxnResponseTopic:
		return decodeProduceMsg[yodlee.TransactionResponse](msg)
	case storage.CnctRefreshTopic:
		return decodeProduceMsg[[]yodlee.DataExtractsProviderAccount](msg)
	case storage.AcctRefreshTopic:
		return decodeProduceMsg[[]yodlee.DataExtractsAccount](msg)
	case storage.HoldRefreshTopic:
		return decodeProduceMsg[[]yodlee.DataExtractsHolding](msg)
	case storage.TxnRefreshTopic:
		return decodeProduceMsg[[]yodlee.DataExtractsTransaction](msg)
	case storage.BroadcastTopic:
		return decodeBroadcast(msg)
	default:
		return fmt.Sprintf("unexpected Topic: %s", msg.Topic)
	}
}

func drainMockProducerMessages(mockProducer *saramaMocks.AsyncProducer) []any {
	var messages []any

Collect:
	for {
		select {
		case msg, ok := <-mockProducer.Successes():
			if !ok {
				break Collect
			}
			messages = append(messages, decodeProducerMessage(msg))
		case err, ok := <-mockProducer.Errors():
			if !ok {
				break
			}
			messages = append(messages, fmt.Errorf("mock producer error: %+v", err))
			break Collect
		}
	}

	return messages
}
