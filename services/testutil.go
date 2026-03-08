package svc

import (
	"encoding/json"
	"fmt"
	"github.com/IBM/sarama"
	saramaMocks "github.com/IBM/sarama/mocks"
	"yodleeops/model"
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
		OriginTopic model.Topic `json:"origintopic"`
	}
	if err := json.Unmarshal(v, &brdcast); err != nil {
		return fmt.Errorf("unmarshal broadcast message: %w", err)
	}

	switch brdcast.OriginTopic {
	case model.CnctResponseTopic:
		return decodeProduceMsg[BroadcastInput[OpsProviderAccount, yodlee.ProviderAccount]](msg)
	case model.AcctResponseTopic:
		return decodeProduceMsg[BroadcastInput[OpsAccount, yodlee.Account]](msg)
	case model.HoldResponseTopic:
		return decodeProduceMsg[BroadcastInput[OpsHolding, yodlee.Holding]](msg)
	case model.TxnResponseTopic:
		return decodeProduceMsg[BroadcastInput[OpsTransaction, yodlee.TransactionWithDateTime]](msg)
	case model.CnctRefreshTopic:
		return decodeProduceMsg[BroadcastInput[OpsProviderAccountRefresh, yodlee.DataExtractsProviderAccount]](msg)
	case model.AcctRefreshTopic:
		return decodeProduceMsg[BroadcastInput[OpsAccountRefresh, yodlee.DataExtractsAccount]](msg)
	case model.HoldRefreshTopic:
		return decodeProduceMsg[BroadcastInput[OpsHoldingRefresh, yodlee.DataExtractsHolding]](msg)
	case model.TxnRefreshTopic:
		return decodeProduceMsg[BroadcastInput[OpsTransactionRefresh, yodlee.DataExtractsTransaction]](msg)
	default:
		return fmt.Sprintf("unexpected broadcast origin Topic: %s", msg.Topic)
	}
}

func decodeProducerMessage(msg *sarama.ProducerMessage) any {
	switch model.Topic(msg.Topic) {
	case model.CnctResponseTopic:
		return decodeProduceMsg[yodlee.ProviderAccountResponse](msg)
	case model.AcctResponseTopic:
		return decodeProduceMsg[yodlee.AccountResponse](msg)
	case model.HoldResponseTopic:
		return decodeProduceMsg[yodlee.HoldingResponse](msg)
	case model.TxnResponseTopic:
		return decodeProduceMsg[yodlee.TransactionResponse](msg)
	case model.CnctRefreshTopic:
		return decodeProduceMsg[[]yodlee.DataExtractsProviderAccount](msg)
	case model.AcctRefreshTopic:
		return decodeProduceMsg[[]yodlee.DataExtractsAccount](msg)
	case model.HoldRefreshTopic:
		return decodeProduceMsg[[]yodlee.DataExtractsHolding](msg)
	case model.TxnRefreshTopic:
		return decodeProduceMsg[[]yodlee.DataExtractsTransaction](msg)
	case model.BroadcastTopic:
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
