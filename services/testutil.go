package svc

import (
	"encoding/json"
	"fmt"
	"github.com/IBM/sarama"
	saramaMocks "github.com/IBM/sarama/mocks"
	"yodleeops/infra"
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
		OriginTopic infra.Topic `json:"origintopic"`
	}
	if err := json.Unmarshal(v, &brdcast); err != nil {
		return fmt.Errorf("unmarshal broadcast message: %w", err)
	}

	switch brdcast.OriginTopic {
	case infra.CnctResponseTopic:
		return decodeProduceMsg[BroadcastInput[OpsProviderAccount, yodlee.ProviderAccount]](msg)
	case infra.AcctResponseTopic:
		return decodeProduceMsg[BroadcastInput[OpsAccount, yodlee.Account]](msg)
	case infra.HoldResponseTopic:
		return decodeProduceMsg[BroadcastInput[OpsHolding, yodlee.Holding]](msg)
	case infra.TxnResponseTopic:
		return decodeProduceMsg[BroadcastInput[OpsTransaction, yodlee.TransactionWithDateTime]](msg)
	case infra.CnctRefreshTopic:
		return decodeProduceMsg[BroadcastInput[OpsProviderAccountRefresh, yodlee.DataExtractsProviderAccount]](msg)
	case infra.AcctRefreshTopic:
		return decodeProduceMsg[BroadcastInput[OpsAccountRefresh, yodlee.DataExtractsAccount]](msg)
	case infra.HoldRefreshTopic:
		return decodeProduceMsg[BroadcastInput[OpsHoldingRefresh, yodlee.DataExtractsHolding]](msg)
	case infra.TxnRefreshTopic:
		return decodeProduceMsg[BroadcastInput[OpsTransactionRefresh, yodlee.DataExtractsTransaction]](msg)
	default:
		return fmt.Sprintf("unexpected broadcast origin Topic: %s", msg.Topic)
	}
}

func decodeProducerMessage(msg *sarama.ProducerMessage) any {
	switch infra.Topic(msg.Topic) {
	case infra.CnctResponseTopic:
		return decodeProduceMsg[yodlee.ProviderAccountResponse](msg)
	case infra.AcctResponseTopic:
		return decodeProduceMsg[yodlee.AccountResponse](msg)
	case infra.HoldResponseTopic:
		return decodeProduceMsg[yodlee.HoldingResponse](msg)
	case infra.TxnResponseTopic:
		return decodeProduceMsg[yodlee.TransactionResponse](msg)
	case infra.CnctRefreshTopic:
		return decodeProduceMsg[[]yodlee.DataExtractsProviderAccount](msg)
	case infra.AcctRefreshTopic:
		return decodeProduceMsg[[]yodlee.DataExtractsAccount](msg)
	case infra.HoldRefreshTopic:
		return decodeProduceMsg[[]yodlee.DataExtractsHolding](msg)
	case infra.TxnRefreshTopic:
		return decodeProduceMsg[[]yodlee.DataExtractsTransaction](msg)
	case infra.BroadcastTopic:
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
