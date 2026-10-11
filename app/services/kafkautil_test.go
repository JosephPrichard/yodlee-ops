package svc

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"
	"yodleeops/storage"

	"github.com/IBM/sarama"
	saramaMocks "github.com/IBM/sarama/mocks"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/slices"

	"yodleeops/yodlee"
)

type MockConsumers struct {
	t         *testing.T
	consumers []Consumer
}

func (c *MockConsumers) Consume(topic storage.Topic, key string, value any) {
	v, err := json.Marshal(value)
	require.NoError(c.t, err)

	session := &fakeSession{}
	claim := &fakeClaim{
		msgChan: make(chan *sarama.ConsumerMessage, 1),
	}

	msg := &sarama.ConsumerMessage{Key: []byte(key), Value: v}
	claim.msgChan <- msg
	close(claim.msgChan)

	consumerIndex := slices.IndexFunc(c.consumers, func(consumer Consumer) bool { return consumer.Topic() == topic })

	err = c.consumers[consumerIndex].ConsumeClaim(session, claim)
	require.NoError(c.t, err)
}

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
	var broadcast struct {
		OriginTopic storage.Topic `json:"originTopic"`
	}
	if err := json.Unmarshal(v, &broadcast); err != nil {
		return fmt.Errorf("unmarshal broadcast message: %w", err)
	}

	switch broadcast.OriginTopic {
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
		return fmt.Sprintf("decode producer message: unexpected broadcast origin topic: %s", msg.Topic)
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
	case storage.DeleteRetryTopic:
		return decodeProduceMsg[DeleteResult](msg)
	case storage.BroadcastTopic:
		return decodeBroadcast(msg)
	default:
		return fmt.Sprintf("decode producer message: unexpected topic: %s", msg.Topic)
	}
}

func ConsumeMockProducer(t *testing.T, mp *saramaMocks.AsyncProducer, count int) []any {
	ctx, cancel := context.WithTimeout(t.Context(), time.Second*2)
	defer cancel()

	var messages []any

	for range count {
		select {
		case m := <-mp.Successes():
			messages = append(messages, decodeProducerMessage(m))
		case <-ctx.Done():
			t.Fatalf("timed out while draining mock producer messages: %v", ctx.Err())
		}
	}

	return messages
}
