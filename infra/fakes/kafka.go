package fakes

import (
	"context"
	"github.com/segmentio/kafka-go"
	"yodleeops/infra"
)

type KafkaMessage struct {
	Topic infra.Topic
	Key   string
	Value []byte
}

type FakeProducer struct {
	Messages []KafkaMessage
}

func (p *FakeProducer) WriteMessages(_ context.Context, msgs ...kafka.Message) error {
	for _, m := range msgs {
		p.Messages = append(p.Messages, KafkaMessage{Topic: infra.Topic(m.Topic), Key: string(m.Key), Value: m.Value})
	}
	return nil
}

func (p *FakeProducer) Close() error { return nil }
