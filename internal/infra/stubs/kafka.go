package infrastub

import (
	"context"
	"github.com/segmentio/kafka-go"
	"yodleeops/internal/infra"
)

type KafkaMessage struct {
	Topic infra.Topic
	Key   string
	Value []byte
}

type Producer struct {
	Messages []KafkaMessage
}

func (p *Producer) WriteMessages(_ context.Context, msgs ...kafka.Message) error {
	for _, m := range msgs {
		p.Messages = append(p.Messages, KafkaMessage{Topic: infra.Topic(m.Topic), Key: string(m.Key), Value: m.Value})
	}
	return nil
}

func (p *Producer) Close() error { return nil }
