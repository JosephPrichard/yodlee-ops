package svc

import (
	"context"
	"encoding/json"
	"errors"
	"filogger"
	"fmt"
	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
	"log/slog"
	"time"
)

type Consumers struct {
	ErrorLogConsumer       *kafka.Reader
	CnctEnrichmentConsumer *kafka.Reader
	AcctEnrichmentConsumer *kafka.Reader
	HoldEnrichmentConsumer *kafka.Reader
	TxnEnrichmentConsumer  *kafka.Reader
	CnctRefreshConsumer    *kafka.Reader
	AcctRefreshConsumer    *kafka.Reader
	HoldRefreshConsumer    *kafka.Reader
	TxnRefreshConsumer     *kafka.Reader
}

func (c *Consumers) Close() error {
	return errors.Join(
		closeConsumer(c.ErrorLogConsumer),
		closeConsumer(c.CnctEnrichmentConsumer),
		closeConsumer(c.AcctEnrichmentConsumer),
		closeConsumer(c.HoldEnrichmentConsumer),
		closeConsumer(c.TxnEnrichmentConsumer),
		closeConsumer(c.CnctRefreshConsumer),
		closeConsumer(c.AcctRefreshConsumer),
		closeConsumer(c.HoldRefreshConsumer),
		closeConsumer(c.TxnRefreshConsumer),
	)
}

func closeConsumer(r *kafka.Reader) error {
	if r == nil {
		return nil
	}
	return r.Close()
}

func MakeConsumers(config flog.Config) Consumers {
	makeReader := func(topic string) *kafka.Reader {
		return kafka.NewReader(kafka.ReaderConfig{
			Brokers:        config.KafkaBrokers,
			GroupID:        config.GroupID,
			Topic:          topic,
			CommitInterval: time.Second, // auto-commit
		})
	}

	return Consumers{
		ErrorLogConsumer:       makeReader(config.ErrorLogTopic),
		CnctEnrichmentConsumer: makeReader(config.CnctEnrichmentTopic),
		AcctEnrichmentConsumer: makeReader(config.AcctEnrichmentTopic),
		TxnEnrichmentConsumer:  makeReader(config.TxnEnrichmentTopic),
		HoldEnrichmentConsumer: makeReader(config.HoldEnrichmentTopic),
		CnctRefreshConsumer:    makeReader(config.CnctRefreshTopic),
		AcctRefreshConsumer:    makeReader(config.AcctRefreshTopic),
		HoldRefreshConsumer:    makeReader(config.HoldRefreshTopic),
		TxnRefreshConsumer:     makeReader(config.TxnRefreshTopic),
	}
}

func ConsumeFiMessages[Message any](reader *kafka.Reader, onMessage func(ctx context.Context, message Message)) {
	cfg := reader.Config()
	for {
		ctx := context.WithValue(context.Background(), "trace", uuid.NewString())

		m, err := reader.ReadMessage(ctx)
		if err != nil {
			slog.ErrorContext(ctx, "failed to fetch fi message", "topic", cfg.Topic, "err", err)
			break
		}
		slog.InfoContext(ctx, "read message from kafka topic", "topic", cfg.Topic, "offset", m.Offset, "key", string(m.Key), "value", string(m.Value))

		var data Message
		if err := json.Unmarshal(m.Value, &data); err != nil {
			slog.ErrorContext(ctx, "failed to unmarshal fi message", "type", fmt.Sprintf("%T", data), "topic", cfg.Topic, "err", err)
			continue
		}

		onMessage(ctx, data)
	}
}

func ConsumeErrorLogs() {
	topic := app.ErrorLogTopic
	for {
		ctx := context.WithValue(context.Background(), "trace", uuid.NewString())

		m, err := app.ErrorLogConsumer.FetchMessage(ctx)
		if err != nil {
			slog.ErrorContext(ctx, "failed to fetch message", "topic", topic, "err", err)
			break
		}
		slog.InfoContext(ctx, "read message from kafka topic", "topic", topic, "offset", m.Offset, "key", string(m.Key), "value", string(m.Value))

		var data ErrorLog
		err = json.Unmarshal(m.Value, &data)

		if err != nil {
			// log the error and commit. a message that cannot be unmarshalled is malformed.
			slog.ErrorContext(ctx, "failed to unmarshal error log message", "topic", topic, "err", err)
		} else {
			if err := IngestErrorLog(ctx, data); err != nil {
				// log the error and skip without commiting. the message will be re consumed until ingestion is successful.
				slog.ErrorContext(ctx, "failed to ingest error log message", "topic", topic, "err", err)
				continue
			}
		}
		if err := app.ErrorLogConsumer.CommitMessages(ctx, m); err != nil {
			slog.ErrorContext(ctx, "failed to commit message", "topic", topic, "err", err)
		}
	}
}

func ConsumeCnctRefreshes() {
	ConsumeFiMessages(
		app.CnctRefreshConsumer,
		func(ctx context.Context, cncts []ExtnCnctRefresh) {
			ProducePutErrors(ctx, app.CnctRefreshTopic, IngestCnctRefreshes(ctx, cncts))
		},
	)
}

func ConsumeAcctRefreshes() {
	ConsumeFiMessages(
		app.AcctRefreshConsumer,
		func(ctx context.Context, accts []ExtnAcctRefresh) {
			ProducePutErrors(ctx, app.AcctRefreshTopic, IngestAcctsRefreshes(ctx, accts))
		},
	)
}

func ConsumeTxnRefreshes() {
	ConsumeFiMessages(
		app.TxnRefreshConsumer,
		func(ctx context.Context, txns []ExtnTxnRefresh) {
			ProducePutErrors(ctx, app.TxnRefreshTopic, IngestTxnRefreshes(ctx, txns))
		},
	)
}

func ConsumeHoldRefreshes() {
	ConsumeFiMessages(
		app.HoldRefreshConsumer,
		func(ctx context.Context, holds []ExtnHoldRefresh) {
			ProducePutErrors(ctx, app.HoldRefreshTopic, IngestHoldRefreshes(ctx, holds))
		},
	)
}

func ConsumeCnctEnrichments() {
	ConsumeFiMessages(
		app.CnctEnrichmentConsumer,
		func(ctx context.Context, cncts []ExtnCnctEnrichment) {
			ProducePutErrors(ctx, app.CnctEnrichmentTopic, IngestCnctEnrichments(ctx, cncts))
		},
	)
}

func ConsumeAcctEnrichments() {
	ConsumeFiMessages(
		app.AcctEnrichmentConsumer,
		func(ctx context.Context, accts []ExtnAcctEnrichment) {
			ProducePutErrors(ctx, app.AcctEnrichmentTopic, IngestAcctEnrichments(ctx, accts))
		},
	)
}

func ConsumeTxnEnrichments() {
	ConsumeFiMessages(
		app.TxnEnrichmentConsumer,
		func(ctx context.Context, txns []ExtnTxnEnrichment) {
			ProducePutErrors(ctx, app.TxnEnrichmentTopic, IngestTxnEnrichments(ctx, txns))
		},
	)
}

func ConsumeHoldEnrichments() {
	ConsumeFiMessages(
		app.HoldEnrichmentConsumer,
		func(ctx context.Context, holds []ExtnHoldEnrichment) {
			ProducePutErrors(ctx, app.HoldEnrichmentTopic, IngestHoldEnrichments(ctx, holds))
		},
	)
}
