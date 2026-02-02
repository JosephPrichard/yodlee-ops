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

func (app *App) HandleCnctRefreshMessage(ctx context.Context, cncts []ExtnCnctRefresh) {
	slog.InfoContext(ctx, "handling cnct refresh message", "cncts", cncts)

	result := IngestCnctRefreshes(ctx, app, cncts)
	slog.InfoContext(ctx, "completed cnct refresh ingestion", "putErrs", result.PutErrs, "deleteErrs", result.DeleteErrs)

	ProducePutErrors(ctx, app, app.CnctRefreshTopic, result.PutErrs)
}

func (app *App) HandleAcctRefreshMessage(ctx context.Context, accts []ExtnAcctRefresh) {
	slog.InfoContext(ctx, "handling acct refresh message", "accts", accts)

	result := IngestAcctsRefreshes(ctx, app, accts)
	slog.InfoContext(ctx, "completed acct refresh ingestion", "putErrs", result.PutErrs, "deleteErrs", result.DeleteErrs)

	ProducePutErrors(ctx, app, app.AcctRefreshTopic, result.PutErrs)
}

func (app *App) HandleTxnRefreshMessage(ctx context.Context, txns []ExtnTxnRefresh) {
	slog.InfoContext(ctx, "handling txn refresh message", "txns", txns)

	result := IngestTxnRefreshes(ctx, app, txns)
	slog.InfoContext(ctx, "completed txn refresh ingestion", "putErrs", result.PutErrs, "deleteErrs", result.DeleteErrs)

	ProducePutErrors(ctx, app, app.TxnRefreshTopic, result.PutErrs)
}

func (app *App) HandleHoldRefreshMessage(ctx context.Context, holds []ExtnHoldRefresh) {
	slog.InfoContext(ctx, "handling hold refresh message", "holds", holds)

	result := IngestHoldRefreshes(ctx, app, holds)
	slog.InfoContext(ctx, "completed hold refresh ingestion", "putErrs", result.PutErrs, "deleteErrs", result.DeleteErrs)

	ProducePutErrors(ctx, app, app.HoldRefreshTopic, result.PutErrs)
}

func (app *App) HandleCnctEnrichmentMessage(ctx context.Context, cncts []ExtnCnctEnrichment) {
	slog.InfoContext(ctx, "handling cnct enrichment message", "cncts", cncts)

	putErrors := IngestCnctEnrichments(ctx, app, cncts)
	ProducePutErrors(ctx, app, app.CnctEnrichmentTopic, putErrors)
}

func (app *App) HandleAcctEnrichmentMessage(ctx context.Context, accts []ExtnAcctEnrichment) {
	slog.InfoContext(ctx, "handling acct enrichment message", "accts", accts)

	putErrors := IngestAcctEnrichments(ctx, app, accts)
	ProducePutErrors(ctx, app, app.AcctEnrichmentTopic, putErrors)
}

func (app *App) HandleTxnEnrichmentMessage(ctx context.Context, txns []ExtnTxnEnrichment) {
	slog.InfoContext(ctx, "handling txn enrichment message", "txns", txns)

	putErrors := IngestTxnEnrichments(ctx, app, txns)
	ProducePutErrors(ctx, app, app.TxnEnrichmentTopic, putErrors)
}

func (app *App) HandleHoldEnrichmentMessage(ctx context.Context, holds []ExtnHoldEnrichment) {
	slog.InfoContext(ctx, "handling hold enrichment message", "holds", holds)

	putErrors := IngestHoldEnrichments(ctx, app, holds)
	ProducePutErrors(ctx, app, app.HoldEnrichmentTopic, putErrors)
}
