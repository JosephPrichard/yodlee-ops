package svc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
	"log/slog"
	"time"
)

type ConsumersConfig struct {
	Context     context.Context
	ConsumeChan chan bool
	Concurrency int
}

func (app *App) StartConsumers(cfg ConsumersConfig) {
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 1
	}
	for range cfg.Concurrency {
		go ConsumeFiMessages(cfg, app.CnctRefreshConsumer, app.HandleCnctRefreshMessage)
		go ConsumeFiMessages(cfg, app.AcctRefreshConsumer, app.HandleAcctRefreshMessage)
		go ConsumeFiMessages(cfg, app.HoldRefreshConsumer, app.HandleHoldRefreshMessage)
		go ConsumeFiMessages(cfg, app.TxnRefreshConsumer, app.HandleTxnRefreshMessage)
		go ConsumeFiMessages(cfg, app.CnctEnrichmentConsumer, app.HandleCnctRefreshMessage)
		go ConsumeFiMessages(cfg, app.AcctEnrichmentConsumer, app.HandleAcctRefreshMessage)
		go ConsumeFiMessages(cfg, app.HoldEnrichmentConsumer, app.HandleHoldRefreshMessage)
		go ConsumeFiMessages(cfg, app.TxnEnrichmentConsumer, app.HandleTxnRefreshMessage)
		go ConsumeFiMessages(cfg, app.DeleteRecoveryConsumer, app.HandleDeleteRecoveryMessage)
	}
}

func ConsumeFiMessages[Message any](cfg ConsumersConfig, reader *kafka.Reader, onMessage func(ctx context.Context, message Message)) {
	readCfg := reader.Config()
	for {
		ctx := context.WithValue(cfg.Context, "trace", uuid.NewString())

		m, err := reader.ReadMessage(ctx)
		if errors.Is(err, context.Canceled) {
			break
		} else if err != nil {
			slog.ErrorContext(ctx, "failed to fetch fi messages", "topic", readCfg.Topic, "err", err)
			continue
		}
		slog.InfoContext(ctx, "read messages from kafka topic", "topic", readCfg.Topic, "offset", m.Offset, "key", string(m.Key), "value", string(m.Value))
		start := time.Now()

		var data Message
		if err := json.Unmarshal(m.Value, &data); err != nil {
			slog.ErrorContext(ctx, "failed to unmarshal fi messages", "type", fmt.Sprintf("%T", data), "topic", readCfg.Topic, "err", err)
			continue
		}

		onMessage(ctx, data)
		slog.InfoContext(ctx, "consumed message from kafka topic", "topic", readCfg.Topic, "elapsed", time.Since(start))

		if cfg.ConsumeChan != nil {
			cfg.ConsumeChan <- true
		}
	}
}

func (app *App) HandleCnctRefreshMessage(ctx context.Context, cncts []ExtnCnctRefresh) {
	slog.InfoContext(ctx, "handling cnct refresh messages", "cncts", cncts)

	result := app.IngestCnctRefreshes(ctx, cncts)
	slog.InfoContext(ctx, "completed cnct refresh ingestion", "putErrs", result.PutErrors, "deleteErrs", result.DeleteErrors)

	app.ProducePutErrors(ctx, app.CnctRefreshTopic, result.PutErrors)
	app.ProduceDeleteErrors(ctx, result.DeleteErrors)
}

func (app *App) HandleAcctRefreshMessage(ctx context.Context, accts []ExtnAcctRefresh) {
	slog.InfoContext(ctx, "handling acct refresh messages", "accts", accts)

	result := app.IngestAcctsRefreshes(ctx, accts)
	slog.InfoContext(ctx, "completed acct refresh ingestion", "putErrs", result.PutErrors, "deleteErrs", result.DeleteErrors)

	app.ProducePutErrors(ctx, app.AcctRefreshTopic, result.PutErrors)
	app.ProduceDeleteErrors(ctx, result.DeleteErrors)
}

func (app *App) HandleTxnRefreshMessage(ctx context.Context, txns []ExtnTxnRefresh) {
	slog.InfoContext(ctx, "handling txn refresh messages", "txns", txns)

	result := app.IngestTxnRefreshes(ctx, txns)
	slog.InfoContext(ctx, "completed txn refresh ingestion", "putErrs", result.PutErrors, "deleteErrs", result.DeleteErrors)

	app.ProducePutErrors(ctx, app.TxnRefreshTopic, result.PutErrors)
	app.ProduceDeleteErrors(ctx, result.DeleteErrors)
}

func (app *App) HandleHoldRefreshMessage(ctx context.Context, holds []ExtnHoldRefresh) {
	slog.InfoContext(ctx, "handling hold refresh messages", "holds", holds)

	result := app.IngestHoldRefreshes(ctx, holds)
	slog.InfoContext(ctx, "completed hold refresh ingestion", "putErrs", result.PutErrors, "deleteErrs", result.DeleteErrors)

	app.ProducePutErrors(ctx, app.HoldRefreshTopic, result.PutErrors)
	app.ProduceDeleteErrors(ctx, result.DeleteErrors)
}

func (app *App) HandleCnctEnrichmentMessage(ctx context.Context, cncts []ExtnCnctEnrichment) {
	slog.InfoContext(ctx, "handling cnct enrichment messages", "cncts", cncts)

	putErrors := app.IngestCnctEnrichments(ctx, cncts)
	app.ProducePutErrors(ctx, app.CnctEnrichmentTopic, putErrors)
}

func (app *App) HandleAcctEnrichmentMessage(ctx context.Context, accts []ExtnAcctEnrichment) {
	slog.InfoContext(ctx, "handling acct enrichment messages", "accts", accts)

	putErrors := app.IngestAcctEnrichments(ctx, accts)
	app.ProducePutErrors(ctx, app.AcctEnrichmentTopic, putErrors)
}

func (app *App) HandleTxnEnrichmentMessage(ctx context.Context, txns []ExtnTxnEnrichment) {
	slog.InfoContext(ctx, "handling txn enrichment messages", "txns", txns)

	putErrors := app.IngestTxnEnrichments(ctx, txns)
	app.ProducePutErrors(ctx, app.TxnEnrichmentTopic, putErrors)
}

func (app *App) HandleHoldEnrichmentMessage(ctx context.Context, holds []ExtnHoldEnrichment) {
	slog.InfoContext(ctx, "handling hold enrichment messages", "holds", holds)

	putErrors := app.IngestHoldEnrichments(ctx, holds)
	app.ProducePutErrors(ctx, app.HoldEnrichmentTopic, putErrors)
}

func (app *App) HandleDeleteRecoveryMessage(ctx context.Context, deleteRetries []DeleteRetry) {
	slog.InfoContext(ctx, "handling delete recovery messages", "deleteRetries", deleteRetries)

	deleteErrors := app.IngestDeleteRetries(ctx, deleteRetries)
	app.ProduceDeleteErrors(ctx, deleteErrors)
}
