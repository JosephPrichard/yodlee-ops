package svc

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
	"log/slog"
)

func ConsumeFiMessages[Message any](reader *kafka.Reader, onMessage func(ctx context.Context, message Message) []PutInput) {
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

	result := app.IngestCnctRefreshes(ctx, cncts)
	slog.InfoContext(ctx, "completed cnct refresh ingestion", "putErrs", result.PutErrs, "deleteErrs", result.DeleteErrs)

	app.ProducePutErrors(ctx, app.CnctRefreshTopic, result.PutErrs)
	app.ProduceDeleteErrors(ctx, result.DeleteErrs)
}

func (app *App) HandleAcctRefreshMessage(ctx context.Context, accts []ExtnAcctRefresh) {
	slog.InfoContext(ctx, "handling acct refresh message", "accts", accts)

	result := app.IngestAcctsRefreshes(ctx, accts)
	slog.InfoContext(ctx, "completed acct refresh ingestion", "putErrs", result.PutErrs, "deleteErrs", result.DeleteErrs)

	app.ProducePutErrors(ctx, app.AcctRefreshTopic, result.PutErrs)
	app.ProduceDeleteErrors(ctx, result.DeleteErrs)
}

func (app *App) HandleTxnRefreshMessage(ctx context.Context, txns []ExtnTxnRefresh) {
	slog.InfoContext(ctx, "handling txn refresh message", "txns", txns)

	result := app.IngestTxnRefreshes(ctx, txns)
	slog.InfoContext(ctx, "completed txn refresh ingestion", "putErrs", result.PutErrs, "deleteErrs", result.DeleteErrs)

	app.ProducePutErrors(ctx, app.TxnRefreshTopic, result.PutErrs)
	app.ProduceDeleteErrors(ctx, result.DeleteErrs)
}

func (app *App) HandleHoldRefreshMessage(ctx context.Context, holds []ExtnHoldRefresh) {
	slog.InfoContext(ctx, "handling hold refresh message", "holds", holds)

	result := app.IngestHoldRefreshes(ctx, holds)
	slog.InfoContext(ctx, "completed hold refresh ingestion", "putErrs", result.PutErrs, "deleteErrs", result.DeleteErrs)

	app.ProducePutErrors(ctx, app.HoldRefreshTopic, result.PutErrs)
	app.ProduceDeleteErrors(ctx, result.DeleteErrs)
}

func (app *App) HandleCnctEnrichmentMessage(ctx context.Context, cncts []ExtnCnctEnrichment) {
	slog.InfoContext(ctx, "handling cnct enrichment message", "cncts", cncts)

	putErrors := app.IngestCnctEnrichments(ctx, cncts)
	app.ProducePutErrors(ctx, app.CnctEnrichmentTopic, putErrors)
}

func (app *App) HandleAcctEnrichmentMessage(ctx context.Context, accts []ExtnAcctEnrichment) {
	slog.InfoContext(ctx, "handling acct enrichment message", "accts", accts)

	putErrors := app.IngestAcctEnrichments(ctx, accts)
	app.ProducePutErrors(ctx, app.AcctEnrichmentTopic, putErrors)
}

func (app *App) HandleTxnEnrichmentMessage(ctx context.Context, txns []ExtnTxnEnrichment) {
	slog.InfoContext(ctx, "handling txn enrichment message", "txns", txns)

	putErrors := app.IngestTxnEnrichments(ctx, txns)
	app.ProducePutErrors(ctx, app.TxnEnrichmentTopic, putErrors)
}

func (app *App) HandleHoldEnrichmentMessage(ctx context.Context, holds []ExtnHoldEnrichment) {
	slog.InfoContext(ctx, "handling hold enrichment message", "holds", holds)

	putErrors := app.IngestHoldEnrichments(ctx, holds)
	app.ProducePutErrors(ctx, app.HoldEnrichmentTopic, putErrors)
}
