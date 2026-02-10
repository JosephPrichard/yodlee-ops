package svc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"log/slog"
	"time"
	"yodleeops/infra"
	"yodleeops/internal/yodlee"
)

type ConsumersConfig struct {
	Context     context.Context
	Concurrency int
}

func (app *App) StartConsumers(cfg ConsumersConfig) {
	if cfg.Context == nil {
		cfg.Context = context.Background()
	}
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 1
	}
	for range cfg.Concurrency {
		go ConsumeFiMessages(cfg, app.CnctRefreshConsumer, app.HandleCnctRefreshMessage)
		go ConsumeFiMessages(cfg, app.AcctRefreshConsumer, app.HandleAcctRefreshMessage)
		go ConsumeFiMessages(cfg, app.HoldRefreshConsumer, app.HandleHoldRefreshMessage)
		go ConsumeFiMessages(cfg, app.TxnRefreshConsumer, app.HandleTxnRefreshMessage)
		go ConsumeFiMessages(cfg, app.CnctResponseConsumer, app.HandleAcctResponseMessage)
		go ConsumeFiMessages(cfg, app.AcctResponseConsumer, app.HandleAcctResponseMessage)
		go ConsumeFiMessages(cfg, app.HoldResponseConsumer, app.HandleHoldResponseMessage)
		go ConsumeFiMessages(cfg, app.TxnResponseConsumer, app.HandleTxnResponseMessage)
		go ConsumeFiMessages(cfg, app.DeleteRetryConsumer, app.HandleDeleteRecoveryMessage)
		go ConsumeFiMessages(cfg, app.BroadcastConsumer, app.HandleBroadcastMessage)
	}

	slog.Info("started consumers", "config", cfg)
}

func ConsumeFiMessages[Message any](cfg ConsumersConfig, reader infra.Consumer, onMessage func(ctx context.Context, key string, message Message)) {
	count := 0
	readCfg := reader.Config()
	for {
		ctx := context.WithValue(cfg.Context, "trace", uuid.NewString())

		count++
		m, err := reader.ReadMessage(cfg.Context) // config allows cancel.
		if errors.Is(err, context.Canceled) {
			break
		} else if err != nil {
			slog.ErrorContext(ctx, "failed to fetch fi messages", "Topic", readCfg.Topic, "err", err)
			continue
		}

		slog.InfoContext(ctx, "read messages from kafka Topic", "count", count, "Topic", readCfg.Topic, "offset", m.Offset, "Key", string(m.Key), "value", string(m.Value))
		start := time.Now()

		var data Message
		if err := json.Unmarshal(m.Value, &data); err != nil {
			slog.ErrorContext(ctx, "failed to unmarshal fi messages", "type", fmt.Sprintf("%T", data), "Topic", readCfg.Topic, "err", err)
			continue
		}

		onMessage(ctx, string(m.Key), data)
		slog.InfoContext(ctx, "consumed message from kafka Topic", "count", count, "Topic", readCfg.Topic, "elapsed", time.Since(start))
	}
}

func collectPutResults[Wrap YodleeWrapper[Inner], Inner any](putResults []PutResult[Wrap]) ([]Wrap, []Inner) {
	var successInputs []Wrap
	var errInputs []Inner
	for _, putErr := range putResults {
		if putErr.Err != nil {
			errInputs = append(errInputs, putErr.Input.Inner())
		} else {
			successInputs = append(successInputs, putErr.Input)
		}
	}
	return successInputs, errInputs
}

type CompleteRefreshEvent[Wrap YodleeWrapper[Inner], Inner any] struct {
	Ctx    context.Context
	App    *App
	Topic  string
	Key    string
	Result RefreshResult[Wrap]
}

func MakeCompleteRefreshEvent[Wrap YodleeWrapper[Inner], Inner any](
	ctx context.Context,
	app *App,
	topic string,
	key string,
	result RefreshResult[Wrap],
) CompleteRefreshEvent[Wrap, Inner] {
	return CompleteRefreshEvent[Wrap, Inner]{ctx, app, topic, key, result}
}

func (e CompleteRefreshEvent[Wrap, Inner]) Process() {
	successInputs, errInputs := collectPutResults(e.Result.PutResults)

	if len(successInputs) > 0 {
		e.App.ProduceJsonMessage(e.Ctx, infra.BroadcastTopic, "", successInputs)
	}
	if len(errInputs) > 0 {
		e.App.ProduceJsonMessage(e.Ctx, e.Topic, e.Key, errInputs)
	}
	e.App.ProduceDeleteErrors(e.Ctx, e.Key, e.Result.DeleteErrors)
}

func (app *App) HandleCnctRefreshMessage(ctx context.Context, key string, cncts []yodlee.DataExtractsProviderAccount) {
	slog.InfoContext(ctx, "handling cnct refresh messages", "cncts", cncts)

	result := app.IngestCnctRefreshes(ctx, key, cncts)
	slog.InfoContext(ctx, "completed cnct refresh ingestion", "putResults", result.PutResults, "deleteErrs", result.DeleteErrors)

	MakeCompleteRefreshEvent(ctx, app, infra.CnctRefreshTopic, key, result).Process()
}

func (app *App) HandleAcctRefreshMessage(ctx context.Context, key string, accts []yodlee.DataExtractsAccount) {
	slog.InfoContext(ctx, "handling acct refresh messages", "accts", accts)

	result := app.IngestAcctsRefreshes(ctx, key, accts)
	slog.InfoContext(ctx, "completed acct refresh ingestion", "putResults", result.PutResults, "deleteErrs", result.DeleteErrors)

	MakeCompleteRefreshEvent(ctx, app, infra.AcctRefreshTopic, key, result).Process()
}

func (app *App) HandleTxnRefreshMessage(ctx context.Context, key string, txns []yodlee.DataExtractsTransaction) {
	slog.InfoContext(ctx, "handling txn refresh messages", "txns", txns)

	result := app.IngestTxnRefreshes(ctx, key, txns)
	slog.InfoContext(ctx, "completed txn refresh ingestion", "putResults", result.PutResults, "deleteErrs", result.DeleteErrors)

	MakeCompleteRefreshEvent(ctx, app, infra.TxnRefreshTopic, key, result).Process()
}

func (app *App) HandleHoldRefreshMessage(ctx context.Context, key string, holds []yodlee.DataExtractsHolding) {
	slog.InfoContext(ctx, "handling hold refresh messages", "holds", holds)

	result := app.IngestHoldRefreshes(ctx, key, holds)
	slog.InfoContext(ctx, "completed hold refresh ingestion", "putResults", result.PutResults, "deleteErrs", result.DeleteErrors)

	MakeCompleteRefreshEvent(ctx, app, infra.HoldRefreshTopic, key, result).Process()
}

func (app *App) HandleCnctResponseMessage(ctx context.Context, key string, cncts yodlee.ProviderAccountResponse) {
	slog.InfoContext(ctx, "handling cnct response messages", "cncts", cncts)

	putResults := app.IngestCnctResponses(ctx, key, cncts)

	successInputs, errInputs := collectPutResults(putResults)

	if len(successInputs) > 0 {
		app.ProduceJsonMessage(ctx, infra.BroadcastTopic, "", successInputs)
	}
	if len(errInputs) > 0 {
		nextInput := yodlee.ProviderAccountResponse{ProviderAccount: errInputs}
		app.ProduceJsonMessage(ctx, infra.CnctResponseTopic, key, nextInput)
	}
}

func (app *App) HandleAcctResponseMessage(ctx context.Context, key string, accts yodlee.AccountResponse) {
	slog.InfoContext(ctx, "handling acct response messages", "accts", accts)

	putResults := app.IngestAcctResponses(ctx, key, accts)

	successInputs, errInputs := collectPutResults(putResults)

	if len(successInputs) > 0 {
		app.ProduceJsonMessage(ctx, infra.BroadcastTopic, "", successInputs)
	}
	if len(errInputs) > 0 {
		nextInput := yodlee.AccountResponse{Account: errInputs}
		app.ProduceJsonMessage(ctx, infra.AcctResponseTopic, key, nextInput)
	}
}

func (app *App) HandleTxnResponseMessage(ctx context.Context, key string, txns yodlee.TransactionResponse) {
	slog.InfoContext(ctx, "handling txn response messages", "txns", txns)

	putResults := app.IngestTxnResponses(ctx, key, txns)

	successInputs, errInputs := collectPutResults(putResults)

	if len(successInputs) > 0 {
		app.ProduceJsonMessage(ctx, infra.BroadcastTopic, "", successInputs)
	}
	if len(errInputs) > 0 {
		nextInput := yodlee.TransactionResponse{Transaction: errInputs}
		app.ProduceJsonMessage(ctx, infra.TxnResponseTopic, key, nextInput)
	}
}

func (app *App) HandleHoldResponseMessage(ctx context.Context, key string, holds yodlee.HoldingResponse) {
	slog.InfoContext(ctx, "handling hold response messages", "holds", holds)

	putResults := app.IngestHoldResponses(ctx, key, holds)

	successInputs, errInputs := collectPutResults(putResults)

	if len(successInputs) > 0 {
		app.ProduceJsonMessage(ctx, infra.BroadcastTopic, "", successInputs)
	}
	if len(errInputs) > 0 {
		nextInput := yodlee.HoldingResponse{Holding: errInputs}
		app.ProduceJsonMessage(ctx, infra.HoldResponseTopic, key, nextInput)
	}
}

func (app *App) HandleDeleteRecoveryMessage(ctx context.Context, key string, deleteRetries []DeleteRetry) {
	slog.InfoContext(ctx, "handling delete recovery messages", "deleteRetries", deleteRetries)

	deleteErrors := app.IngestDeleteRetries(ctx, deleteRetries)
	app.ProduceDeleteErrors(ctx, key, deleteErrors)
}

func (app *App) HandleBroadcastMessage(ctx context.Context, _ string, broadcasts []json.RawMessage) {
	for _, brd := range broadcasts {
		slog.InfoContext(ctx, "broadcasting message", "message", string(brd))
		app.Broadcaster.Broadcast(brd)
	}
}
