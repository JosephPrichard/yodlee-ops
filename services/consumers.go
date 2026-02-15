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
			slog.ErrorContext(ctx, "failed to fetch message", "topic", readCfg.Topic, "err", err)
			continue
		}
		slog.InfoContext(ctx, "read message from kafka topic", "count", count, "topic", readCfg.Topic, "offset", m.Offset, "Key", string(m.Key), "value", string(m.Value))
		start := time.Now()

		var data Message
		if err := json.Unmarshal(m.Value, &data); err != nil {
			slog.ErrorContext(ctx, "failed to unmarshal message", "type", fmt.Sprintf("%T", data), "topic", readCfg.Topic, "err", err)
			continue
		}
		onMessage(ctx, string(m.Key), data)
		slog.InfoContext(ctx, "consumed message from kafka topic", "count", count, "topic", readCfg.Topic, "elapsed", time.Since(start))
	}
}

type PostPublishEvent[Wrap YodleeWrapper[Inner], Inner any] struct {
	Ctx        context.Context
	App        *App
	Topic      string
	Key        string
	PutResults []PutResult[Wrap]
}

func MakePostPublishEvent[Wrap YodleeWrapper[Inner], Inner any](
	ctx context.Context,
	app *App,
	topic string,
	key string,
	putResults []PutResult[Wrap],
) PostPublishEvent[Wrap, Inner] {
	return PostPublishEvent[Wrap, Inner]{ctx, app, topic, key, putResults}
}

type BroadcastInput[Wrap YodleeWrapper[Inner], Inner any] struct {
	// content of the fi messages, data extracts, response, etc.
	FiMessages []Wrap `json:"messages"`
	// the topic that a broadcast to be sent *originally* comes from
	// if we receive an input on `CnctRefreshTopic` and then need to broadcast the data after success upload, this value will be `CnctRefreshTopic`
	OriginTopic string `json:"origintopic"`
}

func (e PostPublishEvent[Wrap, Inner]) Process(mapInputs func([]Inner) any) {
	var successUploads []Wrap
	var errInputs []Inner

	for _, putErr := range e.PutResults {
		if putErr.Err == nil {
			successUploads = append(successUploads, putErr.Input)
		} else {
			errInputs = append(errInputs, putErr.Input.Inner())
		}
	}

	if len(successUploads) > 0 {
		// write success uploads with a small wrapper describing the topic the upload originally came from (for broadcasting).
		e.App.ProduceJsonMessage(e.Ctx, infra.BroadcastTopic, "", BroadcastInput[Wrap, Inner]{
			FiMessages:  successUploads,
			OriginTopic: e.Topic,
		})
	}
	if len(errInputs) > 0 {
		// maps the failed inputs into a new datastructure or writes as is.
		// mapInputs guarantees that `writeErrInputs` is the same type that `e.OriginTopic` expects.
		var writeErrInputs any = errInputs
		if mapInputs != nil {
			writeErrInputs = mapInputs(errInputs)
		}
		e.App.ProduceJsonMessage(e.Ctx, e.Topic, e.Key, writeErrInputs)
	}
}

func (app *App) HandleCnctRefreshMessage(ctx context.Context, key string, cncts []yodlee.DataExtractsProviderAccount) {
	slog.InfoContext(ctx, "handling cnct refresh messages", "cncts", cncts)

	result := app.IngestCnctRefreshes(ctx, key, cncts)
	slog.InfoContext(ctx, "completed cnct refresh ingestion", "putResults", result.PutResults, "deleteErrs", result.DeleteErrors)

	MakePostPublishEvent(ctx, app, infra.CnctRefreshTopic, key, result.PutResults).Process(nil)
	app.ProduceDeleteErrors(ctx, key, result.DeleteErrors)
}

func (app *App) HandleAcctRefreshMessage(ctx context.Context, key string, accts []yodlee.DataExtractsAccount) {
	slog.InfoContext(ctx, "handling acct refresh messages", "accts", accts)

	result := app.IngestAcctsRefreshes(ctx, key, accts)
	slog.InfoContext(ctx, "completed acct refresh ingestion", "putResults", result.PutResults, "deleteErrs", result.DeleteErrors)

	MakePostPublishEvent(ctx, app, infra.AcctRefreshTopic, key, result.PutResults).Process(nil)
	app.ProduceDeleteErrors(ctx, key, result.DeleteErrors)
}

func (app *App) HandleTxnRefreshMessage(ctx context.Context, key string, txns []yodlee.DataExtractsTransaction) {
	slog.InfoContext(ctx, "handling txn refresh messages", "txns", txns)

	result := app.IngestTxnRefreshes(ctx, key, txns)
	slog.InfoContext(ctx, "completed txn refresh ingestion", "putResults", result.PutResults, "deleteErrs", result.DeleteErrors)

	MakePostPublishEvent(ctx, app, infra.TxnRefreshTopic, key, result.PutResults).Process(nil)
	app.ProduceDeleteErrors(ctx, key, result.DeleteErrors)
}

func (app *App) HandleHoldRefreshMessage(ctx context.Context, key string, holds []yodlee.DataExtractsHolding) {
	slog.InfoContext(ctx, "handling hold refresh messages", "holds", holds)

	result := app.IngestHoldRefreshes(ctx, key, holds)
	slog.InfoContext(ctx, "completed hold refresh ingestion", "putResults", result.PutResults, "deleteErrs", result.DeleteErrors)

	MakePostPublishEvent(ctx, app, infra.HoldRefreshTopic, key, result.PutResults).Process(nil)
	app.ProduceDeleteErrors(ctx, key, result.DeleteErrors)
}

func (app *App) HandleCnctResponseMessage(ctx context.Context, key string, cncts yodlee.ProviderAccountResponse) {
	slog.InfoContext(ctx, "handling cnct response messages", "cncts", cncts)

	putResults := app.IngestCnctResponses(ctx, key, cncts)

	MakePostPublishEvent(ctx, app, infra.CnctResponseTopic, key, putResults).Process(func(errInputs []yodlee.ProviderAccount) any {
		return yodlee.ProviderAccountResponse{ProviderAccount: errInputs}
	})
}

func (app *App) HandleAcctResponseMessage(ctx context.Context, key string, accts yodlee.AccountResponse) {
	slog.InfoContext(ctx, "handling acct response messages", "accts", accts)

	putResults := app.IngestAcctResponses(ctx, key, accts)

	MakePostPublishEvent(ctx, app, infra.AcctResponseTopic, key, putResults).Process(func(errInputs []yodlee.Account) any {
		return yodlee.AccountResponse{Account: errInputs}
	})
}

func (app *App) HandleTxnResponseMessage(ctx context.Context, key string, txns yodlee.TransactionResponse) {
	slog.InfoContext(ctx, "handling txn response messages", "txns", txns)

	putResults := app.IngestTxnResponses(ctx, key, txns)

	MakePostPublishEvent(ctx, app, infra.TxnResponseTopic, key, putResults).Process(func(errInputs []yodlee.TransactionWithDateTime) any {
		return yodlee.TransactionResponse{Transaction: errInputs}
	})
}

func (app *App) HandleHoldResponseMessage(ctx context.Context, key string, holds yodlee.HoldingResponse) {
	slog.InfoContext(ctx, "handling hold response messages", "holds", holds)

	putResults := app.IngestHoldResponses(ctx, key, holds)

	MakePostPublishEvent(ctx, app, infra.HoldResponseTopic, key, putResults).Process(func(errInputs []yodlee.Holding) any {
		return yodlee.HoldingResponse{Holding: errInputs}
	})
}

func (app *App) HandleDeleteRecoveryMessage(ctx context.Context, key string, deleteRetries []DeleteRetry) {
	slog.InfoContext(ctx, "handling delete recovery messages", "deleteRetries", deleteRetries)

	deleteErrors := app.IngestDeleteRetries(ctx, deleteRetries)
	app.ProduceDeleteErrors(ctx, key, deleteErrors)
}

type BroadcastOutput struct {
	OriginTopic string            `json:"origintopic"`
	FiMessages  []json.RawMessage `json:"messages"`
}

func (app *App) HandleBroadcastMessage(ctx context.Context, _ string, broadcast BroadcastOutput) {
	for _, msg := range broadcast.FiMessages {
		var partial OpsFiMessage
		if err := json.Unmarshal(msg, &partial); err != nil {
			slog.ErrorContext(ctx, "failed to unmarshal broadcast message partial", "err", err)
			continue
		}
		strMsg := string(msg)
		slog.InfoContext(ctx, "broadcasting message", "topic", broadcast.OriginTopic, "message", strMsg)
		app.FiMessageBroadcaster.Broadcast(partial.ProfileId, broadcast.OriginTopic, strMsg)
	}
}
