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
	"yodleeops/yodlee"
)

type ConsumersConfig struct {
	App         *App
	Context     context.Context
	Concurrency int
}

func StartConsumers(ctx context.Context, app *App, concurrency int) {
	if ctx == nil {
		ctx = context.Background()
	}
	appCtx := Context{App: app, Context: ctx}
	if concurrency <= 0 {
		concurrency = 1
	}
	for range concurrency {
		go ConsumeFiMessages(appCtx, appCtx.CnctRefreshConsumer, HandleCnctRefreshMessage)
		go ConsumeFiMessages(appCtx, appCtx.AcctRefreshConsumer, HandleAcctRefreshMessage)
		go ConsumeFiMessages(appCtx, appCtx.HoldRefreshConsumer, HandleHoldRefreshMessage)
		go ConsumeFiMessages(appCtx, appCtx.TxnRefreshConsumer, HandleTxnRefreshMessage)
		go ConsumeFiMessages(appCtx, appCtx.CnctResponseConsumer, HandleAcctResponseMessage)
		go ConsumeFiMessages(appCtx, appCtx.AcctResponseConsumer, HandleAcctResponseMessage)
		go ConsumeFiMessages(appCtx, appCtx.HoldResponseConsumer, HandleHoldResponseMessage)
		go ConsumeFiMessages(appCtx, appCtx.TxnResponseConsumer, HandleTxnResponseMessage)
		go ConsumeFiMessages(appCtx, appCtx.DeleteRetryConsumer, HandleDeleteRecoveryMessage)
		go ConsumeFiMessages(appCtx, appCtx.BroadcastConsumer, HandleBroadcastMessage)
	}

	slog.Info("started consumers", "config", appCtx)
}

func ConsumeFiMessages[Message any](appCtx Context, reader infra.Consumer, onMessage func(Context, string, Message)) {
	count := 0
	readCfg := reader.Config()
	for {
		ctx := context.WithValue(appCtx, "trace", uuid.NewString())

		count++
		m, err := reader.ReadMessage(ctx) // config allows cancel.
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
		onMessage(appCtx, string(m.Key), data)
		slog.InfoContext(ctx, "consumed message from kafka topic", "count", count, "topic", readCfg.Topic, "elapsed", time.Since(start))
	}
}

type BroadcastInput[Wrap YodleeWrapper[Inner], Inner any] struct {
	// content of the fi messages, data extracts, response, etc.
	FiMessages  []Wrap      `json:"messages"`
	OriginTopic infra.Topic `json:"originTopic"`
}

func HandleAfterPublish[Wrap YodleeWrapper[Inner], Inner any](
	ctx Context,
	topic infra.Topic,
	key string,
	putResults []PutResult[Wrap],
	mapInputs func([]Inner) any,
) {
	var successUploads []Wrap
	var errInputs []Inner

	for _, putErr := range putResults {
		if putErr.Err == nil {
			successUploads = append(successUploads, putErr.Input)
		} else {
			errInputs = append(errInputs, putErr.Input.Inner())
		}
	}

	if len(successUploads) > 0 {
		// write success uploads with a small wrapper describing the topic the upload originally came from (for broadcasting).
		ProduceJsonMessage(ctx, infra.BroadcastTopic, "", BroadcastInput[Wrap, Inner]{
			FiMessages:  successUploads,
			OriginTopic: topic,
		})
	}
	if len(errInputs) > 0 {
		// maps the failed inputs into a new datastructure or writes as is.
		// mapInputs guarantees that `writeErrInputs` is the same type that `e.OriginTopic` expects.
		var writeErrInputs any = errInputs
		if mapInputs != nil {
			writeErrInputs = mapInputs(errInputs)
		}
		ProduceJsonMessage(ctx, topic, key, writeErrInputs)
	}
}

func HandleCnctRefreshMessage(ctx Context, key string, cncts []yodlee.DataExtractsProviderAccount) {
	slog.InfoContext(ctx, "handling cnct refresh messages", "cncts", cncts)

	result := IngestCnctRefreshes(ctx, key, cncts)
	slog.InfoContext(ctx, "completed cnct refresh ingestion", "putResults", result.PutResults, "deleteErrs", result.DeleteErrors)

	HandleAfterPublish(ctx, infra.CnctRefreshTopic, key, result.PutResults, nil)
	ProduceDeleteErrors(ctx, key, result.DeleteErrors)
}

func HandleAcctRefreshMessage(ctx Context, key string, accts []yodlee.DataExtractsAccount) {
	slog.InfoContext(ctx, "handling acct refresh messages", "accts", accts)

	result := IngestAcctsRefreshes(ctx, key, accts)
	slog.InfoContext(ctx, "completed acct refresh ingestion", "putResults", result.PutResults, "deleteErrs", result.DeleteErrors)

	HandleAfterPublish(ctx, infra.AcctRefreshTopic, key, result.PutResults, nil)
	ProduceDeleteErrors(ctx, key, result.DeleteErrors)
}

func HandleTxnRefreshMessage(ctx Context, key string, txns []yodlee.DataExtractsTransaction) {
	slog.InfoContext(ctx, "handling txn refresh messages", "txns", txns)

	result := IngestTxnRefreshes(ctx, key, txns)
	slog.InfoContext(ctx, "completed txn refresh ingestion", "putResults", result.PutResults, "deleteErrs", result.DeleteErrors)

	HandleAfterPublish(ctx, infra.TxnRefreshTopic, key, result.PutResults, nil)
	ProduceDeleteErrors(ctx, key, result.DeleteErrors)
}

func HandleHoldRefreshMessage(ctx Context, key string, holds []yodlee.DataExtractsHolding) {
	slog.InfoContext(ctx, "handling hold refresh messages", "holds", holds)

	result := IngestHoldRefreshes(ctx, key, holds)
	slog.InfoContext(ctx, "completed hold refresh ingestion", "putResults", result.PutResults, "deleteErrs", result.DeleteErrors)

	HandleAfterPublish(ctx, infra.HoldRefreshTopic, key, result.PutResults, nil)
	ProduceDeleteErrors(ctx, key, result.DeleteErrors)
}

func HandleCnctResponseMessage(ctx Context, key string, cncts yodlee.ProviderAccountResponse) {
	slog.InfoContext(ctx, "handling cnct response messages", "cncts", cncts)

	putResults := IngestCnctResponses(ctx, key, cncts)

	HandleAfterPublish(ctx, infra.CnctResponseTopic, key, putResults, func(errInputs []yodlee.ProviderAccount) any {
		return yodlee.ProviderAccountResponse{ProviderAccount: errInputs}
	})
}

func HandleAcctResponseMessage(ctx Context, key string, accts yodlee.AccountResponse) {
	slog.InfoContext(ctx, "handling acct response messages", "accts", accts)

	putResults := IngestAcctResponses(ctx, key, accts)

	HandleAfterPublish(ctx, infra.AcctResponseTopic, key, putResults, func(errInputs []yodlee.Account) any {
		return yodlee.AccountResponse{Account: errInputs}
	})
}

func HandleTxnResponseMessage(ctx Context, key string, txns yodlee.TransactionResponse) {
	slog.InfoContext(ctx, "handling txn response messages", "txns", txns)

	putResults := IngestTxnResponses(ctx, key, txns)

	HandleAfterPublish(ctx, infra.TxnResponseTopic, key, putResults, func(errInputs []yodlee.TransactionWithDateTime) any {
		return yodlee.TransactionResponse{Transaction: errInputs}
	})
}

func HandleHoldResponseMessage(ctx Context, key string, holds yodlee.HoldingResponse) {
	slog.InfoContext(ctx, "handling hold response messages", "holds", holds)

	putResults := IngestHoldResponses(ctx, key, holds)

	HandleAfterPublish(ctx, infra.HoldResponseTopic, key, putResults, func(errInputs []yodlee.Holding) any {
		return yodlee.HoldingResponse{Holding: errInputs}
	})
}

func HandleDeleteRecoveryMessage(ctx Context, key string, deleteRetries []DeleteRetry) {
	slog.InfoContext(ctx, "handling delete recovery messages", "deleteRetries", deleteRetries)

	deleteErrors := IngestDeleteRetries(ctx, deleteRetries)
	ProduceDeleteErrors(ctx, key, deleteErrors)
}

type BroadcastOutput struct {
	OriginTopic string            `json:"origintopic"`
	FiMessages  []json.RawMessage `json:"messages"`
}

func HandleBroadcastMessage(ctx Context, _ string, broadcast BroadcastOutput) {
	for _, binaryMsg := range broadcast.FiMessages {
		var opsFiMessage OpsFiMessage
		if err := json.Unmarshal(binaryMsg, &opsFiMessage); err != nil {
			slog.ErrorContext(ctx, "failed to unmarshal broadcast message opsFiMessage", "err", err)
			continue
		}

		strMsg := string(binaryMsg)
		slog.InfoContext(ctx, "broadcasting message", "topic", opsFiMessage.OriginTopic, "message", strMsg)
		ctx.FiMessageBroadcaster.Broadcast(opsFiMessage.ProfileId, opsFiMessage.OriginTopic, strMsg)
	}
}
