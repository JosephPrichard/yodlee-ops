package svc

import (
	"context"
	"io"
	"log/slog"
	"os"
)

func Fatal(ctx context.Context, msg string, err error) {
	slog.ErrorContext(ctx, msg, "err", err.Error())
	os.Exit(1)
}

type TraceHandler struct {
	slog.Handler
}

func (h *TraceHandler) Handle(ctx context.Context, r slog.Record) error {
	if v := ctx.Value("trace"); v != nil {
		r.Add("trace", v)
	}
	return h.Handler.Handle(ctx, r)
}

var LogWriter io.Writer = os.Stderr

func InitLoggers(f *os.File) {
	if f != nil {
		LogWriter = io.MultiWriter(os.Stderr, f)
	}
	handler := slog.NewTextHandler(LogWriter, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})
	slog.SetDefault(slog.New(&TraceHandler{handler}))
}
