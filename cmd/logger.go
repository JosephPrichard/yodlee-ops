package cmd

import (
	"context"
	"io"
	"log/slog"
	"os"
)

type TraceHandler struct {
	slog.Handler
}

func (h *TraceHandler) Handle(ctx context.Context, r slog.Record) error {
	if v := ctx.Value("trace"); v != nil {
		r.Add("trace", v)
	}
	return h.Handler.Handle(ctx, r)
}

func InitLoggers(f *os.File) {
	var logWriter io.Writer = os.Stderr
	if f != nil {
		logWriter = io.MultiWriter(os.Stderr, f)
	}
	handler := slog.NewTextHandler(logWriter, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})
	slog.SetDefault(slog.New(&TraceHandler{handler}))
}
