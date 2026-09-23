package svc

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"testing"
)

func EncodeGzipJSON(ctx context.Context, v any) ([]byte, bool) {
	var encoded bytes.Buffer

	gzipWriter := gzip.NewWriter(&encoded)
	defer gzipWriter.Close()

	if err := json.NewEncoder(gzipWriter).Encode(v); err != nil {
		slog.ErrorContext(ctx, "json encode failed", "err", err)
		return nil, false
	}

	// Close is required to flush gzip footer
	if err := gzipWriter.Close(); err != nil {
		slog.ErrorContext(ctx, "gzip close failed", "err", err)
		return nil, false
	}

	return encoded.Bytes(), true
}

func DecodeGzipJSON[JSON any](r io.Reader, decoded *JSON) error {
	gzipReader, err := gzip.NewReader(r)
	if err != nil {
		return fmt.Errorf("make gzip reader: %w", err)
	}
	defer gzipReader.Close()
	if err := json.NewDecoder(gzipReader).Decode(decoded); err != nil {
		return fmt.Errorf("decode gzip json: %w", err)
	}
	return nil
}

func MustEncodeJson(t *testing.T, v any) []byte {
	var encoded bytes.Buffer

	gzipWriter := gzip.NewWriter(&encoded)
	defer gzipWriter.Close()

	if err := json.NewEncoder(gzipWriter).Encode(v); err != nil {
		t.Fatal(err)
	}
	// Close is required to flush gzip footer
	if err := gzipWriter.Close(); err != nil {
		t.Fatal(err)
	}

	return encoded.Bytes()
}
