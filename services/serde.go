package svc

import (
	"encoding/json"
	"log/slog"
)

func SerializeYodleeData(v any) ([]byte, bool) {
	jsonData, err := json.Marshal(v)
	if err != nil {
		slog.Error("json marshal failed", "err", err)
		return nil, false
	}

	//var buf bytes.Buffer
	//gzipWriter := gzip.NewWriter(&buf)
	//defer gzipWriter.Close()
	//
	//if _, err := gzipWriter.Write(jsonData); err != nil {
	//	slog.Error("gzip write failed", "err", err)
	//	return nil, false
	//}
	//return buf.Bytes(), true

	return jsonData, true
}
