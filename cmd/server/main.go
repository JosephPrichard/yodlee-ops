package server

import (
	"context"
	"encoding/json"
	"filogger"
	"filogger/services"
)

func main() {
	ctx := context.WithValue(context.Background(), "trace", "setup-server")

	var config flog.Config
	if err := json.Unmarshal(flog.ConfigFile, &config); err != nil {
		svc.Fatal(ctx, "failed to parse config", err)
	}

	_, err := svc.MakeAwsClient(ctx, config)
	if err != nil {
		svc.Fatal(ctx, "failed to make services", err)
	}
}
