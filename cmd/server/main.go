package main

import (
	"context"
	cfg "filogger/config"
	svc "filogger/services"
	"github.com/joho/godotenv"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
)

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatalf("failed to load .env file: %v", err)
	}

	cfg.InitLoggers(nil)
	config := cfg.MakeConfig(cfg.ReadEnv())
	config.IsLocal = true

	slog.Info("starting server with configuration", "config", config)
	app := svc.MakeApp(config)

	consumerContext, cancelConsumer := context.WithCancel(context.Background())
	app.StartConsumers(svc.ConsumersConfig{Context: consumerContext, Concurrency: config.Concurrency})

	go func() {
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, os.Interrupt, os.Kill)
		<-sigChan

		slog.Info("shutting down server")

		cancelConsumer()        // stops the listeners
		app.KafkaClient.Close() // allows the kafka client to flush buffers

		os.Exit(1)
	}()

	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatalf("failed to start server: %v", err)
	}
}
