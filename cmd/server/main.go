package main

import (
	"context"
	"github.com/joho/godotenv"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"time"
	"yodleeops/cmd"
	"yodleeops/internal/infra"
	svc "yodleeops/services"
)

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatalf("failed to load .env file: %v", err)
	}

	cmd.InitLoggers(nil)
	config := infra.MakeConfig()
	config.IsLocal = true

	app := &svc.App{
		AwsClient:            infra.MakeAwsClient(config),
		KafkaClient:          infra.MakeKafkaConsumerProducer(config),
		FiMessageBroadcaster: &svc.FiMessageBroadcaster{},
	}

	consumerCtx, cancelConsumer := context.WithCancel(context.Background())
	svc.StartConsumers(consumerCtx, app, 3)
	defer app.Close()

	go func() {
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, os.Interrupt, os.Kill)
		<-sigChan

		slog.Info("shutting down server")

		cancelConsumer() // stops the listeners

		os.Exit(0)
	}()

	if err := http.ListenAndServe(":8080", svc.MakeRoot(app, config.AllowOrigins)); err != nil {
		log.Fatalf("failed to start server: %v", err)
	}

	for {
		select {
		case <-consumerCtx.Done():
			return
		case <-time.After(time.Millisecond * 50):
		}
	}
}
