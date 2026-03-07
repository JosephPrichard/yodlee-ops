package main

import (
	"context"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"time"
	"yodleeops/cmd"
	"yodleeops/infra"
	svc "yodleeops/services"

	"github.com/joho/godotenv"
)

func main() {
	err := godotenv.Load()
	if err != nil {
		slog.Error("failed to load .env file", "err", err)
	}

	cmd.InitLoggers(nil)

	config := infra.MakeConfig()
	//config.IsLocal = true
	kafkaConfig := infra.MakeSaramaConfig(config)
	producer := infra.MakeSaramaProducer(config.KafkaBrokers, kafkaConfig)
	s3Client := infra.MakeS3Client(config)

	state := &svc.State{
		AWS:                  infra.MakeAWS(s3Client),
		Producer:             producer,
		FiMessageBroadcaster: &svc.FiMessageBroadcaster{},
	}

	rootCtx, cancel := context.WithCancel(context.Background())
	if err := svc.StartConsumers(rootCtx, config.KafkaBrokers, kafkaConfig, state); err != nil {
		log.Fatalf("failed to start consumers: %v", err)
	}

	go func() {
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, os.Interrupt, os.Kill)
		<-sigChan

		slog.Info("shutting down server")

		cancel()
		os.Exit(0)
	}()

	mux := svc.MakeServeMux(state)
	if err := http.ListenAndServe(":8080", mux); err != nil {
		log.Fatalf("failed to start server: %v", err)
	}

	for {
		select {
		case <-rootCtx.Done():
			return
		case <-time.After(time.Millisecond * 50):
		}
	}
}
