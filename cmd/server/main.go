package main

import (
	"context"
	"github.com/IBM/sarama"
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

	kafkaConfig := sarama.NewConfig()
	kafkaConfig.Version = sarama.V3_6_0_0
	kafkaConfig.Producer.Return.Errors = true

	producer, err := sarama.NewAsyncProducer(config.KafkaBrokers, kafkaConfig)
	if err != nil {
		log.Fatalf("failed to create kafka producer: %v", err)
	}
	go func() {
		for err := range producer.Errors() {
			slog.Error("failed to produce message", "err", err)
		}
	}()

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

	mux := svc.MakeServeMux(state, config.AllowOrigins)
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
