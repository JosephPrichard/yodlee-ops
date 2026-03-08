package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"yodleeops/cmd"
	"yodleeops/model"
	svc "yodleeops/services"

	_ "net/http/pprof"

	"github.com/joho/godotenv"
)

func main() {
	err := godotenv.Load()
	if err != nil {
		slog.Warn("starting yodlee ops: failed to load .env file", "err", err)
	}

	cmd.InitLoggers(nil)

	serverConfig := model.MakeConfig()
	serverConfig.IsLocal = true

	slog.Info("starting yodlee ops: starting server", "serverConfig", serverConfig)

	s3Client := model.MakeS3Client(serverConfig)

	kafkaConfig := model.MakeSaramaConfig(serverConfig)
	model.CreateKafkaTopics(serverConfig.KafkaBrokers, kafkaConfig)
	producer := model.MakeSaramaProducer(serverConfig.KafkaBrokers, kafkaConfig)

	// produces messages to topics to easily test that producer/consumers are working without an external producer. comment out in prod.
	go cmd.ExecuteDemoProducer(serverConfig, kafkaConfig)

	state := &svc.State{
		AWS:                  model.MakeAWS(serverConfig, s3Client),
		Producer:             producer,
		FiMessageBroadcaster: &svc.FiMessageBroadcaster{},
	}

	slog.Info("starting yodlee ops: starting consumer", "serverConfig", serverConfig, "kafkaConfig", fmt.Sprintf("%+v", kafkaConfig))

	if err := svc.StartConsumers(context.Background(), serverConfig.KafkaBrokers, kafkaConfig, state); err != nil {
		log.Fatalf("starting yodlee ops: failed to start consumers: %v", err)
	}

	go func() {
		if err := http.ListenAndServe(":6060", nil); err != nil {
			log.Fatalf("starting yodlee ops: failed to start pprof server: %v", err)
		}
	}()

	mux := svc.MakeServeMux(state)
	if err := http.ListenAndServe(":8080", mux); err != nil {
		log.Fatalf("starting yodlee ops: failed to start server: %v", err)
	}
}
