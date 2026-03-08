package main

import (
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
		slog.Warn("failed to load .env file", "err", err)
	}

	cmd.InitLoggers(nil)

	serverConfig := model.MakeConfig()
	serverConfig.IsLocal = true

	s3Client := model.MakeS3Client(serverConfig)

	kafkaConfig := model.MakeSaramaConfig(serverConfig)
	//producer := client.MakeSaramaProducer(serverConfig.KafkaBrokers, kafkaConfig)

	// produces messages to topics to easily test that producer/consumers are working without an external producer. comment out in prod.
	//go cmd.ExecuteDemoProducer(serverConfig, kafkaConfig)

	state := &svc.State{
		AWS: model.MakeAWS(serverConfig, s3Client),
		//Producer:             producer,
		FiMessageBroadcaster: &svc.FiMessageBroadcaster{},
	}

	slog.Info("starting consumer", "serverConfig", serverConfig, "kafkaConfig", fmt.Sprintf("%+v", kafkaConfig)) // fmt.Sprintf is needed to serialize closures.

	//if err := svc.StartConsumers(context.Background(), serverConfig.KafkaBrokers, kafkaConfig, state); err != nil {
	//	log.Fatalf("failed to start consumers: %v", err)
	//}

	go func() {
		if err := http.ListenAndServe(":6060", nil); err != nil {
			log.Fatalf("failed to start pprof server: %v", err)
		}
	}()

	mux := svc.MakeServeMux(state)
	if err := http.ListenAndServe(":8080", mux); err != nil {
		log.Fatalf("failed to start server: %v", err)
	}
}
