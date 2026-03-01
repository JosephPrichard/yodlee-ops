package main

import (
	"log"
	"log/slog"
	"net/http"
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
	config.IsLocal = true

	s3Client := infra.MakeS3Client(config)
	app := &svc.App{
		AWS:                  infra.MakeAwsClient(s3Client),
		Kafka:                infra.MakeKafkaConsumerProducer(config),
		FiMessageBroadcaster: &svc.FiMessageBroadcaster{},
	}

	//consumerCtx, cancelConsumer := context.WithCancel(context.Background())
	//svc.StartConsumers(svc.Context{Context: consumerCtx, App: app}, 3)
	//defer app.Kafka.Close()
	//
	//go func() {
	//	sigChan := make(chan os.Signal, 1)
	//	signal.Notify(sigChan, os.Interrupt, os.Kill)
	//	<-sigChan
	//
	//	slog.Info("shutting down server")
	//
	//	cancelConsumer() // stops the listeners
	//
	//	os.Exit(0)
	//}()

	mux := svc.MakeServeMux(app, config.AllowOrigins)
	svc.WithHealthChecker(mux, svc.HealthCheckConfig{
		S3: s3Client,
	})
	if err := http.ListenAndServe(":8080", mux); err != nil {
		log.Fatalf("failed to start server: %v", err)
	}

	//for {
	//	select {
	//	case <-consumerCtx.Done():
	//		return
	//	case <-time.After(time.Millisecond * 50):
	//	}
	//}
}
