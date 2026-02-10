package main

import (
	"context"
	"github.com/joho/godotenv"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"time"
	"yodleeops/infra"
	svc "yodleeops/services"
)

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatalf("failed to load .env file: %v", err)
	}

	infra.InitLoggers(nil)
	config := infra.MakeConfig(infra.ReadEnv())

	log.Printf("config: %+v", config)
	config.IsLocal = true

	slog.Info("starting server with configuration", "config", config)
	app := svc.MakeApp(config)

	consumerCtx, cancelConsumer := context.WithCancel(context.Background())
	app.StartConsumers(svc.ConsumersConfig{Context: consumerCtx, Concurrency: config.Concurrency})
	defer app.Close()

	go func() {
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, os.Interrupt, os.Kill)
		<-sigChan

		slog.Info("shutting down server")

		cancelConsumer() // stops the listeners

		os.Exit(0)
	}()

	//if err := http.ListenAndServe(":8080", nil); err != nil {
	//	log.Fatalf("failed to start server: %v", err)
	//}

	for {
		select {
		case <-consumerCtx.Done():
			return
		case <-time.After(time.Millisecond * 50):
		}
	}
}
