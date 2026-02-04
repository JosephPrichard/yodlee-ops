package svc

import cfg "filogger/config"

type App struct {
	cfg.AwsClient
	cfg.KafkaClient
}

func MakeApp(config cfg.Config) *App {
	return &App{
		AwsClient:   cfg.MakeAwsClient(config.AwsConfig),
		KafkaClient: cfg.MakeKafka(config.KafkaConfig),
	}
}
