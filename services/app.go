package svc

import cfg "filogger/config"

type App struct {
	cfg.AwsClient
	cfg.KafkaClient
}
