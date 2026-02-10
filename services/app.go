package svc

import (
	"yodleeops/infra"
)

type App struct {
	*infra.AwsClient
	*infra.KafkaClient
	Broadcaster
}
