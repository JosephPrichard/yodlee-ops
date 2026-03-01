package svc

import (
	"context"

	"yodleeops/infra"
)

type App struct {
	AWS                  infra.AWS
	Kafka                infra.KafkaClient
	FiMessageBroadcaster *FiMessageBroadcaster
}

type Context struct {
	context.Context
	*App
}
