package svc

import (
	"context"
	"yodleeops/infra"
)

type App struct {
	infra.AwsClient
	infra.KafkaClient
	FiMessageBroadcaster *FiMessageBroadcaster
}

type AppContext struct {
	context.Context
	*App
}
