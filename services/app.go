package svc

import (
	"context"
	"yodleeops/internal/infra"
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
