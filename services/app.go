package svc

import (
	"context"
	"yodleeops/infra"
)

type App struct {
	infra.AWS
	infra.KafkaClient
	FiMessageBroadcaster *FiMessageBroadcaster
}

type Context struct {
	context.Context
	*App
}
