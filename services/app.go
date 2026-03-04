package svc

import (
	"context"
	"github.com/IBM/sarama"

	"yodleeops/infra"
)

type App struct {
	AWS                  infra.AWS
	Producer             sarama.AsyncProducer
	FiMessageBroadcaster *FiMessageBroadcaster
}

type Context struct {
	context.Context
	*App
}
