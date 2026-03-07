package svc

import (
	"context"
	"github.com/IBM/sarama"

	"yodleeops/client"
)

type State struct {
	AWS                  client.AWS
	Producer             sarama.AsyncProducer
	FiMessageBroadcaster *FiMessageBroadcaster
}

type Context struct {
	context.Context
	*State
}
