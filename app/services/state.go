package svc

import (
	"context"
	"yodleeops/storage"

	"github.com/IBM/sarama"
)

type State struct {
	AWS                  storage.AWS
	Producer             sarama.AsyncProducer
	FiMessageBroadcaster *FiMessageBroadcaster
}

type Context struct {
	context.Context
	*State
}
