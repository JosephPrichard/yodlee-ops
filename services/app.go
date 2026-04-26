package svc

import (
	"context"
	"github.com/IBM/sarama"
	"yodleeops/storage"
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
