package svc

import (
	"context"
	"github.com/IBM/sarama"

	"yodleeops/model"
)

type State struct {
	AWS                  model.AWS
	Producer             sarama.AsyncProducer
	FiMessageBroadcaster *FiMessageBroadcaster
}

type Context struct {
	context.Context
	*State
}
