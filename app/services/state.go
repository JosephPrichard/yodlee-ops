package svc

import (
	"context"
	"yodleeops/storage"

	"github.com/IBM/sarama"
)

type State struct {
	S3Client             storage.S3Client
	Producer             sarama.AsyncProducer
	FiMessageBroadcaster *FiMessageBroadcaster
}

type Context struct {
	context.Context
	*State
}
