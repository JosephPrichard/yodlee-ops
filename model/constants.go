package model

import (
	"github.com/aws/aws-sdk-go-v2/aws"
)

type Topic string
type Bucket string

func (b Bucket) String() *string {
	return aws.String(string(b))
}

type Buckets struct {
	CnctBucket Bucket
	AcctBucket Bucket
	HoldBucket Bucket
	TxnBucket  Bucket
}

const (
	CnctRefreshTopic  Topic = "cnct-refreshes"
	AcctRefreshTopic  Topic = "acct-refreshes"
	HoldRefreshTopic  Topic = "hold-refreshes"
	TxnRefreshTopic   Topic = "txn-refreshes"
	CnctResponseTopic Topic = "cnct-responses"
	AcctResponseTopic Topic = "acct-responses"
	HoldResponseTopic Topic = "hold-responses"
	TxnResponseTopic  Topic = "txn-responses"
	DeleteRetryTopic  Topic = "delete-retry"
	BroadcastTopic    Topic = "broadcast"

	CnctRefreshTopicGroupID  = "cnct-refreshes-group"
	AcctRefreshTopicGroupID  = "acct-refreshes-group"
	HoldRefreshTopicGroupID  = "hold-refreshes-group"
	TxnRefreshTopicGroupID   = "txn-refreshes-group"
	CnctResponseTopicGroupID = "cnct-responses-group"
	AcctResponseTopicGroupID = "acct-responses-group"
	HoldResponseTopicGroupID = "hold-responses-group"
	TxnResponseTopicGroupID  = "txn-responses-group"
	DeleteRetryTopicGroupID  = "delete-retry-group"

	//CnctBucket Bucket = "yodlee-cncts"
	//AcctBucket Bucket = "yodlee-accts"
	//HoldBucket Bucket = "yodlee-holds"
	//TxnBucket  Bucket = "yodlee-txns"
)
