package client

import (
	"github.com/aws/aws-sdk-go-v2/aws"
)

type Topic string
type Bucket string

func (b Bucket) String() *string {
	return aws.String(string(b))
}

const (
	// CnctRefreshTopic etc. topics
	CnctRefreshTopic  Topic = "cnct-refreshes"
	AcctRefreshTopic  Topic = "acct-refreshes"
	HoldRefreshTopic  Topic = "hold-refreshes"
	TxnRefreshTopic   Topic = "txn-refreshes"
	CnctResponseTopic Topic = "cnct-responses"
	AcctResponseTopic Topic = "acct-responses"
	HoldResponseTopic Topic = "hold-responses"
	TxnResponseTopic  Topic = "txn-responses"
	DeleteRetryTopic  Topic = "delete-recovery"
	BroadcastTopic    Topic = "broadcast"

	// CnctRefreshTopicGroupID etc, group ids for topics
	CnctRefreshTopicGroupID  = "cnct-refreshes_group"
	AcctRefreshTopicGroupID  = "acct-refreshes_group"
	HoldRefreshTopicGroupID  = "hold-refreshes_group"
	TxnRefreshTopicGroupID   = "txn-refreshes_group"
	CnctResponseTopicGroupID = "cnct-responses_group"
	AcctResponseTopicGroupID = "acct-responses_group"
	HoldResponseTopicGroupID = "hold-responses_group"
	TxnResponseTopicGroupID  = "txn-responses_group"
	DeleteRetryTopicGroupID  = "delete-recovery_group"

	// CnctBucket etc. buckets
	CnctBucket Bucket = "yodlee-cncts"
	AcctBucket Bucket = "yodlee-accts"
	HoldBucket Bucket = "yodlee-holds"
	TxnBucket  Bucket = "yodlee-txns"
)

var BucketList = []Bucket{CnctBucket, AcctBucket, HoldBucket, TxnBucket}

var TopicList = []Topic{
	CnctRefreshTopic,
	AcctRefreshTopic,
	HoldRefreshTopic,
	TxnRefreshTopic,
	CnctResponseTopic,
	AcctResponseTopic,
	HoldResponseTopic,
	TxnResponseTopic,
	DeleteRetryTopic,
	BroadcastTopic,
}

type Buckets struct {
	Connections  Bucket
	Accounts     Bucket
	Holdings     Bucket
	Transactions Bucket
}
