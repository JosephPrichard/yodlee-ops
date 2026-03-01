package infra

import "github.com/aws/aws-sdk-go-v2/aws"

type Topic string
type Bucket string

func (b Bucket) String() *string {
	return aws.String(string(b))
}

const (
	CnctRefreshTopic    Topic  = "cnct-refreshes"
	AcctRefreshTopic    Topic  = "acct-refreshes"
	HoldRefreshTopic    Topic  = "hold-refreshes"
	TxnRefreshTopic     Topic  = "txn-refreshes"
	CnctResponseTopic   Topic  = "cnct-responses"
	AcctResponseTopic   Topic  = "acct-responses"
	HoldResponseTopic   Topic  = "hold-responses"
	TxnResponseTopic    Topic  = "txn-responses"
	DeleteRecoveryTopic Topic  = "delete-recovery"
	BroadcastTopic      Topic  = "broadcast"
	CnctBucket          Bucket = "yodlee-cncts"
	AcctBucket          Bucket = "yodlee-accts"
	HoldBucket          Bucket = "yodlee-holds"
	TxnBucket           Bucket = "yodlee-txns"
)

var BucketList = []Bucket{CnctBucket, AcctBucket, HoldBucket, TxnBucket}

type Buckets struct {
	Connections  Bucket
	Accounts     Bucket
	Holdings     Bucket
	Transactions Bucket
}
