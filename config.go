package flog

import _ "embed"

//go:embed config.json
var ConfigFile []byte

type Config struct {
	AwsSecretID  string `json:"awsSecretID"`
	AwsSecretKey string `json:"awsSecretKey"`
	AwsRegion    string `json:"awsRegion"`

	CnctBucket string `json:"cnctBucket"`
	AcctBucket string `json:"acctBucket"`
	HoldBucket string `json:"holdBucket"`
	TxnBucket  string `json:"txnBucket"`

	KafkaBrokers []string `json:"kafkaBrokers"`
	GroupID      string   `json:"groupID"`

	CnctRefreshTopic    string `json:"cnctRefreshTopic"`
	AcctRefreshTopic    string `json:"acctRefreshTopic"`
	HoldRefreshTopic    string `json:"holdRefreshTopic"`
	TxnRefreshTopic     string `json:"txnRefreshTopic"`
	CnctEnrichmentTopic string `json:"cnctEnrichmentTopic"`
	AcctEnrichmentTopic string `json:"acctEnrichmentTopic"`
	HoldEnrichmentTopic string `json:"holdEnrichmentTopic"`
	TxnEnrichmentTopic  string `json:"txnEnrichmentTopic"`

	Endpoint   string
	PageLength *int32
}
