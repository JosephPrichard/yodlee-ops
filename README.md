# OpsLogger
A demo for a system that ships hierarchically related financial information to s3 through message queueapp.
This information can later be viewed through a GUI for auditing and debugging.

Based off an idea I had for a system at work. I wanted to implement the solution but was moved out of the org.

## Problem Statement

## Usage

### Configure server

Create a config.json file in the root directory.
`$ touch config.json`

The file should fill out the following fields correctly.
This contains the connection information data, and the topics each protobuf message will be sent on.

`
{
    "awsSecretID": "string",
    "awsSecretKey": "string",
    "awsRegion": "string",
    "errorLogBucket": "error_logs",
    "cnctBucket": "extn_cnct",
    "acctBucket": "extn_acct",
    "holdBucket": "extn_hold",
    "txnBucket": "extn_txn",
    "kafkaBrokers": ["string"],
    "groupID": "string",
    "errorLogTopic": "error_logs",
    "cnctRefreshTopic": "extn_cnct_refreshes",
    "acctRefreshTopic": "extn_acct_refreshes",
    "holdRefreshTopic": "extn_hold_refreshes",
    "txnRefreshTopic": "extn_txn_refreshes",
    "cnctEnrichmentTopic": "extn_cnct_enrichments",
    "acctEnrichmentTopic": "extn_acct_enrichments",
    "holdEnrichmentTopic": "extn_hold_enrichments",
    "txnEnrichmentTopic": "extn_txn_enrichments"
}
`

### Run the server

Execute
`$ go run main.go`