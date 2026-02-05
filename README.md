# Yodlee Ops
Microservice to ship yodlee responses to s3 through message queues.

### Configure server

Environment Variables
```.env
AWS_SECRET_ID=string
AWS_SECRET_KEY=string
AWS_REGION=string
AWS_ENDPOINT=url

CNCT_BUCKET=extn_cnct
ACCT_BUCKET=extn_acct
HOLD_BUCKET=extn_hold
TXN_BUCKET=extn_txn

KAFKA_BROKERS=string
GROUP_ID=string

CNCT_REFRESH_TOPIC=extn_cnct_refreshes
ACCT_REFRESH_TOPIC=extn_acct_refreshes
HOLD_REFRESH_TOPIC=extn_hold_refreshes
TXN_REFRESH_TOPIC=extn_txn_refreshes
CNCT_ENRICHMENT_TOPIC=extn_cnct_enrichments
ACCT_ENRICHMENT_TOPIC=extn_acct_enrichments
HOLD_ENRICHMENT_TOPIC=extn_hold_enrichments
TXN_ENRICHMENT_TOPIC=extn_txn_enrichments
DELETE_RETRY_TOPIC=delete_retries

CONSUMER_COMMIT_INTERVAL=1s
CONSUMER_MAX_WAIT=250ms
CONSUMER_MIN_BYTES=1
CONSUMER_MAX_BYTES=1048576
CONSUMER_CONCURRENCY=3

PRODUCER_BATCH_TIMEOUT=200ms
PRODUCER_BATCH_SIZE=100
```

### Build & Execution

Execute
`$ go run main.go`