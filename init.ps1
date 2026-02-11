awslocal s3 mb s3://yodlee-cncts
awslocal s3 mb s3://yodlee-accts
awslocal s3 mb s3://yodlee-holds
awslocal s3 mb s3://yodlee-txns

$buckets = @(
    "yodlee-cncts",
    "yodlee-accts",
    "yodlee-holds",
    "yodlee-txns"
)

foreach ($b in $buckets) {
    awslocal s3 mb s3://$b
}

$topics = @(
    "cnct_refreshes",
    "acct_refreshes",
    "hold_refreshes",
    "txn_refreshes",
    "cnct_responses",
    "acct_responses",
    "hold_responses",
    "txn_responses",
    "delete_recovery",
    "broadcast"
)

foreach ($t in $topics) {
    docker exec -it ops-kafka /usr/bin/kafka-topics --create `
    --topic $t `
    --bootstrap-server localhost:9092 `
    --partitions 1 `
    --replication-factor 1
}
