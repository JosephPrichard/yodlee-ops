awslocal s3 mb s3://extn-cncts
awslocal s3 mb s3://extn-accts
awslocal s3 mb s3://extn-holds
awslocal s3 mb s3://extn-txns

docker exec -it ops-kafka /usr/bin/kafka-topics --create --topic extn_cnct_refreshes    --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
docker exec -it ops-kafka /usr/bin/kafka-topics --create --topic extn_acct_refreshes    --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
docker exec -it ops-kafka /usr/bin/kafka-topics --create --topic extn_hold_refreshes    --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
docker exec -it ops-kafka /usr/bin/kafka-topics --create --topic extn_txn_refreshes     --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

docker exec -it ops-kafka /usr/bin/kafka-topics --create --topic extn_cnct_enrichments  --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
docker exec -it ops-kafka /usr/bin/kafka-topics --create --topic extn_acct_enrichments  --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
docker exec -it ops-kafka /usr/bin/kafka-topics --create --topic extn_hold_enrichments  --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
docker exec -it ops-kafka /usr/bin/kafka-topics --create --topic extn_txn_enrichments   --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1