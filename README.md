# Yodlee Ops
Microservice to ship yodlee responses to s3 through message queues.

### Configure server

Environment Variables
```.env
AWS_SECRET_ID=test
AWS_SECRET_KEY=test
AWS_REGION=us-east-1
AWS_ENDPOINT=http://localhost:4566

KAFKA_BROKERS=localhost:9092
```

### Build & Execution

Execute
`$ go run main.go`