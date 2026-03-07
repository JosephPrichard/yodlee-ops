# Yodlee Ops
Microservice to ship yodlee responses to s3 through message queues.

## Configuration

Environment Variables
```.env
AWS_SECRET_ID=test
AWS_SECRET_KEY=test
AWS_DEFAULT_REGION=us-east-1
AWS_ENDPOINT=http://localhost:4566
KAFKA_BROKERS=localhost:9092
```

Create a '.env' file at the root for local development or pass them as arguments when deploying.

## Development

Build

`$ make install && make`

Execute

`$ go run cmd/server/main.go`

## Deployment

Deployment to the test environment is done through GitHub actions. Refer to `.github/workflows/deploy.yml` 

### Architecture

<img width="4416" height="2580" alt="Test us-east-1 (1)" src="https://github.com/user-attachments/assets/af77555a-0848-482a-8843-126b27cd66de" />