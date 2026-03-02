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
ALLOW_ORIGINS=http://localhost:5173
```

Create a '.env' file at the root for local development or pass them as arguments when deploying.

## Development

Build

`$ make install && make`

Execute

`$ go run cmd/server/main.go`

## Deployment

### Manual
Build

`$ docker build -t development/yodlee-ops:latest ECR_REGISTRY/ECR_REPOSITORY:$(git log -n 1 --pretty=format:"%H") .`

`$ aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin ECR_REGISTRY`

`$ docker push ECR_REGISTRY/IMAGE_NAME`

