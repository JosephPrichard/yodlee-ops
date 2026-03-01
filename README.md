# Yodlee Ops
Microservice to ship yodlee responses to s3 through message queues.

### Configuration

Environment Variables
```.env
AWS_SECRET_ID=test
AWS_SECRET_KEY=test
AWS_REGION=us-east-1
AWS_ENDPOINT=http://localhost:4566
KAFKA_BROKERS=localhost:9092
ALLOW_ORIGINS=http://localhost:5173
```

Create a '.env' file at the root for local development or pass them as arguments when deploying.

### Development

Build

`$ docker build -t yodlee-ops:latest .`

Execute

`$ docker run -d -p 8080:8080 yodlee-ops:latest`

OR

Build

`$ make install && make`

Execute

`$ go run cmd/server/main.go`

### Deployment

Build

`$ docker build -t yodlee-ops:latest .`