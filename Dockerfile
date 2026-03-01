# ---- Build Stage ----
FROM golang:1.24-alpine AS builder
RUN apk add --no-cache make

WORKDIR /sources

# ---- Dependency Layer ----
COPY go.mod go.sum Makefile ./
RUN go mod download
RUN make install

# ---- Source Layer ----
COPY . .
RUN make
RUN go build -o /bin/app ./cmd/server

# ---- Runtime Stage ----
FROM alpine:latest

# Copy swagger page
COPY --from=builder /sources/openapi ./openapi

# Copy binary
COPY --from=builder /bin/app ./app

EXPOSE 8080
CMD ["./app"]