# ---- Build Stage ----
FROM golang:1.25-alpine AS builder
RUN apk add --no-cache make

WORKDIR /sources

# ---- Dependency Layer ----
COPY ./app/go.mod ./app/go.sum ./app/Makefile ./
RUN go mod download
RUN make install

# ---- Source Layer ----
COPY ./app .
RUN make
RUN CGO_ENABLED=0 go build -trimpath -ldflags=-s -o /bin/app ./cmd/server

# ---- Runtime Stage ----
FROM alpine:latest AS runner

# Copy swagger page
COPY --from=builder /sources/openapi ./openapi

# Copy binary
COPY --from=builder /bin/app ./app

EXPOSE 8080
CMD ["./app"]