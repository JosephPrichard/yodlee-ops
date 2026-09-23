all: sources

sources:
	cd openapi && go generate

install:
	go install github.com/ogen-go/ogen/cmd/ogen@v1.19.0

tests:
	go test ./... -timeout=60s -v

clean:
	rm -rf ./openapi/sources

.PHONY: all clean