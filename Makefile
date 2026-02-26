all: go-generate swagger-ui

go-generate:
	cd openapi && go generate

swagger-ui:
	docker run --rm -v ./openapi:/openapi redocly/cli build-docs /openapi/yodlee-ops.yaml -o /openapi/static/index.html

install:
	go install github.com/ogen-go/ogen/cmd/ogen@latest

clean:
	rm -rf ./openapi/sources
	rm -rf ./openapi/static

.PHONY: all clean
