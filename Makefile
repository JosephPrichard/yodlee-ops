SWAGGER_VERSION := 5.18.2
SWAGGER_URL := https://github.com/swagger-api/swagger-ui/archive/refs/tags/v$(SWAGGER_VERSION).zip

all: codegen

codegen:
	cd openapi && go generate

install:
	go install github.com/ogen-go/ogen/cmd/ogen@v1.19.0

clean:
	rm -rf ./openapi/sources

.PHONY: all clean

SWAGGER_VERSION := 5.18.2
SWAGGER_URL := https://github.com/swagger-api/swagger-ui/archive/refs/tags/v$(SWAGGER_VERSION).zip