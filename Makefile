all: protos

protos:
	cd pb && \
	protoc \
		--go_opt=paths=source_relative \
		--go_out=. \
		--proto_path . \
		./messages.proto

templ:
	templ generate

install:
	go install github.com/ogen-go/ogen/cmd/ogen@latest
	go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	go install github.com/a-h/templ/cmd/templ@latest

clean:
	rm ./pb/messageapp.pb.go

.PHONY: all clean
