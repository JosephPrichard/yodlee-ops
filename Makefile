all: protos

protos:
	@echo "Generating protos"
	cd pb && \
	protoc \
		--go_opt=paths=source_relative \
		--go_out=. \
		--proto_path . \
		./messages.proto

templ:
	@echo "Generating templ"
	templ generate

# Install the prerequisite tools for the build in this makefile
install-prereqs:
	@echo "Installing protoc-gen-go..."
	go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	@echo "Installing templ..."
	go install github.com/a-h/templ/cmd/templ@latest

clean:
	rm ./pb/messageapp.pb.go

.PHONY: all clean
