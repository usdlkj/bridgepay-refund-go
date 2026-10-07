.PHONY: build test test-integration vet fmt-check check run generate-contracts

PROTO_ROOT := contracts
PROTOBUF_INCLUDE ?= $(shell dirname $$(command -v protoc))/../include
BACKOFFICE_PROTO_ROOT := internal/proto

generate-contracts:
	protoc -I $(PROTO_ROOT) -I $(BACKOFFICE_PROTO_ROOT) -I $(PROTOBUF_INCLUDE) \
		--go_out=. --go_opt=module=bridgepay-refund-go --go_opt=Mrefund/v1/core.proto=bridgepay-refund-go/internal/coreclient/pb --go_opt=Mrefund/v1/gateway.proto=bridgepay-refund-go/internal/coreclient/pb \
		--go_opt=Mbackoffice/refund/v1/refund.proto=bridgepay-refund-go/internal/backofficepb \
		--go-grpc_out=. --go-grpc_opt=module=bridgepay-refund-go --go-grpc_opt=Mrefund/v1/core.proto=bridgepay-refund-go/internal/coreclient/pb --go-grpc_opt=Mrefund/v1/gateway.proto=bridgepay-refund-go/internal/coreclient/pb --go-grpc_opt=Mbackoffice/refund/v1/refund.proto=bridgepay-refund-go/internal/backofficepb \
		$(PROTO_ROOT)/refund/v1/core.proto \
		$(PROTO_ROOT)/refund/v1/gateway.proto \
		$(BACKOFFICE_PROTO_ROOT)/backoffice/refund/v1/refund.proto

build:
	mkdir -p bin
	go build -o bin/bridgepay-refund-go ./cmd/api

test:
	go test ./...

test-integration:
	go test -tags integration ./...

vet:
	go vet ./...

fmt-check:
	@test -z "$$(gofmt -l .)" || (gofmt -l . && exit 1)

check: fmt-check vet test build

run:
	go run ./cmd/api
