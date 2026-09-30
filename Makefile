.PHONY: build test test-integration vet fmt-check check run generate-contracts

PROTO_ROOT := ../../contracts

generate-contracts:
	protoc -I $(PROTO_ROOT) \
		--go_out=. --go_opt=module=bridgepay-refund-go --go_opt=Mrefund/v1/core.proto=bridgepay-refund-go/internal/coreclient/pb \
		--go-grpc_out=. --go-grpc_opt=module=bridgepay-refund-go --go-grpc_opt=Mrefund/v1/core.proto=bridgepay-refund-go/internal/coreclient/pb \
		$(PROTO_ROOT)/refund/v1/core.proto

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
