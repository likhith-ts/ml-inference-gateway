.PHONY: proto build test lint docker-build up down

PROTOC_GEN_GO  := $(shell go env GOPATH)/bin/protoc-gen-go
PROTOC_GEN_GRPC:= $(shell go env GOPATH)/bin/protoc-gen-go-grpc

## Generate Go gRPC stubs from proto/inference.proto
proto:
	@mkdir -p /tmp/pb-gen
	protoc \
	  --go_out=/tmp/pb-gen --go_opt=paths=source_relative \
	  --go-grpc_out=/tmp/pb-gen --go-grpc_opt=paths=source_relative \
	  proto/inference.proto
	@mv /tmp/pb-gen/proto/* pb/ 2>/dev/null || true
	@rm -rf /tmp/pb-gen

## Build the gateway binary
build:
	CGO_ENABLED=0 go build -ldflags="-s -w" -o bin/gateway ./cmd/gateway

## Run unit tests with the race detector
test:
	go test -race -count=1 ./...

## Run go vet
lint:
	go vet ./...

## Build Docker images
docker-build:
	docker build -t ml-inference-gateway:latest -f Dockerfile .
	docker build -t ml-inference-worker:latest -f Dockerfile.worker .

## Start the full local cluster
up:
	docker compose -f deployments/docker-compose.yml up --build

## Tear down the local cluster
down:
	docker compose -f deployments/docker-compose.yml down -v
