BINARY_NAME=scalesync
MODULE=github.com/walkerscm/scaleSyncGo
VERSION=$(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
COMMIT=$(shell git rev-parse --short HEAD 2>/dev/null || echo "none")
BUILD_DATE=$(shell date -u +%Y-%m-%dT%H:%M:%SZ)
LDFLAGS=-ldflags "-X $(MODULE)/internal/cli.Version=$(VERSION) -X $(MODULE)/internal/cli.CommitSHA=$(COMMIT) -X $(MODULE)/internal/cli.BuildDate=$(BUILD_DATE)"

.PHONY: build run test lint clean fmt vet tidy docker-build

build:
	go build $(LDFLAGS) -o bin/$(BINARY_NAME) ./cmd/scalesync

run:
	go run $(LDFLAGS) ./cmd/scalesync $(ARGS)

test:
	go test ./... -v -race

test-cover:
	go test ./... -v -race -coverprofile=coverage.out
	go tool cover -html=coverage.out -o coverage.html

lint:
	golangci-lint run ./...

fmt:
	gofmt -s -w .

vet:
	go vet ./...

tidy:
	go mod tidy

clean:
	rm -rf bin/ coverage.out coverage.html

docker-build:
	docker build -t $(BINARY_NAME):$(VERSION) .
