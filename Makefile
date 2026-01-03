.PHONY: all release-local docker build build-cli build-server

BIN=veridicaldb
VERSION?=v1.0.0

all: build

# Build the CLI binary (default)
build:
	go build -o build/$(BIN) ./cmd/veridicaldb

# Build the CLI binary (explicit)
build-cli:
	go build -o build/$(BIN) ./cmd/veridicaldb

# Build the server binary
build-server:
	go build -o build/$(BIN)-server ./cmd/server

release-local:
	@echo "Running local release script (cross-compile)..."
	./scripts/release.sh $(VERSION)

docker:
	@echo "Building Docker image"
	docker build -t veridicaldb:$(VERSION) .

# VeridicalDB Makefile
# Build, test, and manage the database

.PHONY: test clean install run init fmt lint help smoke-test stress-test

# Build variables
VERSION?=v1.0.0
BUILD_DATE=$(shell date -u +"%Y-%m-%dT%H:%M:%SZ")
LDFLAGS=-ldflags "-X main.version=$(VERSION) -X main.buildDate=$(BUILD_DATE)"

# Directories
BUILD_DIR=./build

# Run tests
test:
	@echo "Running tests..."
	go test -v -race ./...

# Run tests with coverage
test-coverage:
	@echo "Running tests with coverage..."
	go test -v -race -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report: coverage.html"

# Run smoke tests
smoke-test: build
	@echo "Running smoke tests..."
	./scripts/smoke_test.sh

# Run stress tests
stress-test: build
	@echo "Running stress tests..."
	./scripts/stress_test.sh

stress-test-quick: build
	@echo "Running quick stress tests..."
	./scripts/stress_test.sh --quick

stress-test-full: build
	@echo "Running full stress tests..."
	./scripts/stress_test.sh --full

# Clean build artifacts
clean:
	@echo "Cleaning..."
	rm -rf $(BUILD_DIR)
	rm -f coverage.out coverage.html
	rm -rf ./data

# Install to GOPATH/bin
install: build
	@echo "Installing..."
	cp $(BUILD_DIR)/$(BINARY_NAME) $(GOPATH)/bin/

# Initialize a new database
init: build
	@echo "Initializing database..."
	$(BUILD_DIR)/$(BINARY_NAME) init ./data

# Run the database
run: build
	@echo "Starting VeridicalDB..."
	$(BUILD_DIR)/$(BINARY_NAME)

# Format code
fmt:
	@echo "Formatting code..."
	go fmt ./...

# Run linter (requires golangci-lint)
lint:
	@echo "Running linter..."
	@which golangci-lint > /dev/null || (echo "Install golangci-lint: go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest" && exit 1)
	golangci-lint run ./... --timeout 10m

# Run full CI steps locally: deps, lint, vet, tests (coverage)
.PHONY: ci
ci: deps
	@echo "Running CI: lint, vet, tests..."
	@command -v golangci-lint >/dev/null || (echo "golangci-lint not found, installing v2.7.2..." && go install github.com/golangci/golangci-lint/cmd/golangci-lint@v2.7.2)
	golangci-lint run ./... --timeout 10m
	go vet ./...
	go test ./... -v -coverprofile=coverage.out

# Download dependencies
deps:
	@echo "Downloading dependencies..."
	go mod download
	go mod tidy

# Development setup
dev-setup: deps
	@echo "Setting up development environment..."
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest

# Quick development cycle
dev: fmt test build

# Show version
version:
	@echo "VeridicalDB $(VERSION)"

# Help
help:
	@echo "VeridicalDB Makefile"
	@echo ""
	@echo "Usage: make [target]"
	@echo ""
	@echo "Targets:"
	@echo "  build         Build the binary"
	@echo "  build-all     Build for all platforms (Linux, macOS, Windows)"
	@echo "  test          Run tests"
	@echo "  test-coverage Run tests with coverage report"
	@echo "  clean         Remove build artifacts"
	@echo "  install       Install to GOPATH/bin"
	@echo "  init          Initialize a new database in ./data"
	@echo "  run           Build and run the database"
	@echo "  fmt           Format code"
	@echo "  lint          Run linter"
	@echo "  deps          Download dependencies"
	@echo "  dev-setup     Set up development environment"
	@echo "  dev           Format, test, and build (development cycle)"
	@echo "  help          Show this help"
