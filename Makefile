.PHONY: all release-local docker build

BIN=veridicaldb
VERSION?=v1.0.0

all: build

build:
	go build -o build/$(BIN) ./cmd/veridicaldb

release-local:
	@echo "Running local release script (cross-compile)..."
	./scripts/release.sh $(VERSION)

docker:
	@echo "Building Docker image"
	docker build -t veridicaldb:$(VERSION) .
# VeridicalDB Makefile
# Build, test, and manage the database

.PHONY: all build test clean install run init fmt lint help

# Build variables
BINARY_NAME=veridicaldb
VERSION?=v1.0.0
BUILD_DATE=$(shell date -u +"%Y-%m-%dT%H:%M:%SZ")
LDFLAGS=-ldflags "-X main.version=$(VERSION) -X main.buildDate=$(BUILD_DATE)"

# Directories
BUILD_DIR=./build
CMD_DIR=./cmd/server

# Default target
all: build

# Build the binary
build:
	@echo "Building $(BINARY_NAME)..."
	@mkdir -p $(BUILD_DIR)
	go build $(LDFLAGS) -o $(BUILD_DIR)/$(BINARY_NAME) $(CMD_DIR)
	@echo "Built: $(BUILD_DIR)/$(BINARY_NAME)"

# Build for multiple platforms
build-all: build-linux build-darwin build-windows

build-linux:
	@echo "Building for Linux..."
	GOOS=linux GOARCH=amd64 go build $(LDFLAGS) -o $(BUILD_DIR)/$(BINARY_NAME)-linux-amd64 $(CMD_DIR)
	GOOS=linux GOARCH=arm64 go build $(LDFLAGS) -o $(BUILD_DIR)/$(BINARY_NAME)-linux-arm64 $(CMD_DIR)

build-darwin:
	@echo "Building for macOS..."
	GOOS=darwin GOARCH=amd64 go build $(LDFLAGS) -o $(BUILD_DIR)/$(BINARY_NAME)-darwin-amd64 $(CMD_DIR)
	GOOS=darwin GOARCH=arm64 go build $(LDFLAGS) -o $(BUILD_DIR)/$(BINARY_NAME)-darwin-arm64 $(CMD_DIR)

build-windows:
	@echo "Building for Windows..."
	GOOS=windows GOARCH=amd64 go build $(LDFLAGS) -o $(BUILD_DIR)/$(BINARY_NAME)-windows-amd64.exe $(CMD_DIR)

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
	golangci-lint run

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
