.PHONY: all release-local docker build build-cli build-server

BIN=veridicaldb
BINARY_NAME?=$(BIN)
VERSION?=v2.0.0

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

.PHONY: test clean install run init fmt lint help smoke-test stress-test phase3-benchmark phase3-benchmark-quick phase3-regression-gate phase3-regression-gate-quick

# Build variables
VERSION?=v2.0.0
BUILD_DATE=$(shell date -u +"%Y-%m-%dT%H:%M:%SZ")
LDFLAGS=-ldflags "-X main.version=$(VERSION) -X main.buildDate=$(BUILD_DATE)"

# Where instruments may install binaries (for go install)
GOBIN := $(shell go env GOBIN)
GOPATH := $(shell go env GOPATH)
GOLANGCI_BIN := $(if $(GOBIN),$(GOBIN)/golangci-lint,$(GOPATH)/bin/golangci-lint)

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

# Run Phase 3 benchmark baseline harness (Week 1)
phase3-benchmark: build
	@echo "Running Phase 3 benchmark harness..."
	./scripts/phase3_benchmark.sh

# Fast local benchmark sanity run
phase3-benchmark-quick: build
	@echo "Running quick Phase 3 benchmark harness..."
	./scripts/phase3_benchmark.sh --runs 3 --rows 800 --lookups 300 --ranges 120 --mixed-ops 360

# Run Phase 3 regression gate against baseline summary
# Usage:
#   make phase3-regression-gate BASELINE=.benchmarks/phase3/<ts>/summary.csv THRESHOLD=5
phase3-regression-gate: build
	@if [ -z "$(BASELINE)" ]; then \
		echo "Provide BASELINE path. Example:"; \
		echo "  make phase3-regression-gate BASELINE=.benchmarks/phase3/<ts>/summary.csv THRESHOLD=5"; \
		exit 1; \
	fi
	@echo "Running Phase 3 regression gate..."
	./scripts/phase3_regression_gate.sh --baseline-summary "$(BASELINE)" --threshold-percent "$(if $(THRESHOLD),$(THRESHOLD),5)"

# Fast local regression gate with smaller run profile
phase3-regression-gate-quick: build
	@if [ -z "$(BASELINE)" ]; then \
		echo "Provide BASELINE path. Example:"; \
		echo "  make phase3-regression-gate-quick BASELINE=.benchmarks/phase3/<ts>/summary.csv THRESHOLD=10"; \
		exit 1; \
	fi
	@echo "Running quick Phase 3 regression gate..."
	./scripts/phase3_regression_gate.sh --baseline-summary "$(BASELINE)" --threshold-percent "$(if $(THRESHOLD),$(THRESHOLD),10)" --runs 3 --rows 800 --lookups 300 --ranges 120 --mixed-ops 360

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
	@# Use golangci-lint from PATH (installed by CI action or locally)
	@if command -v golangci-lint >/dev/null 2>&1; then \
		echo "Using golangci-lint from PATH"; \
		golangci-lint run ./... --timeout 10m; \
	elif command -v $(GOLANGCI_BIN) >/dev/null 2>&1; then \
		echo "Using $(GOLANGCI_BIN)"; \
		$(GOLANGCI_BIN) run ./... --timeout 10m; \
	else \
		echo "golangci-lint not found, skipping lint step"; \
	fi
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
	go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.7.2

# Run a fast pre-commit check locally (lint + quick tests)
.PHONY: pre-commit
pre-commit: fmt
	@echo "Running pre-commit checks..."
	@if command -v golangci-lint >/dev/null 2>&1; then \
		golangci-lint run ./... --deadline=3m || exit 1; \
	else \
		echo "golangci-lint not installed; run 'make dev-setup'"; exit 1; \
	fi
	go test ./... -short -timeout 5m

# Install / uninstall the git pre-commit hook
.PHONY: install-hook uninstall-hook
install-hook:
	@echo "Installing git pre-commit hook..."
	@./scripts/install-hook.sh

uninstall-hook:
	@echo "Removing git pre-commit hook..."
	@rm -f .git/hooks/pre-commit || true


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
	@echo "  phase3-benchmark        Run full Week 1 Phase 3 baseline benchmark"
	@echo "  phase3-benchmark-quick  Run quick local Phase 3 benchmark sanity check"
	@echo "  phase3-regression-gate       Compare current benchmark p95 against a baseline"
	@echo "  phase3-regression-gate-quick Quick regression gate with smaller run profile"
	@echo "  help          Show this help"
