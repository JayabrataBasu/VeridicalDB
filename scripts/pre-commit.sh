#!/usr/bin/env bash
set -euo pipefail

echo "Running pre-commit checks..."

if ! command -v golangci-lint >/dev/null 2>&1; then
  echo "golangci-lint not found. Run 'make dev-setup' to install it." >&2
  exit 1
fi

# Lint (fast)
golangci-lint run ./... --deadline=3m

# Run quick tests
go test ./... -short -timeout 5m

echo "pre-commit checks passed"
