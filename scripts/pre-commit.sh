#!/usr/bin/env bash
set -euo pipefail

echo "Running pre-commit checks..."

LINTER="$(command -v golangci-lint 2>/dev/null || true)"
if [ -z "$LINTER" ] && [ -x "$(go env GOPATH)/bin/golangci-lint" ]; then
  LINTER="$(go env GOPATH)/bin/golangci-lint"
fi

if [ -z "$LINTER" ]; then
  echo "golangci-lint not found. Run 'make dev-setup' to install it or add \
$(go env GOPATH)/bin to your PATH." >&2
  exit 1
fi

# Lint (fast)
$LINTER run ./... --timeout 3m

# Run quick tests
go test ./... -short -timeout 5m

echo "pre-commit checks passed"
