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

# Optional Phase 3 performance regression gate.
# Enable by setting PHASE3_BASELINE_SUMMARY to a summary.csv path.
if [ -n "${PHASE3_BASELINE_SUMMARY:-}" ]; then
  echo "Running optional Phase 3 regression gate..."
  THRESHOLD="${PHASE3_THRESHOLD_PERCENT:-10}"
  ./scripts/phase3_regression_gate.sh \
    --baseline-summary "$PHASE3_BASELINE_SUMMARY" \
    --threshold-percent "$THRESHOLD" \
    --runs 3 --rows 800 --lookups 300 --ranges 120 --mixed-ops 360
fi

echo "pre-commit checks passed"
