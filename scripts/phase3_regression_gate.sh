#!/usr/bin/env bash
set -euo pipefail

# Phase 3 performance regression gate.
# Runs benchmark harness and compares p95 latency and QPS against a baseline summary CSV.
# Exits non-zero if any workload regresses beyond the threshold.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
BENCH_SCRIPT="$PROJECT_DIR/scripts/phase3_benchmark.sh"

BASELINE_SUMMARY=""
THRESHOLD_PERCENT=5
RUNS=5
ROWS=2000
LOOKUPS=1000
RANGES=400
MIXED_OPS=1200
PARSE_OPS=1200
SEED=1337
OUTDIR=""

usage() {
    cat <<EOF
Usage: $0 --baseline-summary PATH [options]

Required:
  --baseline-summary PATH   Path to baseline summary.csv file

Optional:
  --threshold-percent N     Allowed p95 regression percentage (default: 5)
  --runs N                  Benchmark runs per workload (default: 5)
  --rows N                  Seed rows (default: 2000)
  --lookups N               Point lookups per run (default: 1000)
  --ranges N                Range queries per run (default: 400)
  --mixed-ops N             Mixed workload operations per run (default: 1200)
    --parse-ops N             Parse-cache workload operations per run (default: 1200)
  --seed N                  Deterministic seed (default: 1337)
  --outdir PATH             Output directory for this gate run
  --help                    Show help
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --baseline-summary)
            BASELINE_SUMMARY="$2"
            shift 2
            ;;
        --threshold-percent)
            THRESHOLD_PERCENT="$2"
            shift 2
            ;;
        --runs)
            RUNS="$2"
            shift 2
            ;;
        --rows)
            ROWS="$2"
            shift 2
            ;;
        --lookups)
            LOOKUPS="$2"
            shift 2
            ;;
        --ranges)
            RANGES="$2"
            shift 2
            ;;
        --mixed-ops)
            MIXED_OPS="$2"
            shift 2
            ;;
        --parse-ops)
            PARSE_OPS="$2"
            shift 2
            ;;
        --seed)
            SEED="$2"
            shift 2
            ;;
        --outdir)
            OUTDIR="$2"
            shift 2
            ;;
        --help)
            usage
            exit 0
            ;;
        *)
            echo "Unknown option: $1" >&2
            usage
            exit 1
            ;;
    esac
done

if [[ -z "$BASELINE_SUMMARY" ]]; then
    echo "--baseline-summary is required" >&2
    usage
    exit 1
fi

if [[ ! -f "$BASELINE_SUMMARY" ]]; then
    # Try resolving relative to project dir for convenience in CI/scripts.
    if [[ "$BASELINE_SUMMARY" != /* ]] && [[ -f "$PROJECT_DIR/$BASELINE_SUMMARY" ]]; then
        BASELINE_SUMMARY="$PROJECT_DIR/$BASELINE_SUMMARY"
    else
        echo "Baseline summary not found: $BASELINE_SUMMARY" >&2
        exit 1
    fi
fi

if [[ -z "$OUTDIR" ]]; then
    TS="$(date -u +"%Y%m%dT%H%M%SZ")"
    OUTDIR="$PROJECT_DIR/.benchmarks/phase3/gate/$TS"
fi

mkdir -p "$OUTDIR"

echo "[phase3-gate] baseline: $BASELINE_SUMMARY"
echo "[phase3-gate] outdir:   $OUTDIR"
echo "[phase3-gate] threshold: ${THRESHOLD_PERCENT}%"

"$BENCH_SCRIPT" \
    --runs "$RUNS" \
    --rows "$ROWS" \
    --lookups "$LOOKUPS" \
    --ranges "$RANGES" \
    --mixed-ops "$MIXED_OPS" \
    --parse-ops "$PARSE_OPS" \
    --seed "$SEED" \
    --outdir "$OUTDIR"

CURRENT_SUMMARY="$OUTDIR/summary.csv"
if [[ ! -f "$CURRENT_SUMMARY" ]]; then
    echo "Current summary not found: $CURRENT_SUMMARY" >&2
    exit 1
fi

RESULT_CSV="$OUTDIR/regression_comparison.csv"
{
    echo "workload,baseline_p95,current_p95,p95_delta_percent,baseline_qps,current_qps,qps_delta_percent,status"
    awk -F, 'NR>1 {print $1","$4","$9}' "$BASELINE_SUMMARY" | sort > "$OUTDIR/.baseline_metrics.tmp"
    awk -F, 'NR>1 {print $1","$4","$9}' "$CURRENT_SUMMARY" | sort > "$OUTDIR/.current_metrics.tmp"

    join -t, -1 1 -2 1 "$OUTDIR/.baseline_metrics.tmp" "$OUTDIR/.current_metrics.tmp" |
    awk -F, -v threshold="$THRESHOLD_PERCENT" '
        {
            workload = $1
            base = $2 + 0
            baseQPS = $3 + 0
            cur = $4 + 0
            curQPS = $5 + 0

            if (base <= 0) {
                p95Delta = 0
            } else {
                p95Delta = ((cur - base) / base) * 100
            }

            if (baseQPS <= 0) {
                qpsDelta = 0
            } else {
                qpsDelta = ((curQPS - baseQPS) / baseQPS) * 100
            }

            # Fail if p95 increases beyond threshold OR if QPS drops beyond threshold.
            if (p95Delta > threshold || qpsDelta < (-1 * threshold)) {
                status = "FAIL"
            } else {
                status = "PASS"
            }

            printf "%s,%.6f,%.6f,%.2f,%.2f,%.2f,%.2f,%s\n", workload, base, cur, p95Delta, baseQPS, curQPS, qpsDelta, status
        }
    '
} > "$RESULT_CSV"

rm -f "$OUTDIR/.baseline_metrics.tmp" "$OUTDIR/.current_metrics.tmp"

echo "[phase3-gate] comparison report: $RESULT_CSV"
cat "$RESULT_CSV"

FAIL_COUNT="$(awk -F, 'NR>1 && $8 == "FAIL" {c++} END {print c+0}' "$RESULT_CSV")"
if [[ "$FAIL_COUNT" -gt 0 ]]; then
    echo "[phase3-gate] FAILED: $FAIL_COUNT workload(s) exceeded ${THRESHOLD_PERCENT}% regression threshold (p95 up or QPS down)" >&2
    exit 2
fi

echo "[phase3-gate] PASSED: no workload exceeded ${THRESHOLD_PERCENT}% regression threshold (p95 up or QPS down)"
