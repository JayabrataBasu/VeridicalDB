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
WORKLOAD_THRESHOLDS=""
MAX_CV_PERCENT=15
CV_MARGIN_PERCENT=3
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
    --workload-thresholds S   Per-workload thresholds, e.g.
                                                        point_lookup=5,range_scan=5,mixed_oltp=10,parse_cache=8
    --max-cv-percent N        Maximum allowed current CV%% before failing (default: 15)
    --cv-margin-percent N     Allowed CV%% increase vs baseline before failing (default: 3)
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
        --workload-thresholds)
            WORKLOAD_THRESHOLDS="$2"
            shift 2
            ;;
        --max-cv-percent)
            MAX_CV_PERCENT="$2"
            shift 2
            ;;
        --cv-margin-percent)
            CV_MARGIN_PERCENT="$2"
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
if [[ -n "$WORKLOAD_THRESHOLDS" ]]; then
    echo "[phase3-gate] workload thresholds: $WORKLOAD_THRESHOLDS"
fi
echo "[phase3-gate] variance guard: max_cv=${MAX_CV_PERCENT}% cv_margin=${CV_MARGIN_PERCENT}%"

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
    echo "workload,threshold_percent,baseline_p95,current_p95,p95_delta_percent,baseline_qps,current_qps,qps_delta_percent,baseline_cv,current_cv,cv_delta_percent,status,reason"
    awk -F, 'NR>1 {print $1","$4","$9","$6}' "$BASELINE_SUMMARY" | sort > "$OUTDIR/.baseline_metrics.tmp"
    awk -F, 'NR>1 {print $1","$4","$9","$6}' "$CURRENT_SUMMARY" | sort > "$OUTDIR/.current_metrics.tmp"

    join -t, -1 1 -2 1 "$OUTDIR/.baseline_metrics.tmp" "$OUTDIR/.current_metrics.tmp" |
    awk -F, -v threshold="$THRESHOLD_PERCENT" -v thresholds="$WORKLOAD_THRESHOLDS" -v maxCV="$MAX_CV_PERCENT" -v cvMargin="$CV_MARGIN_PERCENT" '
        BEGIN {
            n = split(thresholds, arr, /[,;]/)
            for (i = 1; i <= n; i++) {
                if (arr[i] == "") {
                    continue
                }
                split(arr[i], kv, "=")
                if (length(kv[1]) > 0 && length(kv[2]) > 0) {
                    perThreshold[kv[1]] = kv[2] + 0
                }
            }
        }
        {
            workload = $1
            base = $2 + 0
            baseQPS = $3 + 0
            baseCV = $4 + 0
            cur = $5 + 0
            curQPS = $6 + 0
            curCV = $7 + 0

            workloadThreshold = threshold + 0
            if (workload in perThreshold) {
                workloadThreshold = perThreshold[workload]
            }

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

            cvDelta = curCV - baseCV

            reason = ""
            failed = 0

            # Fail if p95 increases beyond threshold OR if QPS drops beyond threshold.
            if (p95Delta > workloadThreshold) {
                failed = 1
                reason = reason "p95_regression;"
            }
            if (qpsDelta < (-1 * workloadThreshold)) {
                failed = 1
                reason = reason "qps_regression;"
            }

            # Fail on high-variance runs only when the run is both noisy and meaningfully noisier than baseline.
            if (curCV > maxCV && cvDelta > cvMargin) {
                failed = 1
                reason = reason "variance_high;"
            }

            status = (failed ? "FAIL" : "PASS")
            if (reason == "") {
                reason = "ok"
            }

            printf "%s,%.2f,%.6f,%.6f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%s,%s\n", workload, workloadThreshold, base, cur, p95Delta, baseQPS, curQPS, qpsDelta, baseCV, curCV, cvDelta, status, reason
        }
    '
} > "$RESULT_CSV"

rm -f "$OUTDIR/.baseline_metrics.tmp" "$OUTDIR/.current_metrics.tmp"

echo "[phase3-gate] comparison report: $RESULT_CSV"
cat "$RESULT_CSV"

FAIL_COUNT="$(awk -F, 'NR>1 && $12 == "FAIL" {c++} END {print c+0}' "$RESULT_CSV")"
if [[ "$FAIL_COUNT" -gt 0 ]]; then
    echo "[phase3-gate] FAILED: $FAIL_COUNT workload(s) exceeded regression or variance thresholds" >&2
    exit 2
fi

echo "[phase3-gate] PASSED: no workload exceeded regression or variance thresholds"
