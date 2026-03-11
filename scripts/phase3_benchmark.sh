#!/usr/bin/env bash
set -euo pipefail

# Phase 3 benchmark harness.
# Produces repeatable latency/throughput/resource baselines for:
# - point lookup
# - range scan
# - mixed OLTP (70% read / 30% write)
# - parse cache / normalized repeated SQL

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
BINARY="$PROJECT_DIR/build/veridicaldb"
TIMESTAMP="$(date -u +"%Y%m%dT%H%M%SZ")"
OUTDIR="$PROJECT_DIR/.benchmarks/phase3/$TIMESTAMP"
RUNS=5
ROWS=2000
LOOKUPS=1000
RANGES=400
MIXED_OPS=1200
PARSE_OPS=1200
SEED=1337

usage() {
    cat <<EOF
Usage: $0 [options]

Options:
  --runs N          Number of runs per workload (default: 5)
  --rows N          Number of seed rows (default: 2000)
  --lookups N       Point lookup statements per run (default: 1000)
  --ranges N        Range scan statements per run (default: 400)
  --mixed-ops N     Mixed workload statements per run (default: 1200)
    --parse-ops N     Parse-cache statements per run (default: 1200)
  --seed N          Deterministic seed value used for query generation (default: 1337)
    --outdir PATH     Output directory (default: .benchmarks/phase3/<timestamp>)
  --help            Show this help
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
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

# Normalize relative outdir paths against project root so subshell cwd changes
# during execution do not break workload/log file references.
if [[ "$OUTDIR" != /* ]]; then
    OUTDIR="$PROJECT_DIR/$OUTDIR"
fi

mkdir -p "$OUTDIR"
WORKLOAD_DIR="$OUTDIR/workloads"
mkdir -p "$WORKLOAD_DIR"
RAW_CSV="$OUTDIR/raw_runs.csv"
REPORT_MD="$OUTDIR/baseline_report.md"
SUMMARY_CSV="$OUTDIR/summary.csv"

workload_statement_count() {
    local file="$1"
    wc -l <"$file" | awk '{print $1}'
}

if [[ ! -x "$BINARY" ]]; then
    echo "[phase3] building CLI binary at $BINARY"
    (cd "$PROJECT_DIR" && go build -o "$BINARY" ./cmd/veridicaldb)
fi

make_setup_sql() {
    local file="$1"
    {
        echo "CREATE DATABASE benchdb;"
        echo "USE benchdb;"
        echo "CREATE TABLE kv (id INT PRIMARY KEY, payload TEXT, v INT, updated_at TEXT);"
        for ((i = 1; i <= ROWS; i++)); do
            printf "INSERT INTO kv VALUES (%d, 'payload_%d', %d, '2026-03-10T00:00:00Z');\n" "$i" "$i" "$((i % 1000))"
        done
    } >"$file"
}

generate_point_workload() {
    local file="$1"
    local setup="$WORKLOAD_DIR/setup.sql"
    cat "$setup" >"$file"
    for ((i = 1; i <= LOOKUPS; i++)); do
        # Deterministic pseudo-random id sequence
        local id=$(( ((i * 97 + SEED) % ROWS) + 1 ))
        printf "SELECT * FROM kv WHERE id = %d;\n" "$id" >>"$file"
    done
}

generate_range_workload() {
    local file="$1"
    local setup="$WORKLOAD_DIR/setup.sql"
    cat "$setup" >"$file"
    local span=25
    for ((i = 1; i <= RANGES; i++)); do
        local start=$(( ((i * 53 + SEED) % (ROWS - span)) + 1 ))
        local end=$((start + span))
        printf "SELECT * FROM kv WHERE id BETWEEN %d AND %d;\n" "$start" "$end" >>"$file"
    done
}

generate_mixed_workload() {
    local file="$1"
    local setup="$WORKLOAD_DIR/setup.sql"
    cat "$setup" >"$file"

    local insert_base=$((ROWS + 1))
    local inserted=0

    for ((i = 1; i <= MIXED_OPS; i++)); do
        local lane=$((i % 10))

        if [[ "$lane" -lt 7 ]]; then
            local id=$(( ((i * 89 + SEED) % ROWS) + 1 ))
            printf "SELECT * FROM kv WHERE id = %d;\n" "$id" >>"$file"
        elif [[ "$lane" -lt 9 ]]; then
            local id=$(( ((i * 41 + SEED) % ROWS) + 1 ))
            local value=$(( (i * 17) % 10000 ))
            printf "UPDATE kv SET v = %d WHERE id = %d;\n" "$value" "$id" >>"$file"
        else
            local id=$((insert_base + inserted))
            local value=$(( (i * 29) % 10000 ))
            printf "INSERT INTO kv VALUES (%d, 'payload_new_%d', %d, '2026-03-10T00:00:00Z');\n" "$id" "$id" "$value" >>"$file"
            inserted=$((inserted + 1))
        fi
    done
}

generate_parse_cache_workload() {
    local file="$1"
    local setup="$WORKLOAD_DIR/setup.sql"
    cat "$setup" >"$file"

    for ((i = 1; i <= PARSE_OPS; i++)); do
        local id=$(( ((i * 97 + SEED) % ROWS) + 1 ))
        local lane=$((i % 4))

        case "$lane" in
            0)
                printf "SELECT * FROM kv WHERE id = %d;\n" "$id" >>"$file"
                ;;
            1)
                printf "  SELECT * FROM kv WHERE id = %d  \n" "$id" >>"$file"
                ;;
            2)
                printf "SELECT   *   FROM kv   WHERE   id = %d;\n" "$id" >>"$file"
                ;;
            *)
                printf "SELECT * FROM kv WHERE id=%d;\n" "$id" >>"$file"
                ;;
        esac
    done
}

run_workload() {
    local workload_name="$1"
    local workload_file="$2"
    local statement_count
    statement_count="$(workload_statement_count "$workload_file")"

    for ((run = 1; run <= RUNS; run++)); do
        local test_dir
        test_dir="$(mktemp -d /tmp/veridical_phase3_${workload_name}_${run}_XXXXXX)"
        local run_out="$OUTDIR/${workload_name}_run${run}.log"
        local time_out="$OUTDIR/${workload_name}_run${run}.time"

        (
            cd "$test_dir"
            "$BINARY" init data >/dev/null 2>&1

            if command -v /usr/bin/time >/dev/null 2>&1; then
                /usr/bin/time -f "%e,%M" -o "$time_out" "$BINARY" --config veridicaldb.yaml <"$workload_file" >"$run_out" 2>&1
            else
                local start_ns end_ns
                start_ns="$(date +%s%N)"
                "$BINARY" --config veridicaldb.yaml <"$workload_file" >"$run_out" 2>&1
                end_ns="$(date +%s%N)"
                local elapsed
                elapsed="$(awk -v s="$start_ns" -v e="$end_ns" 'BEGIN { printf "%.6f", (e-s)/1000000000 }')"
                echo "$elapsed,0" >"$time_out"
            fi
        )

        if grep -q "Error:" "$run_out"; then
            echo "[phase3] workload $workload_name run $run reported SQL errors" >&2
            tail -n 20 "$run_out" >&2
            rm -rf "$test_dir"
            exit 1
        fi

        local elapsed rss
        elapsed="$(cut -d, -f1 "$time_out")"
        rss="$(cut -d, -f2 "$time_out")"
        local qps
        qps="$(awk -v ops="$statement_count" -v t="$elapsed" 'BEGIN { if (t <= 0) print 0; else printf "%.2f", ops / t }')"
        echo "$workload_name,$run,$statement_count,$elapsed,$qps,$rss" >>"$RAW_CSV"

        echo "[phase3] $workload_name run $run/$RUNS elapsed=${elapsed}s qps=$qps rss_kb=$rss"
        rm -rf "$test_dir"
    done
}

write_summary_for_workload() {
    local workload="$1"

    mapfile -t durations < <(awk -F, -v w="$workload" '$1 == w {print $4}' "$RAW_CSV" | sort -n)
    mapfile -t qps_values < <(awk -F, -v w="$workload" '$1 == w {print $5}' "$RAW_CSV")
    mapfile -t rss_values < <(awk -F, -v w="$workload" '$1 == w {print $6}' "$RAW_CSV")

    local n="${#durations[@]}"
    if [[ "$n" -eq 0 ]]; then
        return
    fi

    local p95_index=$(( (95 * n + 99) / 100 - 1 ))
    if [[ "$p95_index" -lt 0 ]]; then
        p95_index=0
    fi
    if [[ "$p95_index" -ge "$n" ]]; then
        p95_index=$((n - 1))
    fi
    local p95="${durations[$p95_index]}"

    local metrics
    metrics="$(printf '%s\n' "${durations[@]}" | awk '
        BEGIN { min = 1e18; max = -1e18; sum = 0; n = 0 }
        {
            x = $1 + 0;
            vals[n] = x;
            if (x < min) min = x;
            if (x > max) max = x;
            sum += x;
            n++;
        }
        END {
            if (n == 0) {
                printf "0,0,0,0,0";
                exit;
            }
            mean = sum / n;
            var = 0;
            for (i = 0; i < n; i++) {
                d = vals[i] - mean;
                var += d * d;
            }
            stddev = (n > 1) ? sqrt(var / n) : 0;
            cv = (mean > 0) ? (stddev / mean) * 100 : 0;
            printf "%.6f,%.6f,%.2f,%.6f,%.6f", mean, stddev, cv, min, max;
        }
    ')"

    local mean stddev cv min max
    IFS=',' read -r mean stddev cv min max <<<"$metrics"

    local avg_rss
    avg_rss="$(printf '%s\n' "${rss_values[@]}" | awk '
        BEGIN { sum = 0; n = 0 }
        { sum += ($1 + 0); n++ }
        END { if (n == 0) print 0; else printf "%.2f", sum / n }
    ')"

    local avg_qps
    avg_qps="$(printf '%s\n' "${qps_values[@]}" | awk '
        BEGIN { sum = 0; n = 0 }
        { sum += ($1 + 0); n++ }
        END { if (n == 0) print 0; else printf "%.2f", sum / n }
    ')"

    echo "$workload,$n,$mean,$p95,$stddev,$cv,$min,$max,$avg_qps,$avg_rss" >>"$SUMMARY_CSV"
}

# Generate workloads
echo "[phase3] generating deterministic workloads (seed=$SEED)"
make_setup_sql "$WORKLOAD_DIR/setup.sql"
generate_point_workload "$WORKLOAD_DIR/point_lookup.sql"
generate_range_workload "$WORKLOAD_DIR/range_scan.sql"
generate_mixed_workload "$WORKLOAD_DIR/mixed_oltp.sql"
generate_parse_cache_workload "$WORKLOAD_DIR/parse_cache.sql"

# Run workloads
echo "workload,run,statement_count,elapsed_seconds,qps,max_rss_kb" >"$RAW_CSV"
run_workload "point_lookup" "$WORKLOAD_DIR/point_lookup.sql"
run_workload "range_scan" "$WORKLOAD_DIR/range_scan.sql"
run_workload "mixed_oltp" "$WORKLOAD_DIR/mixed_oltp.sql"
run_workload "parse_cache" "$WORKLOAD_DIR/parse_cache.sql"

echo "workload,runs,mean_seconds,p95_seconds,stddev_seconds,cv_percent,min_seconds,max_seconds,avg_qps,avg_rss_kb" >"$SUMMARY_CSV"
write_summary_for_workload "point_lookup"
write_summary_for_workload "range_scan"
write_summary_for_workload "mixed_oltp"
write_summary_for_workload "parse_cache"

{
    echo "# Phase 3 Baseline Report"
    echo
    echo "Generated: $(date -u +"%Y-%m-%dT%H:%M:%SZ")"
    echo
    echo "## Configuration"
    echo
    echo "- runs_per_workload: $RUNS"
    echo "- seed_rows: $ROWS"
    echo "- point_lookups: $LOOKUPS"
    echo "- range_queries: $RANGES"
    echo "- mixed_ops: $MIXED_OPS"
    echo "- parse_ops: $PARSE_OPS"
    echo "- seed: $SEED"
    echo
    echo "## Workload Summary"
    echo
    echo "| Workload | Runs | Mean (s) | p95 (s) | StdDev (s) | CV (%) | Min (s) | Max (s) | Avg QPS | Avg RSS (KB) |"
    echo "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|"
    awk -F, 'NR > 1 {
        printf "| %s | %s | %.6f | %.6f | %.6f | %.2f | %.6f | %.6f | %.2f | %.2f |\n", $1, $2, $3, $4, $5, $6, $7, $8, $9, $10
    }' "$SUMMARY_CSV"
    echo
    echo "## Raw Artifacts"
    echo
    echo '- Raw per-run CSV: `raw_runs.csv`'
    echo '- Summary CSV: `summary.csv`'
    echo '- Workload SQL files: `workloads/*.sql`'
    echo '- Per-run logs: `*_run*.log`'
} >"$REPORT_MD"

echo "[phase3] baseline complete"
echo "[phase3] report: $REPORT_MD"
echo "[phase3] summary: $SUMMARY_CSV"
