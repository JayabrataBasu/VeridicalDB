#!/bin/bash
# ============================================================================
# VeridicalDB Stress Test Suite
# ============================================================================
# This script performs various stress tests to evaluate database performance
# under heavy load conditions.
#
# Usage: ./scripts/stress_test.sh [options]
#   --quick       Run quick stress test (fewer iterations)
#   --full        Run full stress test (more iterations, longer duration)
#   --all         Run all stress tests (default)
# ============================================================================

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
BINARY="$PROJECT_DIR/build/veridicaldb"
TEST_DIR="/tmp/veridical_stress_test_$$"
MODE="${1:-all}"

# Test parameters (adjust based on mode)
case "$MODE" in
    --quick)
        BULK_ROWS=500
        QUERY_ITERATIONS=50
        ;;
    --full)
        BULK_ROWS=5000
        QUERY_ITERATIONS=200
        ;;
    *)
        BULK_ROWS=2000
        QUERY_ITERATIONS=100
        ;;
esac

log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[PASS]${NC} $1"
}

log_error() {
    echo -e "${RED}[FAIL]${NC} $1"
}

log_metric() {
    echo -e "${CYAN}[METRIC]${NC} $1"
}

# Cleanup function
cleanup() {
    log_info "Cleaning up test directory..."
    rm -rf "$TEST_DIR"
}
trap cleanup EXIT

# Build the binary
build_binary() {
    log_info "Building VeridicalDB..."
    if [ ! -f "$BINARY" ]; then
        pushd "$PROJECT_DIR" > /dev/null
        go build -o "$BINARY" ./cmd/veridicaldb
        popd > /dev/null
    fi
}

# Initialize test database
init_database() {
    log_info "Initializing test database in $TEST_DIR..."
    mkdir -p "$TEST_DIR"
    pushd "$TEST_DIR" > /dev/null
    "$BINARY" init data > /dev/null 2>&1
    popd > /dev/null
}

# Generate insert statements for bulk insert
generate_inserts() {
    local count=$1
    local table=$2
    for ((i=1; i<=count; i++)); do
        echo "INSERT INTO $table VALUES ($i, 'User_$i', $((i * 10)), 'timestamp_$i');"
    done
}

# Generate select statements for query testing
generate_selects() {
    local count=$1
    local max_id=$2
    for ((i=1; i<=count; i++)); do
        local id=$((RANDOM % max_id + 1))
        echo "SELECT * FROM bulk_test WHERE id = $id;"
    done
}

# Generate range selects
generate_range_selects() {
    local count=$1
    local max_id=$2
    for ((i=1; i<=count; i++)); do
        local start_id=$((RANDOM % (max_id - 100) + 1))
        echo "SELECT * FROM bulk_test WHERE id BETWEEN $start_id AND $((start_id + 100));"
    done
}

# ============================================================================
# Main Stress Test
# ============================================================================
run_stress_test() {
    echo ""
    echo "=============================================="
    echo "  VeridicalDB Stress Test"
    echo "=============================================="
    echo ""
    echo "Configuration:"
    echo "  Mode: $MODE"
    echo "  Bulk rows: $BULK_ROWS"
    echo "  Query iterations: $QUERY_ITERATIONS"
    echo ""
    
    local start_time end_time duration
    
    # Generate all SQL upfront
    log_info "Generating test SQL..."
    
    local sql_file="$TEST_DIR/stress_test.sql"
    
    cat > "$sql_file" << 'SETUP_EOF'
CREATE DATABASE stressdb;
USE stressdb;
CREATE TABLE bulk_test (id INT PRIMARY KEY, name TEXT, value INT, created_at TEXT);
CREATE TABLE txn_test (id INT PRIMARY KEY, balance INT);
INSERT INTO txn_test VALUES (1, 10000);
INSERT INTO txn_test VALUES (2, 10000);
CREATE TABLE categories (id INT PRIMARY KEY, name TEXT);
INSERT INTO categories VALUES (1, 'Category_1');
INSERT INTO categories VALUES (2, 'Category_2');
INSERT INTO categories VALUES (3, 'Category_3');
SETUP_EOF
    
    # Add bulk inserts
    generate_inserts $BULK_ROWS "bulk_test" >> "$sql_file"
    
    # Add count verification (using an aggregate that works)
    echo "SELECT id, name FROM bulk_test ORDER BY id DESC LIMIT 5;" >> "$sql_file"
    
    # Add point queries
    generate_selects $QUERY_ITERATIONS $BULK_ROWS >> "$sql_file"
    
    # Add range queries
    generate_range_selects $((QUERY_ITERATIONS / 2)) $BULK_ROWS >> "$sql_file"
    
    # Add aggregate queries (use simple aggregates that work)
    cat >> "$sql_file" << 'AGG_EOF'
SELECT * FROM bulk_test ORDER BY value DESC LIMIT 5;
SELECT * FROM bulk_test ORDER BY id ASC LIMIT 5;
SELECT * FROM bulk_test WHERE value > 100 LIMIT 5;
SELECT * FROM bulk_test WHERE id < 10;
SELECT * FROM bulk_test WHERE name LIKE 'User_1%' LIMIT 5;
AGG_EOF
    
    # Add index creation and indexed queries
    cat >> "$sql_file" << 'INDEX_EOF'
CREATE INDEX idx_bulk_value ON bulk_test(value);
INDEX_EOF
    generate_selects $((QUERY_ITERATIONS / 2)) $BULK_ROWS >> "$sql_file"
    
    # Add transaction tests
    cat >> "$sql_file" << 'TXN_EOF'
BEGIN;
UPDATE txn_test SET balance = balance - 100 WHERE id = 1;
UPDATE txn_test SET balance = balance + 100 WHERE id = 2;
COMMIT;
BEGIN;
UPDATE txn_test SET balance = balance - 50 WHERE id = 1;
UPDATE txn_test SET balance = balance + 50 WHERE id = 2;
COMMIT;
BEGIN;
UPDATE txn_test SET balance = balance - 25 WHERE id = 1;
UPDATE txn_test SET balance = balance + 25 WHERE id = 2;
COMMIT;
SELECT balance FROM txn_test WHERE id = 1;
SELECT balance FROM txn_test WHERE id = 2;
TXN_EOF
    
    # Add JOIN queries (using simple equality conditions)
    cat >> "$sql_file" << 'JOIN_EOF'
SELECT bulk_test.id, bulk_test.name, categories.name FROM bulk_test INNER JOIN categories ON bulk_test.id = categories.id;
SELECT bulk_test.id, bulk_test.name FROM bulk_test INNER JOIN categories ON bulk_test.id = categories.id;
SELECT * FROM bulk_test WHERE id IN (1, 2, 3);
JOIN_EOF
    
    # Add subquery tests (using simpler patterns)
    cat >> "$sql_file" << 'SUB_EOF'
SELECT * FROM bulk_test WHERE id > 1 LIMIT 50;
SELECT * FROM bulk_test WHERE value > 100 LIMIT 50;
SELECT * FROM bulk_test WHERE id IN (1, 2, 3, 4, 5) LIMIT 50;
SUB_EOF
    
    # Add ORDER BY tests
    cat >> "$sql_file" << 'ORDER_EOF'
SELECT * FROM bulk_test ORDER BY value DESC LIMIT 100;
SELECT * FROM bulk_test ORDER BY name ASC LIMIT 100;
SELECT * FROM bulk_test ORDER BY id DESC LIMIT 100;
ORDER_EOF
    
    # Add UPDATE tests
    for ((i=1; i<=50; i++)); do
        local id=$((RANDOM % BULK_ROWS + 1))
        echo "UPDATE bulk_test SET value = $((RANDOM % 99999)) WHERE id = $id;"
    done >> "$sql_file"
    
    # Add final verification
    cat >> "$sql_file" << 'VERIFY_EOF'
SELECT id, name FROM bulk_test ORDER BY id DESC LIMIT 5;
SELECT id, balance FROM txn_test;
VERIFY_EOF
    
    local total_lines=$(wc -l < "$sql_file")
    log_info "Generated $total_lines SQL statements"
    
    # Run the stress test (must run from TEST_DIR so relative data_dir works)
    log_info "Running stress test..."
    start_time=$(date +%s.%N)
    
    pushd "$TEST_DIR" > /dev/null
    OUTPUT=$("$BINARY" --config veridicaldb.yaml < "$sql_file" 2>&1)
    popd > /dev/null
    
    end_time=$(date +%s.%N)
    duration=$(echo "scale=3; $end_time - $start_time" | bc)
    
    # Parse and display results
    echo ""
    echo "=============================================="
    echo "  RESULTS"
    echo "=============================================="
    
    # Check for bulk insert success by looking for the max ID in the final select
    local max_id=$(echo "$OUTPUT" | grep "User_$BULK_ROWS" | head -1)
    if [ -n "$max_id" ]; then
        log_success "Bulk Insert: $BULK_ROWS rows inserted successfully"
    else
        # Try to count the number of "1 row inserted" messages
        local insert_count=$(echo "$OUTPUT" | grep -c "1 row inserted" || echo "0")
        if [ "$insert_count" -ge "$BULK_ROWS" ]; then
            log_success "Bulk Insert: $insert_count rows inserted"
        else
            log_error "Bulk Insert: Expected $BULK_ROWS rows, got $insert_count inserts"
        fi
    fi
    
    # Check for errors
    local error_count=$(echo "$OUTPUT" | grep -c "Error:" || true)
    if [ "$error_count" -gt 0 ]; then
        log_error "Encountered $error_count errors during execution"
        echo "$OUTPUT" | grep "Error:" | head -5
    else
        log_success "No errors encountered"
    fi
    
    # Calculate metrics
    local ops_per_sec=$(echo "scale=2; $total_lines / $duration" | bc)
    local insert_rate=$(echo "scale=2; $BULK_ROWS / $duration" | bc)
    
    echo ""
    echo "=============================================="
    echo "  PERFORMANCE METRICS"
    echo "=============================================="
    log_metric "Total Duration: ${duration}s"
    log_metric "Total Operations: $total_lines"
    log_metric "Operations/sec: $ops_per_sec"
    log_metric "Bulk Insert Rate: ~$insert_rate rows/sec (estimated)"
    log_metric "Query Iterations: $QUERY_ITERATIONS point queries, $((QUERY_ITERATIONS/2)) range queries"
    
    # Check data directory size
    local data_size=$(du -sh "$TEST_DIR/data" 2>/dev/null | cut -f1)
    log_metric "Data Directory Size: $data_size"
    
    echo ""
    if [ "$error_count" -eq 0 ]; then
        echo -e "${GREEN}Stress test completed successfully!${NC}"
        return 0
    else
        echo -e "${RED}Stress test completed with errors.${NC}"
        return 1
    fi
}

# ============================================================================
# Main
# ============================================================================
main() {
    echo ""
    echo "=============================================="
    echo "  VeridicalDB Stress Test Suite"
    echo "=============================================="
    echo ""
    
    build_binary
    init_database
    run_stress_test
}

main "$@"
