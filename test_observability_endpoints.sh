#!/bin/bash
# Test script for VeridicalDB observability endpoints with timing

set -e

OBS_PORT=8081
BASE_URL="http://localhost:${OBS_PORT}"

# Color output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${BLUE}╔═══════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║   Testing VeridicalDB Observability Endpoints            ║${NC}"
echo -e "${BLUE}║   Base URL: ${BASE_URL}                       ║${NC}"
echo -e "${BLUE}╚═══════════════════════════════════════════════════════════╝${NC}"
echo ""

# Function to test an endpoint with timing
test_endpoint() {
    local name="$1"
    local endpoint="$2"
    local extra_args="${3:-}"
    
    echo -e "${YELLOW}━━━ Testing: ${name} ━━━${NC}"
    echo -e "${BLUE}Endpoint:${NC} ${endpoint}"
    
    # Use curl with timing (-w) and store response
    start_time=$(date +%s.%N)
    
    if [ -n "$extra_args" ]; then
        response=$(curl -s -w "\n%{http_code}\n%{time_total}" $extra_args "${BASE_URL}${endpoint}" 2>&1)
    else
        response=$(curl -s -w "\n%{http_code}\n%{time_total}" "${BASE_URL}${endpoint}" 2>&1)
    fi
    
    exit_code=$?
    end_time=$(date +%s.%N)
    
    if [ $exit_code -eq 0 ]; then
        # Extract HTTP code and timing from last two lines
        http_code=$(echo "$response" | tail -2 | head -1)
        time_total=$(echo "$response" | tail -1)
        body=$(echo "$response" | head -n -2)
        
        if [ "$http_code" -eq 200 ]; then
            echo -e "${GREEN}✓ Status: HTTP ${http_code}${NC}"
            echo -e "${GREEN}✓ Response Time: ${time_total}s${NC}"
            
            # Pretty print JSON if possible, otherwise show first 500 chars
            if command -v jq &> /dev/null && echo "$body" | jq . &> /dev/null; then
                echo -e "${BLUE}Response:${NC}"
                echo "$body" | jq . | head -20
            else
                echo -e "${BLUE}Response (first 500 chars):${NC}"
                echo "$body" | head -c 500
                echo ""
            fi
        else
            echo -e "${RED}✗ Status: HTTP ${http_code}${NC}"
            echo -e "${YELLOW}Response Time: ${time_total}s${NC}"
            echo -e "${RED}Response:${NC} $body"
        fi
    else
        echo -e "${RED}✗ Failed: Connection error (exit code ${exit_code})${NC}"
        echo -e "${RED}Is the server running on port ${OBS_PORT}?${NC}"
    fi
    
    echo ""
}

# Test if server is reachable
echo -e "${BLUE}Checking if observability server is reachable...${NC}"
if ! curl -s --max-time 2 "${BASE_URL}/health" > /dev/null 2>&1; then
    echo -e "${RED}✗ ERROR: Cannot reach observability server at ${BASE_URL}${NC}"
    echo -e "${YELLOW}Make sure VeridicalDB server is running with observability enabled.${NC}"
    echo ""
    echo "Start the server with:"
    echo "  ./server --config config.yaml"
    echo ""
    echo "Or in the background:"
    echo "  ./server --config config.yaml > server.log 2>&1 &"
    exit 1
fi
echo -e "${GREEN}✓ Server is reachable${NC}"
echo ""

# Test each endpoint
test_endpoint "Health Check" "/health"
test_endpoint "Readiness Probe" "/ready"
test_endpoint "Liveness Probe" "/live"
test_endpoint "Prometheus Metrics" "/metrics"
test_endpoint "pprof Index" "/debug/pprof/"
test_endpoint "Goroutine Profile" "/debug/pprof/goroutine?debug=1"

# Summary
echo -e "${BLUE}╔═══════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║   All Observability Endpoint Tests Complete              ║${NC}"
echo -e "${BLUE}╚═══════════════════════════════════════════════════════════╝${NC}"
echo ""
echo -e "${GREEN}Available Endpoints:${NC}"
echo "  - GET ${BASE_URL}/metrics             (Prometheus metrics)"
echo "  - GET ${BASE_URL}/health              (Health check)"
echo "  - GET ${BASE_URL}/ready               (Readiness probe)"
echo "  - GET ${BASE_URL}/live                (Liveness probe)"
echo "  - GET ${BASE_URL}/debug/pprof/        (pprof index)"
echo "  - GET ${BASE_URL}/debug/pprof/heap    (Heap profile)"
echo "  - GET ${BASE_URL}/debug/pprof/profile (CPU profile, 30s)"
echo ""
echo -e "${YELLOW}For CPU profiling:${NC}"
echo "  go tool pprof ${BASE_URL}/debug/pprof/profile"
echo ""
echo -e "${YELLOW}For heap analysis:${NC}"
echo "  go tool pprof ${BASE_URL}/debug/pprof/heap"
echo ""
