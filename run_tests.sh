#!/bin/bash

# Enhanced run_tests.sh script with intelligent parallel execution and CI reporting support
# Usage: ./run_tests.sh [options] [pytest arguments]
# 
# Options:
#   --serial                                         # Run tests sequentially
#   --ci                                            # Enable CI mode with test reporting
#   --junit-xml <file>                              # Generate JUnit XML report
#   --html-report <file>                            # Generate HTML report
#   --copy-reports                                  # Copy reports from container to host
#   -n <num>                                        # Number of parallel workers (overrides defaults)
#   
# Default Parallel Behavior:
#   Local:    -n auto (CPU core detection, ~4-14 workers)
#   CI/GitHub: -n 2 (conservative for GitHub Actions runners)
#   
# Examples:
#   ./run_tests.sh                                    # Run all tests (intelligent parallel)
#   ./run_tests.sh --serial                          # Run all tests (sequential)
#   ./run_tests.sh --ci                              # Run with CI reporting (auto-detected)
#   ./run_tests.sh -k "mariadb"                      # Run only MariaDB tests
#   ./run_tests.sh tests/integration/ddl/            # Run only DDL tests
#   ./run_tests.sh -x -v -s                         # Run with specific pytest flags
#   ./run_tests.sh -n 4                             # Force 4 parallel workers

echo "🐳 Starting Docker services..."

# Phase 1.75: Pre-test infrastructure monitoring
if [ -f "tools/test_monitor.py" ]; then
    echo "🔍 Phase 1.75: Running infrastructure health check..."
    python3 tools/test_monitor.py --check-processes --performance-baseline
    MONITOR_EXIT_CODE=$?
    if [ $MONITOR_EXIT_CODE -eq 1 ]; then
        echo "❌ Infrastructure health check failed - aborting test execution"
        exit 1
    elif [ $MONITOR_EXIT_CODE -eq 2 ]; then
        echo "⚠️  Infrastructure warnings detected - proceeding with caution"
    fi
fi

docker compose -f docker-compose-tests.yaml up --force-recreate --wait -d

# Get the container ID
CONTAINER_ID=$(docker ps | grep -E "(mysql_ch_replicator_src-replicator|mysql_ch_replicator-replicator)" | awk '{print $1}')

if [ -z "$CONTAINER_ID" ]; then
    echo "❌ Error: Could not find replicator container"
    exit 1
fi

echo "🧪 Running tests in container $CONTAINER_ID..."

# Parse arguments
PARALLEL_ARGS=""
PYTEST_ARGS=""
SERIAL_MODE=false
CI_MODE=false
JUNIT_XML=""
HTML_REPORT=""
COPY_REPORTS=false
SKIP_NEXT=false

rm -rf binlog*

# Set defaults for CI environment
if [ "$CI" = "true" ] || [ "$GITHUB_ACTIONS" = "true" ]; then
    CI_MODE=true
    JUNIT_XML="test-results.xml"
    HTML_REPORT="test-report.html"
    COPY_REPORTS=true
fi

for i in "${!@}"; do
    if [ "$SKIP_NEXT" = true ]; then
        SKIP_NEXT=false
        continue
    fi
    
    arg="${@:$i:1}"
    next_arg="${@:$((i+1)):1}"
    
    case $arg in
        --serial)
            SERIAL_MODE=true
            ;;
        --ci)
            CI_MODE=true
            JUNIT_XML="test-results.xml"
            HTML_REPORT="test-report.html"
            COPY_REPORTS=true
            ;;
        --junit-xml)
            JUNIT_XML="$next_arg"
            SKIP_NEXT=true
            ;;
        --html-report)
            HTML_REPORT="$next_arg"
            SKIP_NEXT=true
            ;;
        --copy-reports)
            COPY_REPORTS=true
            ;;
        -n|--numprocesses)
            PARALLEL_ARGS="$PARALLEL_ARGS $arg $next_arg"
            SKIP_NEXT=true
            ;;
        -n*)
            PARALLEL_ARGS="$PARALLEL_ARGS $arg"
            ;;
        *)
            PYTEST_ARGS="$PYTEST_ARGS $arg"
            ;;
    esac
done

# Build reporting arguments
REPORTING_ARGS=""
if [ -n "$JUNIT_XML" ]; then
    REPORTING_ARGS="$REPORTING_ARGS --junitxml=$JUNIT_XML"
fi
if [ -n "$HTML_REPORT" ]; then
    REPORTING_ARGS="$REPORTING_ARGS --html=$HTML_REPORT --self-contained-html"
fi

# Function to copy reports from container
copy_reports() {
    if [ "$COPY_REPORTS" = true ]; then
        echo "📋 Copying test reports from container..."
        if [ -n "$JUNIT_XML" ]; then
            docker cp "$CONTAINER_ID:/app/$JUNIT_XML" "./$JUNIT_XML" 2>/dev/null || echo "⚠️  Warning: Could not copy JUnit XML report"
        fi
        if [ -n "$HTML_REPORT" ]; then
            docker cp "$CONTAINER_ID:/app/$HTML_REPORT" "./$HTML_REPORT" 2>/dev/null || echo "⚠️  Warning: Could not copy HTML report"
        fi
    fi
}

# Function to cleanup on exit
cleanup() {
    local exit_code=$?
    local end_time=$(date +%s)
    local total_runtime=$((end_time - start_time))
    
    copy_reports
    rm -rf binlog*
    # Phase 1.75: Performance tracking and reporting
    echo "⏱️  Total runtime: ${total_runtime}s"
    
    # Performance baseline reporting (45s baseline)
    if [ $total_runtime -gt 90 ]; then
        echo "🚨 PERFORMANCE ALERT: Runtime ${total_runtime}s exceeds critical threshold (90s)"
    elif [ $total_runtime -gt 60 ]; then
        echo "⚠️  Performance warning: Runtime ${total_runtime}s exceeds baseline (60s threshold)"
    elif [ $total_runtime -le 45 ]; then
        echo "✅ Performance excellent: Runtime within baseline (≤45s)"
    else
        echo "✅ Performance good: Runtime within acceptable range (≤60s)"
    fi
    
    # Phase 1.75: Post-test infrastructure monitoring
    if [ -f "tools/test_monitor.py" ] && [ $exit_code -eq 0 ]; then
        echo "🔍 Phase 1.75: Running post-test infrastructure validation..."
        python3 tools/test_monitor.py --check-processes
        POST_MONITOR_EXIT_CODE=$?
        if [ $POST_MONITOR_EXIT_CODE -eq 1 ]; then
            echo "⚠️  Post-test infrastructure issues detected - may indicate test-induced problems"
        fi
    fi
    
    echo "🐳 Test execution completed with exit code: $exit_code"
    exit $exit_code
}
trap cleanup EXIT

# Phase 1.75: Start timing for performance monitoring
start_time=$(date +%s)


if [ "$SERIAL_MODE" = true ]; then
    echo "🐌 Running tests in serial mode$([ "$CI_MODE" = true ] && echo " (CI mode)")  "
    docker exec -w /app/ -i $CONTAINER_ID python3 -m pytest -x -v -s tests/ $REPORTING_ARGS $PYTEST_ARGS
elif [ -n "$PARALLEL_ARGS" ]; then
    echo "⚙️  Running tests with custom parallel configuration$([ "$CI_MODE" = true ] && echo " (CI mode)")  "
    docker exec -w /app/ -i $CONTAINER_ID python3 -m pytest $PARALLEL_ARGS -x -v -s tests/ $REPORTING_ARGS $PYTEST_ARGS
else
    # Default: Intelligent parallel execution with CI-aware scaling
    if [ "$CI" = "true" ] || [ "$GITHUB_ACTIONS" = "true" ]; then
        # Conservative defaults for GitHub Actions runners (2 CPU cores typically)
        echo "🚀 Running tests in parallel mode (CI-optimized: 2 workers)$([ "$CI_MODE" = true ] && echo " (CI mode)")  "
        docker exec -w /app/ -i $CONTAINER_ID python3 -m pytest -n 2 --dist worksteal --maxfail=5 -v tests/ $REPORTING_ARGS $PYTEST_ARGS
    else
        # Conservative parallelism for local development to avoid resource contention
        echo "🚀 Running tests in parallel mode (local-optimized: 4 workers)$([ "$CI_MODE" = true ] && echo " (CI mode)")  "
        docker exec -w /app/ -i $CONTAINER_ID python3 -m pytest -n 4 --dist worksteal --maxfail=50 -v tests/ $REPORTING_ARGS $PYTEST_ARGS
    fi
fi