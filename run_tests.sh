#!/bin/bash

# Enhanced run_tests.sh script with parallel execution and CI reporting support
# Usage: ./run_tests.sh [options] [pytest arguments]
# 
# Options:
#   --serial                                         # Run tests sequentially
#   --ci                                            # Enable CI mode with test reporting
#   --junit-xml <file>                              # Generate JUnit XML report
#   --html-report <file>                            # Generate HTML report
#   --copy-reports                                  # Copy reports from container to host
#   -n <num>                                        # Number of parallel workers
#   
# Examples:
#   ./run_tests.sh                                    # Run all tests (parallel)
#   ./run_tests.sh --serial                          # Run all tests (sequential)
#   ./run_tests.sh --ci                              # Run with CI reporting (parallel)
#   ./run_tests.sh -k "mariadb"                      # Run only MariaDB tests
#   ./run_tests.sh tests/integration/ddl/            # Run only DDL tests
#   ./run_tests.sh -x -v -s                         # Run with specific pytest flags
#   ./run_tests.sh -n 2                             # Run with 2 parallel workers

echo "🐳 Starting Docker services..."
docker compose -f docker-compose-tests.yaml up --force-recreate --no-deps --wait -d

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
    copy_reports
    echo "🐳 Test execution completed with exit code: $exit_code"
    exit $exit_code
}
trap cleanup EXIT

# Determine execution mode and run tests
if [ "$SERIAL_MODE" = true ]; then
    echo "🐌 Running tests in serial mode$([ "$CI_MODE" = true ] && echo " (CI mode)")..."
    docker exec -w /app/ -i $CONTAINER_ID python3 -m pytest -x -v -s tests/ $REPORTING_ARGS $PYTEST_ARGS
elif [ -n "$PARALLEL_ARGS" ]; then
    echo "⚙️  Running tests with custom parallel configuration$([ "$CI_MODE" = true ] && echo " (CI mode)")..."
    docker exec -w /app/ -i $CONTAINER_ID python3 -m pytest $PARALLEL_ARGS -x -v -s tests/ $REPORTING_ARGS $PYTEST_ARGS
else
    # Default: Auto-parallel execution
    echo "🚀 Running tests in parallel mode (auto-scaling)$([ "$CI_MODE" = true ] && echo " (CI mode)")..."
    docker exec -w /app/ -i $CONTAINER_ID python3 -m pytest -n auto --dist worksteal -x -v -s tests/ $REPORTING_ARGS $PYTEST_ARGS
fi