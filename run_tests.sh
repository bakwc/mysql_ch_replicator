#!/bin/bash

# Enhanced run_tests.sh script that accepts pytest parameters
# Usage: ./run_tests.sh [pytest arguments]
# Examples:
#   ./run_tests.sh                                    # Run all tests
#   ./run_tests.sh -k "mariadb"                      # Run only MariaDB tests
#   ./run_tests.sh tests/integration/ddl/            # Run only DDL tests
#   ./run_tests.sh -x -v -s                         # Run with specific pytest flags

echo "🐳 Starting Docker services..."
docker compose -f docker-compose-tests.yaml up --force-recreate --no-deps --wait -d

# Get the container ID
CONTAINER_ID=$(docker ps | grep -E "(mysql_ch_replicator_src-replicator|mysql_ch_replicator-replicator)" | awk '{print $1}')

if [ -z "$CONTAINER_ID" ]; then
    echo "❌ Error: Could not find replicator container"
    exit 1
fi

echo "🧪 Running tests in container $CONTAINER_ID..."

# Pass all arguments to pytest, with default arguments if none provided
if [ $# -eq 0 ]; then
    # Default test run with all tests
    docker exec -w /app/ -i $CONTAINER_ID python3 -m pytest -x -v -s tests/
else
    # Run with user-provided arguments
    docker exec -w /app/ -i $CONTAINER_ID python3 -m pytest -x -v -s "$@"
fi

TEST_EXIT_CODE=$?

echo "🐳 Test execution completed with exit code: $TEST_EXIT_CODE"
exit $TEST_EXIT_CODE