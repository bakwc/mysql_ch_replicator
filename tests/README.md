# Tests

This directory contains the test suite for mysql-ch-replicator, organized following pytest best practices.

## Structure

```
tests/
├── conftest.py              # Shared fixtures and test utilities
├── unit/                    # Unit tests (fast, isolated)
│   └── test_connection_pooling.py
├── integration/             # Integration tests (require external services)
│   ├── test_basic_replication.py
│   ├── test_data_types.py
│   └── test_schema_evolution.py
├── performance/             # Performance tests (long running)
│   └── test_performance.py
└── fixtures/                # Test data and configuration files
```

## Test Categories

### Unit Tests
- Fast tests that don't require external dependencies
- Test individual components in isolation
- Mock external dependencies when needed
- Run with: `pytest tests/unit/`

### Integration Tests
- Test complete workflows and component interactions
- Require MySQL and ClickHouse to be running
- Test real replication scenarios
- Run with: `pytest tests/integration/`

### Performance Tests
- Long-running tests that measure performance
- Marked as `@pytest.mark.optional` and `@pytest.mark.performance`
- May be skipped in CI environments
- Run with: `pytest tests/performance/`

## Running Tests

### All Tests
```bash
pytest
```

### By Category
```bash
pytest -m unit           # Unit tests only
pytest -m integration    # Integration tests only
pytest -m performance    # Performance tests only
```

### Exclude Slow Tests
```bash
pytest -m "not slow"
```

### Exclude Optional Tests
```bash
pytest -m "not optional"
```

### Verbose Output
```bash
pytest -v
```

### Run Specific Test File
```bash
pytest tests/unit/test_connection_pooling.py
pytest tests/integration/test_basic_replication.py::test_e2e_regular
```

## Test Configuration

- `conftest.py`: Contains shared fixtures and utilities used across all tests
- `pytest.ini`: Pytest configuration with markers and settings
- Test markers are defined to categorize tests by type and characteristics

## Common Fixtures

- `test_config`: Loads test configuration
- `mysql_api_instance`: Creates MySQL API instance
- `clickhouse_api_instance`: Creates ClickHouse API instance  
- `clean_environment`: Sets up clean test environment with automatic cleanup
- `temp_config_file`: Creates temporary config file for custom configurations

## Test Utilities

- `assert_wait()`: Wait for conditions with timeout
- `prepare_env()`: Prepare clean test environment
- `kill_process()`: Kill process by PID
- Various test runners: `BinlogReplicatorRunner`, `DbReplicatorRunner`, `RunAllRunner`

## Prerequisites

Before running integration tests, ensure:

1. MySQL is running and accessible
2. ClickHouse is running and accessible
3. Test configuration files exist:
   - `tests_config.yaml`
   - `tests_config_mariadb.yaml`
   - `tests_config_perf.yaml`

## Adding New Tests

1. **Unit tests**: Add to `tests/unit/` 
   - Mark with `@pytest.mark.unit`
   - Mock external dependencies
   - Keep fast and isolated

2. **Integration tests**: Add to `tests/integration/`
   - Mark with `@pytest.mark.integration`
   - Use `clean_environment` fixture for setup/cleanup
   - Test real functionality end-to-end

3. **Performance tests**: Add to `tests/performance/`
   - Mark with `@pytest.mark.performance` and `@pytest.mark.optional`
   - Include timing and metrics
   - Document expected performance characteristics
