# Tests

This directory contains the test suite for mysql-ch-replicator, organized following pytest best practices.

## Structure

```
tests/
├── conftest.py              # Shared fixtures and test utilities
├── unit/                    # Unit tests (fast, isolated)
│   └── test_connection_pooling.py
├── integration/             # Integration tests (require external services)
│   ├── test_advanced_data_types.py
│   ├── test_basic_crud_operations.py  
│   ├── test_configuration_scenarios.py
│   ├── test_ddl_operations.py
│   ├── test_parallel_initial_replication.py
│   ├── test_replication_edge_cases.py
│   └── ... (11 focused test modules)
├── performance/             # Performance tests (long running)
│   └── test_performance.py
└── configs/                 # Test configuration files
```

## Test Categories

### Unit Tests (`tests/unit/`)
- Fast tests that don't require external dependencies
- Test individual components in isolation

### Integration Tests (`tests/integration/`)
- Test complete replication workflows
- Require MySQL and ClickHouse to be running  
- Organized into 11 focused modules by functionality

### Performance Tests (`tests/performance/`)
- Long-running performance benchmarks
- Marked as `@pytest.mark.optional`

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

### Specific Test Module
```bash
pytest tests/integration/test_basic_crud_operations.py -v
pytest tests/integration/test_basic_data_types.py -v
```

## Prerequisites

Before running integration tests, ensure:

1. MySQL is running and accessible
2. ClickHouse is running and accessible
3. Test configuration files exist in `tests/configs/`

## Test Refactoring

The test suite was recently refactored from large monolithic files into smaller, focused modules. All test files are now under 350 lines for better maintainability and easier understanding.

### What Was Refactored

These large files were broken down into focused modules:
- `test_advanced_replication.py` (663 lines) → moved to focused files
- `test_special_cases.py` (895 lines) → split into 3 files  
- `test_basic_replication.py` (340 lines) → moved to CRUD operations
- `test_data_types.py` (362 lines) → split into basic/advanced data types
- `test_schema_evolution.py` (269 lines) → moved to DDL operations

### Benefits of Refactoring

1. **Smaller, Focused Files** - Each file focuses on specific functionality
2. **Better Organization** - Tests grouped by functionality instead of mixed together  
3. **Improved Maintainability** - Smaller files are easier to review and modify
4. **Faster Execution** - Can run specific test categories independently

## Integration Test Modules

The integration tests are organized into focused modules:

- **`test_basic_crud_operations.py`** (201 lines) - CRUD operations during replication
- **`test_ddl_operations.py`** (268 lines) - DDL operations (ALTER TABLE, etc.)  
- **`test_basic_data_types.py`** (282 lines) - Basic MySQL data type handling
- **`test_advanced_data_types.py`** (220 lines) - Advanced data types (spatial, ENUM)
- **`test_parallel_initial_replication.py`** (172 lines) - Parallel initial sync
- **`test_parallel_worker_scenarios.py`** (191 lines) - Worker failure/recovery
- **`test_basic_process_management.py`** (171 lines) - Basic restart/recovery
- **`test_advanced_process_management.py`** (311 lines) - Complex process scenarios
- **`test_configuration_scenarios.py`** (270 lines) - Special config options
- **`test_replication_edge_cases.py`** (467 lines) - Bug reproductions, edge cases  
- **`test_utility_functions.py`** (178 lines) - Parser and utility functions

## Test Configuration

- `conftest.py` contains shared fixtures and utilities
- Configuration files in `tests/configs/` for different test scenarios
- Use `clean_environment` fixture for test setup/cleanup
