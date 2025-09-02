# MySQL ClickHouse Replicator - Complete Testing Guide

## Overview

Comprehensive test suite with 65+ integration tests ensuring reliable data replication from MySQL to ClickHouse. This guide covers test development patterns, infrastructure, and execution.

## Test Suite Structure

```
tests/
├── conftest.py              # Shared fixtures and test utilities
├── unit/                    # Unit tests (fast, isolated)
│   └── test_connection_pooling.py
├── integration/             # Integration tests (require external services)
│   ├── replication/         # Core replication functionality
│   ├── data_types/         # MySQL data type handling
│   ├── data_integrity/     # Consistency and corruption detection
│   ├── edge_cases/         # Complex scenarios & bug reproductions
│   ├── process_management/ # Process lifecycle & recovery
│   ├── performance/        # Stress testing & concurrent operations
│   └── percona/            # Percona MySQL specific tests
├── performance/             # Performance benchmarks (optional)
└── configs/                 # Test configuration files
```

### Test Categories

- **Unit Tests**: Fast, isolated component tests
- **Integration Tests**: End-to-end replication workflows requiring MySQL/ClickHouse
- **Performance Tests**: Long-running benchmarks marked `@pytest.mark.optional`
- **Percona Tests**: Specialized tests for Percona MySQL features

## Running Tests

**⚠️ CRITICAL**: Always use the test script for ALL test verification:

```bash
./run_tests.sh                    # Full parallel test suite
./run_tests.sh --serial           # Sequential mode
./run_tests.sh -k "test_name"     # Specific tests
./run_tests.sh tests/path/to/test_file.py  # Specific file
```

**❌ NEVER use these commands:**
- `pytest tests/...` 
- `docker exec ... pytest ...`
- Any direct pytest execution

The test script handles all prerequisites automatically:
- Docker containers (MySQL 9306, MariaDB 9307, Percona 9308, ClickHouse 9123)
- Database setup and configuration
- Process lifecycle management and cleanup

## Test Development Patterns

### Base Classes
- **`BaseReplicationTest`**: Core test infrastructure with `self.start_replication()`
- **`DataTestMixin`**: Data operations (`insert_multiple_records`, `verify_record_exists`)
- **`SchemaTestMixin`**: Schema operations (`create_basic_table`, `wait_for_database`)

### Basic Test Pattern
```python
from tests.base import BaseReplicationTest, DataTestMixin, SchemaTestMixin

class MyTest(BaseReplicationTest, DataTestMixin, SchemaTestMixin):
    def test_example(self):
        # 1. Create schema
        self.create_basic_table(TEST_TABLE_NAME)
        
        # 2. Insert data
        self.insert_multiple_records(TEST_TABLE_NAME, test_data)
        
        # 3. Start replication
        self.start_replication()
        
        # 4. Verify
        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=len(test_data))
```

## ✅ Phase 1.75 Pattern (REQUIRED for reliability)

**Critical Rule**: Insert ALL data BEFORE starting replication

```python
def test_example():
    # ✅ CORRECT PATTERN
    schema = TableSchemas.basic_table(TEST_TABLE_NAME)
    self.mysql.execute(schema.sql)
    
    # Pre-populate ALL test data (including data for later scenarios)
    all_data = initial_data + update_data + verification_data
    self.insert_multiple_records(TEST_TABLE_NAME, all_data)
    
    # Start replication with complete dataset
    self.start_replication()  
    self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=len(all_data))
    
    # Test functionality on static data
    # Verify results
```

```python
def test_bad_example():
    # ❌ WRONG PATTERN - Will cause timeouts/failures
    self.create_basic_table(TEST_TABLE_NAME)
    self.insert_multiple_records(TEST_TABLE_NAME, initial_data)
    
    self.start_replication()  # Start replication
    
    # ❌ PROBLEM: Insert more data AFTER replication starts
    self.insert_multiple_records(TEST_TABLE_NAME, more_data)
    self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=total)  # Will timeout!
```

## Test Environment

- **Execution**: Always use `./run_tests.sh` - handles all Docker container management
- **Databases**: MySQL (9306), MariaDB (9307), Percona (9308), ClickHouse (9123)
- **Infrastructure**: Auto-restart processes, monitoring, cleanup
- **Prerequisites**: Docker and Docker Compose (handled automatically by test script)

## Integration Test Modules

The integration tests are organized into focused modules (all under 350 lines):

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

### Test Refactoring Benefits

Recently refactored from large monolithic files:
- **Smaller, Focused Files** - Each file focuses on specific functionality
- **Better Organization** - Tests grouped by functionality instead of mixed together  
- **Improved Maintainability** - Smaller files are easier to review and modify
- **Faster Execution** - Can run specific test categories independently

## 🔄 Dynamic Database Isolation System ✅ **FIXED**

**Complete parallel testing safety implemented** - each test gets isolated databases and binlog directories.

### Architecture
- **Source Isolation**: `test_db_<worker>_<testid>` (MySQL databases)
- **Target Isolation**: `<prefix>_<worker>_<testid>` (ClickHouse databases)
- **Data Directory Isolation**: `/app/binlog_<worker>_<testid>/` 
- **Configuration Isolation**: Dynamic YAML generation with auto-cleanup

### Core Components

**`tests/utils/dynamic_config.py`**
- `DynamicConfigManager` singleton for centralized isolation
- Worker-specific naming using `PYTEST_XDIST_WORKER`
- Thread-local storage for test-specific isolation
- Automatic cleanup of temporary resources

**Enhanced Base Classes**
- `BaseReplicationTest.create_isolated_target_database_name()` 
- `BaseReplicationTest.create_dynamic_config_with_target_mapping()`
- `BaseReplicationTest.update_clickhouse_database_context()` - handles `_tmp` → final transitions
- Automatic isolation in `conftest.py` fixtures

### Usage Patterns

**Basic Isolated Test**
```python
class MyTest(BaseReplicationTest, DataTestMixin):
    def test_with_isolation(self):
        # Database names automatically isolated per worker/test
        # TEST_DB_NAME = "test_db_w1_abc123" (automatic)
        
        self.create_basic_table(TEST_TABLE_NAME)
        self.start_replication()  # Uses isolated databases
        self.update_clickhouse_database_context()  # Handle lifecycle transitions
```

**Target Database Mapping**
```python
def test_with_target_mapping(self):
    # Create isolated target database 
    target_db = self.create_isolated_target_database_name("custom_target")
    
    # Generate dynamic config with mapping
    config_file = self.create_dynamic_config_with_target_mapping(
        source_db_name=TEST_DB_NAME,
        target_db_name=target_db
    )
    
    # Use custom config for replication
    self.start_replication(config_file=config_file)
```

**Manual Dynamic Configuration**
```python
from tests.utils.dynamic_config import create_dynamic_config

def test_custom_mapping(self):
    config_file = create_dynamic_config(
        base_config_path="tests/configs/replicator/tests_config.yaml",
        target_mappings={
            TEST_DB_NAME: f"analytics_target_{worker_id}_{test_id}"
        }
    )
```

### Isolation Verification

Run the isolation verification test to confirm parallel safety:
```bash
./run_tests.sh -k "test_binlog_isolation_verification"
```

Expected output: ✅ `BINLOG ISOLATION VERIFIED: Unique directory /app/binlog_w1_abc123/`

## Real-Time vs Static Testing

- **Static Tests**: Use Phase 1.75 pattern for reliable execution (most tests)
- **Real-Time Tests**: `test_e2e_regular_replication()` validates production scenarios  
- **Pattern Choice**: Insert-before-start for reliability, real-time for validation
- **Parallel Safety**: All patterns work with dynamic database isolation

## Current Status & Recent Fixes

- **Pass Rate**: Expected ~80-90% improvement after binlog isolation fixes
- **Performance**: ~45 seconds for full test suite
- **Infrastructure**: Stable with auto-restart and monitoring
- **Major Fix**: Binlog directory isolation resolved 132 test failures

### Recent Infrastructure Fixes

1. **Binlog Directory Isolation** ✅ - Each test gets unique `/app/binlog_{worker}_{test_id}/`
2. **Configuration Loading** ✅ - Fixed core `test_config` fixture isolation
3. **Database Context Management** ✅ - Added `update_clickhouse_database_context()`
4. **Docker Volume Mount** ✅ - Fixed `/app/binlog/` writability issues
5. **Connection Pool Config** ✅ - Updated for multi-database support (9306/9307/9308)

## Percona MySQL Integration

See `integration/percona/CLAUDE.md` for detailed Percona-specific test documentation including:
- Audit log compatibility
- Performance optimization tests
- GTID consistency validation
- Character set handling

## Historical Documentation

- Previous achievements and detailed fix histories are available in archived documentation
- Focus is now on the current stable, isolated testing infrastructure