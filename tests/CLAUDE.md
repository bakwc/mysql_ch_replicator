# MySQL ClickHouse Replicator Test Architecture

## Overview

This document explains the reusable test components, architecture, and organization principles for the MySQL ClickHouse Replicator test suite. The test architecture is designed for maintainability, reusability, and comprehensive coverage of replication scenarios.

## 🏗️ Test Architecture

### Base Classes & Mixins

#### `BaseReplicationTest`
**Location**: `tests/base/base_replication_test.py`  
**Purpose**: Core test infrastructure for replication scenarios

**Key Features**:
- Database connection management (MySQL & ClickHouse)
- Replication process lifecycle (start/stop)
- Environment cleanup and setup
- Configuration management

**Usage**:
```python
from tests.base import BaseReplicationTest

class MyTest(BaseReplicationTest):
    def test_my_scenario(self):
        self.start_replication()
        # Test implementation
```

#### `DataTestMixin`
**Location**: `tests/base/data_test_mixin.py`  
**Purpose**: Data operations and validation utilities

**Key Methods**:
- `insert_multiple_records()` - Bulk data insertion
- `verify_record_exists()` - Data validation with conditions
- `verify_record_does_not_exist()` - Negative validation
- `wait_for_table_sync()` - Synchronization with expected counts
- `wait_for_record_update()` - Update verification
- `wait_for_stable_state()` - Stability verification

**Usage**:
```python
from tests.base import BaseReplicationTest, DataTestMixin

class MyTest(BaseReplicationTest, DataTestMixin):
    def test_data_operations(self):
        self.insert_multiple_records(table_name, [{"name": "test", "age": 30}])
        self.verify_record_exists(table_name, "name='test'", {"age": 30})
```

#### `SchemaTestMixin`  
**Location**: `tests/base/schema_test_mixin.py`  
**Purpose**: Database schema operations and DDL utilities

**Key Methods**:
- `create_basic_table()` - Standard table creation
- `wait_for_ddl_replication()` - DDL synchronization
- `wait_for_database()` - Database creation verification

### Fixtures System

#### `TableSchemas`
**Location**: `tests/fixtures/table_schemas.py`  
**Purpose**: Reusable table schema definitions

**Available Schemas**:
- `basic_table()` - Standard id/name/age table
- `datetime_test_table()` - Various datetime field types
- `numeric_test_table()` - All numeric data types
- `json_test_table()` - JSON column variations
- `complex_schema()` - Multi-column complex table

**Usage**:
```python
from tests.fixtures import TableSchemas

schema = TableSchemas.datetime_test_table("my_table")
self.mysql.execute(schema.sql)
```

#### `TestDataGenerator`
**Location**: `tests/fixtures/test_data.py`  
**Purpose**: Consistent test data generation

**Available Generators**:
- `basic_records()` - Simple name/age records
- `datetime_records()` - Date/time test data
- `numeric_boundary_data()` - Min/max numeric values
- `unicode_test_data()` - Multi-language content
- `json_test_data()` - Complex JSON structures

#### `AssertionHelpers`
**Location**: `tests/fixtures/assertions.py`  
**Purpose**: Specialized assertion utilities

## 🗂️ Test Organization

### Folder Structure

```
tests/
├── integration/
│   ├── data_types/          # Data type replication tests
│   ├── ddl/                 # DDL operation tests
│   ├── replication/         # Core replication functionality
│   ├── process_management/  # Process lifecycle tests
│   ├── edge_cases/          # Bug reproductions & edge cases
│   └── data_integrity/      # Data consistency & validation
├── unit/                    # Unit tests
├── performance/             # Performance benchmarks
├── base/                    # Base classes & mixins
├── fixtures/                # Reusable test components
├── utils/                   # Test utilities
└── configs/                 # Test configurations
```

### Test Categories

#### Data Types (`tests/integration/data_types/`)
Tests for MySQL data type replication behavior:

- **Basic Data Types**: `test_basic_data_types.py`
  - Integer, varchar, datetime, boolean
  - NULL value handling
  - Type conversion validation

- **Advanced Data Types**: `test_advanced_data_types.py`  
  - TEXT, BLOB, binary data
  - Large object handling
  - Character encoding

- **JSON Data Types**: `test_json_data_types.py`
  - JSON column operations
  - Complex nested structures
  - JSON updates and modifications

- **Specialized Types**: 
  - `test_enum_normalization.py` - ENUM type handling
  - `test_polygon_type.py` - Geometric data
  - `test_year_type.py` - MySQL YEAR type
  - `test_numeric_boundary_limits.py` - Numeric edge cases

#### DDL Operations (`tests/integration/ddl/`)
Data Definition Language operation tests:

- **Core DDL**: `test_ddl_operations.py`
  - CREATE, ALTER, DROP operations
  - Index management

- **Advanced DDL**: `test_advanced_ddl_operations.py`
  - Column positioning (FIRST/AFTER)
  - Conditional statements (IF EXISTS)
  - Percona-specific features

- **Schema Evolution**: `test_create_table_like.py`, `test_multi_alter_statements.py`

#### Replication Core (`tests/integration/replication/`)
Core replication functionality:

- **End-to-End**: `test_e2e_scenarios.py`
  - Complete replication workflows
  - Multi-statement transactions
  - Real-time updates

- **CRUD Operations**: `test_basic_crud_operations.py`
  - Create, Read, Update, Delete
  - Batch operations

- **Process Management**: 
  - `test_initial_only_mode.py` - Initial replication
  - `test_parallel_initial_replication.py` - Parallel processing

#### Data Integrity (`tests/integration/data_integrity/`)
Data consistency and validation:

- **Consistency Validation**: `test_data_consistency.py`
  - Checksum validation
  - Row-level comparison
  - Data integrity verification

- **Corruption Detection**: `test_corruption_detection.py`
  - Malformed data handling
  - Character encoding issues
  - State file corruption

- **Duplicate Detection**: `test_duplicate_detection.py`
  - Duplicate event handling
  - Idempotent operations
  - Binlog position management

- **Ordering Guarantees**: `test_ordering_guarantees.py`
  - Event sequence validation
  - Transaction boundaries
  - Ordering consistency

## 🛠️ Writing New Tests

### Test Naming Conventions

**Files**: `test_<functionality>_<category>.py`
- `test_json_data_types.py`
- `test_advanced_ddl_operations.py`
- `test_schema_evolution_mapping.py`

**Classes**: `Test<Functionality><Category>`
- `TestJsonDataTypes`
- `TestAdvancedDdlOperations`
- `TestSchemaEvolutionMapping`

**Methods**: `test_<specific_scenario>`
- `test_json_basic_operations`
- `test_column_positioning_ddl`
- `test_schema_evolution_with_db_mapping`

### Test Structure Template

```python
\"\"\"Test description explaining the functionality being tested\"\"\"

import pytest
from tests.base import BaseReplicationTest, DataTestMixin, SchemaTestMixin
from tests.conftest import TEST_TABLE_NAME
from tests.fixtures import TableSchemas, TestDataGenerator

class TestMyFunctionality(BaseReplicationTest, SchemaTestMixin, DataTestMixin):
    \"\"\"Test class description\"\"\"

    @pytest.mark.integration
    def test_specific_scenario(self):
        \"\"\"Test specific scenario description\"\"\"
        # 1. Setup - Create schema and data
        schema = TableSchemas.basic_table(TEST_TABLE_NAME)
        self.mysql.execute(schema.sql)
        
        test_data = TestDataGenerator.basic_records(count=3)
        self.insert_multiple_records(TEST_TABLE_NAME, test_data)
        
        # 2. Start replication
        self.start_replication()
        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=3)
        
        # 3. Perform operations
        # Your test logic here
        
        # 4. Verify results
        self.verify_record_exists(TEST_TABLE_NAME, "name='test'", {"age": 30})
```

### File Size Guidelines

- **Maximum 300 lines per test file**
- **Split large files by functionality**
- **Use descriptive file names**
- **Group related tests together**

### Pytest Markers

Use appropriate markers for test categorization:

```python
@pytest.mark.integration      # Integration test
@pytest.mark.performance      # Performance test  
@pytest.mark.slow             # Slow-running test
@pytest.mark.skip(reason="")  # Skip with reason
@pytest.mark.parametrize      # Parameterized test
```

## 🔧 Test Configuration

### Configuration Files
**Location**: `tests/configs/`
- `tests_config.yaml` - Standard configuration
- `tests_config_db_mapping.yaml` - Database mapping
- `tests_config_dynamic_column.yaml` - Dynamic columns

### Environment Variables
- `TEST_DB_NAME` - Test database name
- `TEST_TABLE_NAME` - Test table name
- `CONFIG_FILE` - Configuration file path

### Test Utilities
**Location**: `tests/utils/`
- `mysql_test_api.py` - MySQL test utilities
- Helper functions for common operations

## 🚀 Running Tests

### Full Test Suite
```bash
pytest tests/
```

### By Category
```bash
pytest tests/integration/data_types/
pytest tests/integration/ddl/
pytest tests/integration/replication/
```

### Individual Tests
```bash
pytest tests/integration/data_types/test_json_data_types.py::TestJsonDataTypes::test_json_basic_operations
```

### With Markers
```bash
pytest -m integration  # Only integration tests
pytest -m "not slow"   # Skip slow tests
```

## 📊 Best Practices

### Test Design
1. **Single Responsibility** - One test per scenario
2. **Descriptive Names** - Clear test purpose
3. **Arrange-Act-Assert** - Structure tests clearly
4. **Independent Tests** - No test dependencies
5. **Cleanup** - Proper resource cleanup

### Data Management
1. **Use Fixtures** - Reuse common data patterns
2. **Parameterized Tests** - Test multiple scenarios
3. **Boundary Testing** - Test edge cases
4. **Random Data** - Use controlled randomization

### Assertions
1. **Specific Assertions** - Clear failure messages
2. **Wait Conditions** - Use wait_for_* methods
3. **Timeout Handling** - Set appropriate timeouts
4. **Error Context** - Provide context in assertions

### Performance
1. **Parallel Execution** - Design for parallelization
2. **Resource Management** - Efficient resource usage
3. **Test Isolation** - Avoid shared state
4. **Cleanup Efficiency** - Fast cleanup procedures

## 🔍 Debugging Tests

### Common Issues
1. **Timing Issues** - Use appropriate wait conditions
2. **Resource Conflicts** - Ensure test isolation
3. **Data Consistency** - Verify replication completion
4. **Configuration** - Check test configuration

### Debugging Tools
1. **Logging** - Enable debug logging
2. **Manual Inspection** - Query databases directly
3. **Process Monitoring** - Check replication processes
4. **State Files** - Inspect replication state

### Test Failure Analysis
1. **Check Logs** - Examine replication logs
2. **Verify Environment** - Confirm test setup
3. **Data Validation** - Compare source and target
4. **Process Status** - Ensure processes running

This architecture provides a robust, maintainable, and comprehensive testing framework for MySQL ClickHouse replication scenarios.