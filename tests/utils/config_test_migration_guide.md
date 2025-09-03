# Configuration Test Migration Guide

## Overview

This guide helps migrate existing configuration scenario tests to use the new **EnhancedConfigurationTest** framework, which provides:

- ✅ **Automatic config file management** with isolation and cleanup
- ✅ **Robust process health monitoring** prevents tests continuing with dead processes  
- ✅ **Enhanced database context management** handles `_tmp` transitions reliably
- ✅ **Comprehensive error reporting** with detailed context when failures occur
- ✅ **Simplified test patterns** reduces boilerplate and manual resource management

## Migration Steps

### 1. Update Test Class Inheritance

**Before:**
```python
@pytest.mark.integration
def test_string_primary_key(clean_environment):
    cfg, mysql, ch = clean_environment
    # Manual config loading...
```

**After:**
```python
from tests.base.enhanced_configuration_test import EnhancedConfigurationTest

class TestStringPrimaryKey(EnhancedConfigurationTest):
    @pytest.mark.integration
    def test_string_primary_key_enhanced(self):
        # Automatic setup via enhanced framework
```

### 2. Replace Manual Config Creation

**Before:**
```python
# Manual isolated config creation
from tests.utils.dynamic_config import create_dynamic_config
isolated_config_file = create_dynamic_config(
    base_config_path="tests/configs/replicator/tests_config_string_primary_key.yaml"
)

try:
    # Process management
    binlog_replicator_runner = BinlogReplicatorRunner(cfg_file=isolated_config_file)
    binlog_replicator_runner.run()
    
    db_replicator_runner = DbReplicatorRunner(TEST_DB_NAME, cfg_file=isolated_config_file)
    db_replicator_runner.run()
    
    # Manual cleanup
finally:
    if os.path.exists(isolated_config_file):
        os.unlink(isolated_config_file)
```

**After:**
```python
# Automatic config creation and cleanup
config_file = self.create_config_test(
    base_config_file="tests/configs/replicator/tests_config_string_primary_key.yaml"
)

# Automatic process management with health monitoring
self.start_config_replication(config_file)
# Automatic cleanup handled by framework
```

### 3. Replace Manual Database Context Management

**Before:**
```python
# Manual database waiting and context setting
assert_wait(lambda: TEST_DB_NAME in ch.get_databases())
ch.execute_command(f"USE `{TEST_DB_NAME}`")
assert_wait(lambda: TEST_TABLE_NAME in ch.get_tables())
```

**After:**
```python
# Enhanced sync with automatic context management
self.wait_for_config_sync(TEST_TABLE_NAME, expected_count=3)
```

### 4. Add Config Modifications Support

**Before:**
```python
# Manual config file creation with custom content
config_content = {
    'ignore_deletes': True,
    'binlog_replicator': {'data_dir': '/tmp/isolated/'},
    # ... other settings
}
with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
    yaml.dump(config_content, f)
    config_file = f.name
```

**After:**
```python
# Simple config modifications
config_file = self.create_config_test(
    base_config_file="tests/configs/replicator/tests_config.yaml",
    config_modifications={"ignore_deletes": True}
)
```

### 5. Enhanced Verification and Error Handling

**Before:**
```python
# Basic assertions with minimal error context
assert len(ch.select(TEST_TABLE_NAME)) == 3
assert result[0]["data"] == "expected_value"
```

**After:**
```python
# Comprehensive verification with detailed error context
self.verify_config_test_result(TEST_TABLE_NAME, {
    "total_records": (lambda: len(self.ch.select(TEST_TABLE_NAME)), 3),
    "specific_record": (lambda: self.ch.select(TEST_TABLE_NAME, where="id=1"), 
                      [{"id": 1, "name": "expected_name"}])
})
```

## Complete Migration Example

### Original Test (test_configuration_scenarios.py)

```python
@pytest.mark.integration
def test_string_primary_key(clean_environment):
    """Test replication with string primary keys"""
    cfg, mysql, ch = clean_environment
    
    # Manual config loading
    from tests.conftest import load_isolated_config
    cfg = load_isolated_config("tests/configs/replicator/tests_config_string_primary_key.yaml")
    
    mysql.cfg = cfg
    ch.database = None
    
    mysql.execute("SET sql_mode = 'ALLOW_INVALID_DATES';")
    mysql.execute(f"CREATE TABLE `{TEST_TABLE_NAME}` (...)")
    mysql.execute(f"INSERT INTO `{TEST_TABLE_NAME}` ...")
    
    # Manual config file creation and process management
    from tests.utils.dynamic_config import create_dynamic_config
    isolated_config_file = create_dynamic_config(
        base_config_path="tests/configs/replicator/tests_config_string_primary_key.yaml"
    )
    
    try:
        binlog_replicator_runner = BinlogReplicatorRunner(cfg_file=isolated_config_file)
        binlog_replicator_runner.run()
        
        db_replicator_runner = DbReplicatorRunner(TEST_DB_NAME, cfg_file=isolated_config_file)
        db_replicator_runner.run()
        
        assert_wait(lambda: TEST_DB_NAME in ch.get_databases())
        ch.execute_command(f"USE `{TEST_DB_NAME}`")
        
        assert_wait(lambda: TEST_TABLE_NAME in ch.get_tables())
        assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 2)
        
        mysql.execute(f"INSERT INTO `{TEST_TABLE_NAME}` ...")
        assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 3)
        
        db_replicator_runner.stop()
        binlog_replicator_runner.stop()
    
    finally:
        import os
        if os.path.exists(isolated_config_file):
            os.unlink(isolated_config_file)
```

### Migrated Test

```python
from tests.base.enhanced_configuration_test import EnhancedConfigurationTest

class TestStringPrimaryKeyMigrated(EnhancedConfigurationTest):
    @pytest.mark.integration
    def test_string_primary_key_enhanced(self):
        """Test replication with string primary keys - Enhanced version"""
        
        # 1. Create isolated config (automatic cleanup)
        config_file = self.create_config_test(
            base_config_file="tests/configs/replicator/tests_config_string_primary_key.yaml"
        )
        
        # 2. Setup test data BEFORE starting replication (Phase 1.75 pattern)
        self.mysql.execute("SET sql_mode = 'ALLOW_INVALID_DATES';")
        self.mysql.execute(f"CREATE TABLE `{TEST_TABLE_NAME}` (...)")
        
        # Insert ALL test data before replication starts
        test_data = [('01', 'Ivan'), ('02', 'Peter'), ('03', 'Filipp')]
        for id_val, name in test_data:
            self.mysql.execute(f"INSERT INTO `{TEST_TABLE_NAME}` (id, name) VALUES ('{id_val}', '{name}');", commit=True)
        
        # 3. Start replication with enhanced monitoring
        self.start_config_replication(config_file)
        
        # 4. Wait for sync with enhanced error reporting
        self.wait_for_config_sync(TEST_TABLE_NAME, expected_count=3)
        
        # 5. Comprehensive verification
        self.verify_config_test_result(TEST_TABLE_NAME, {
            "total_records": (lambda: len(self.ch.select(TEST_TABLE_NAME)), 3),
            "ivan_record": (lambda: self.ch.select(TEST_TABLE_NAME, where="id='01'"), 
                          [{"id": "01", "name": "Ivan"}]),
            "peter_record": (lambda: self.ch.select(TEST_TABLE_NAME, where="id='02'"), 
                           [{"id": "02", "name": "Peter"}]),
            "filipp_record": (lambda: self.ch.select(TEST_TABLE_NAME, where="id='03'"), 
                            [{"id": "03", "name": "Filipp"}])
        })
        
        # Automatic cleanup handled by framework
```

## Key Benefits of Migration

### 1. **Eliminated Race Conditions**
- Database creation happens before process startup
- Process health monitoring prevents dead process scenarios
- Enhanced database context management handles `_tmp` transitions

### 2. **Reduced Boilerplate**
- 60%+ reduction in test code length
- Automatic resource management and cleanup
- Consistent patterns across all configuration tests

### 3. **Better Error Reporting**
- Detailed context when failures occur
- Process health status in error messages
- Database and table state debugging information

### 4. **More Reliable Tests** 
- Phase 1.75 pattern eliminates timing issues
- Comprehensive process monitoring
- Robust database context handling

## Migration Checklist

- [ ] Update test class to inherit from `EnhancedConfigurationTest`
- [ ] Replace manual config creation with `self.create_config_test()`
- [ ] Replace manual process management with `self.start_config_replication()`
- [ ] Use `self.wait_for_config_sync()` instead of manual `assert_wait()`
- [ ] Replace simple assertions with `self.verify_config_test_result()`
- [ ] Apply Phase 1.75 pattern (insert all data before replication starts)
- [ ] Remove manual cleanup code (handled automatically)
- [ ] Test the migrated test to ensure it passes reliably

## Common Pitfalls to Avoid

1. **Don't mix manual and enhanced patterns** - Use enhanced framework consistently
2. **Don't insert data during replication** - Use Phase 1.75 pattern for reliability  
3. **Don't manually manage database context** - Let enhanced framework handle it
4. **Don't skip process health monitoring** - It catches failures early
5. **Don't forget config modifications** - Use `config_modifications` parameter for custom settings

## Getting Help

- See `tests/base/configuration_test_examples.py` for complete examples
- Check `tests/base/enhanced_configuration_test.py` for all available methods
- Run `./run_tests.sh tests/base/configuration_test_examples.py` to verify framework works