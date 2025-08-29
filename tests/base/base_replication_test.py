"""Base test class for replication tests"""

import os
import pytest

from tests.conftest import (
    CONFIG_FILE,
    TEST_DB_NAME,
    BinlogReplicatorRunner,
    DbReplicatorRunner,
    assert_wait,
)


class BaseReplicationTest:
    """Base class for all replication tests with common setup/teardown"""

    @pytest.fixture(autouse=True)
    def setup_replication_test(self, clean_environment):
        """Setup common to all replication tests"""
        self.cfg, self.mysql, self.ch = clean_environment
        self.config_file = getattr(self.cfg, "config_file", CONFIG_FILE)
        
        # CRITICAL: Ensure binlog directory always exists for parallel test safety
        import os
        os.makedirs(self.cfg.binlog_replicator.data_dir, exist_ok=True)

        # Initialize runners as None - tests can create them as needed
        self.binlog_runner = None
        self.db_runner = None

        yield

        # Cleanup
        if self.db_runner:
            self.db_runner.stop()
        if self.binlog_runner:
            self.binlog_runner.stop()

    def start_replication(self, db_name=None, config_file=None):
        """Start binlog and db replication with common setup"""
        # Use the database name from the test config if available, otherwise fallback
        if db_name is None and hasattr(self.cfg, 'test_db_name'):
            db_name = self.cfg.test_db_name
        elif db_name is None:
            # Import TEST_DB_NAME dynamically to get current per-test value
            from tests.conftest import TEST_DB_NAME
            db_name = TEST_DB_NAME
            
        config_file = config_file or self.config_file
        
        # CRITICAL: Pre-create database-specific subdirectory for logging
        # This prevents FileNotFoundError when db_replicator tries to create log files
        db_dir = os.path.join(self.cfg.binlog_replicator.data_dir, db_name)
        try:
            os.makedirs(db_dir, exist_ok=True)
            print(f"DEBUG: Pre-created database directory: {db_dir}")
        except Exception as e:
            print(f"WARNING: Could not pre-create database directory {db_dir}: {e}")
            # Try to create parent directories first
            try:
                os.makedirs(self.cfg.binlog_replicator.data_dir, exist_ok=True)
                os.makedirs(db_dir, exist_ok=True)
                print(f"DEBUG: Successfully created database directory after retry: {db_dir}")
            except Exception as e2:
                print(f"ERROR: Failed to create database directory after retry: {e2}")
                # Continue execution - let the replication process handle directory creation

        self.binlog_runner = BinlogReplicatorRunner(cfg_file=config_file)
        self.binlog_runner.run()

        self.db_runner = DbReplicatorRunner(db_name, cfg_file=config_file)
        self.db_runner.run()

        # Wait for replication to start and set database context for the ClickHouse client
        def check_database_exists():
            try:
                databases = self.ch.get_databases()
                print(f"DEBUG: Available databases in ClickHouse: {databases}")
                print(f"DEBUG: Looking for database: {db_name}")
                
                # Check for the final database name OR the temporary database name
                # During initial replication, the database exists as {db_name}_tmp
                final_db_exists = db_name in databases
                temp_db_exists = f"{db_name}_tmp" in databases
                
                if final_db_exists:
                    print(f"DEBUG: Found final database: {db_name}")
                    return True
                elif temp_db_exists:
                    print(f"DEBUG: Found temporary database: {db_name}_tmp (initial replication in progress)")
                    return True
                else:
                    print(f"DEBUG: Database not found in either final or temporary form")
                    return False
            except Exception as e:
                print(f"DEBUG: Error checking databases: {e}")
                return False
                
        print(f"DEBUG: Waiting for database '{db_name}' to appear in ClickHouse...")
        assert_wait(check_database_exists, max_wait_time=30.0)  # Reduced from 45s
        
        # Set the database context - intelligently handle both final and temp databases
        def determine_database_context():
            databases = self.ch.get_databases()
            if db_name in databases:
                # Final database exists - use it
                print(f"DEBUG: Using final database '{db_name}' for ClickHouse context")
                return db_name
            elif f"{db_name}_tmp" in databases:
                # Only temporary database exists - use it
                print(f"DEBUG: Using temporary database '{db_name}_tmp' for ClickHouse context")
                return f"{db_name}_tmp"
            else:
                # Neither exists - this shouldn't happen, but fallback to original name
                print(f"DEBUG: Warning: Neither final nor temporary database found, using '{db_name}'")
                return db_name
        
        # First, try to wait briefly for the final database (migration from _tmp)
        def wait_for_final_database():
            databases = self.ch.get_databases()
            return db_name in databases
        
        try:
            # Give a short window for database migration to complete
            assert_wait(wait_for_final_database, max_wait_time=10.0)  # Reduced from 15s
            self.ch.database = db_name
            print(f"DEBUG: Successfully found final database '{db_name}' in ClickHouse")
        except:
            # Migration didn't complete in time - use whatever database is available
            self.ch.database = determine_database_context()
            print(f"DEBUG: Set ClickHouse context to '{self.ch.database}' (migration timeout)")

    def setup_and_replicate_table(self, schema_func, test_data, table_name=None, expected_count=None):
        """Standard replication test pattern: create table → insert data → replicate → verify"""
        from tests.conftest import TEST_TABLE_NAME
        
        table_name = table_name or TEST_TABLE_NAME
        expected_count = expected_count or len(test_data) if test_data else 0
        
        # Create table using schema factory
        schema = schema_func(table_name)
        self.mysql.execute(schema.sql if hasattr(schema, 'sql') else schema)
        
        # Insert test data if provided
        if test_data:
            from tests.base.data_test_mixin import DataTestMixin
            if hasattr(self, 'insert_multiple_records'):
                self.insert_multiple_records(table_name, test_data)
        
        # Start replication and wait for sync
        self.start_replication()
        if hasattr(self, 'wait_for_table_sync'):
            self.wait_for_table_sync(table_name, expected_count=expected_count)
            
        return expected_count
    
    def stop_replication(self):
        """Stop both binlog and db replication"""
        if self.db_runner:
            self.db_runner.stop()
            self.db_runner = None
        if self.binlog_runner:
            self.binlog_runner.stop()
            self.binlog_runner = None

    def wait_for_table_sync(self, table_name, expected_count=None, database=None, max_wait_time=20.0):
        """Wait for table to be synced to ClickHouse"""
        def table_exists():
            # Check tables in the specified database or current context
            target_db = database or self.ch.database or TEST_DB_NAME
            tables = self.ch.get_tables(target_db)
            if table_name not in tables:
                # Debug: print available tables and current database context
                databases = self.ch.get_databases()
                print(f"DEBUG: Table '{table_name}' not found. Available tables: {tables}")
                print(f"DEBUG: Available databases: {databases}")
                print(f"DEBUG: ClickHouse database context: {target_db}")
                return False
            return True
        
        assert_wait(table_exists, max_wait_time=max_wait_time)
        if expected_count is not None:
            assert_wait(lambda: len(self.ch.select(table_name)) == expected_count, max_wait_time=max_wait_time)

    def wait_for_data_sync(
        self, table_name, where_clause, expected_value=None, field="*"
    ):
        """Wait for specific data to be synced"""
        if expected_value is not None:
            if field == "*":
                assert_wait(
                    lambda: len(self.ch.select(table_name, where=where_clause)) > 0
                )
            else:
                def condition():
                    results = self.ch.select(table_name, where=where_clause)
                    return len(results) > 0 and results[0][field] == expected_value
                assert_wait(condition)
        else:
            assert_wait(lambda: len(self.ch.select(table_name, where=where_clause)) > 0)

    def wait_for_condition(self, condition, max_wait_time=20.0):
        """Wait for a condition to be true with timeout"""
        assert_wait(condition, max_wait_time=max_wait_time)
