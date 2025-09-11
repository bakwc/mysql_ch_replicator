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
            
        # CRITICAL FIX: Create dynamic configuration with isolated paths
        # This ensures spawned processes use the correct isolated directories
        from tests.utils.dynamic_config import create_dynamic_config
        if config_file is None:
            config_file = self.config_file
        
        try:
            # Check if config file is already a dynamic config (temporary file)
            if '/tmp/' in config_file:
                print(f"DEBUG: Using existing dynamic config file: {config_file}")
                actual_config_file = config_file
            else:
                # Create dynamic config file with isolated paths for this test
                dynamic_config_file = create_dynamic_config(config_file)
                print(f"DEBUG: Created dynamic config file: {dynamic_config_file}")
                
                # Use the dynamic config file for process spawning
                actual_config_file = dynamic_config_file
        except Exception as e:
            print(f"WARNING: Failed to create dynamic config, using static config: {e}")
            # Fallback to static config file
            actual_config_file = config_file
        
        # ✅ CRITICAL FIX: Ensure MySQL database exists BEFORE starting replication processes
        # This prevents "DB runner has exited with code 1" failures when subprocess
        # tries to query tables from a database that doesn't exist yet
        print(f"DEBUG: Ensuring MySQL database '{db_name}' exists before starting replication...")
        self.ensure_database_exists(db_name)
        
        # CRITICAL: Pre-create ALL necessary directories for binlog replication
        # This prevents FileNotFoundError when processes try to create state/log files
        try:
            # Ensure parent data directory exists (for state.json)
            os.makedirs(self.cfg.binlog_replicator.data_dir, exist_ok=True)
            print(f"DEBUG: Pre-created binlog data directory: {self.cfg.binlog_replicator.data_dir}")
            
            # Ensure database-specific subdirectory exists (for database files)
            db_dir = os.path.join(self.cfg.binlog_replicator.data_dir, db_name)
            os.makedirs(db_dir, exist_ok=True)
            print(f"DEBUG: Pre-created database directory: {db_dir}")
        except Exception as e:
            print(f"WARNING: Could not pre-create binlog directories: {e}")
            # Try to create parent directories first
            try:
                os.makedirs(self.cfg.binlog_replicator.data_dir, exist_ok=True)
                os.makedirs(db_dir, exist_ok=True)
                print(f"DEBUG: Successfully created database directory after retry: {db_dir}")
            except Exception as e2:
                print(f"ERROR: Failed to create database directory after retry: {e2}")
                # Continue execution - let the replication process handle directory creation

        # Now safe to start replication processes - database exists in MySQL
        self.binlog_runner = BinlogReplicatorRunner(cfg_file=actual_config_file)
        print(f"DEBUG: Starting binlog runner with command: {self.binlog_runner.cmd}")
        try:
            self.binlog_runner.run()
            print(f"DEBUG: Binlog runner process started successfully: {self.binlog_runner.process}")
        except Exception as e:
            print(f"ERROR: Failed to start binlog runner: {e}")
            raise

        self.db_runner = DbReplicatorRunner(db_name, cfg_file=actual_config_file)
        print(f"DEBUG: Starting db runner with command: {self.db_runner.cmd}")
        try:
            self.db_runner.run()
            print(f"DEBUG: DB runner process started successfully: {self.db_runner.process}")
        except Exception as e:
            print(f"ERROR: Failed to start db runner: {e}")
            raise

        # CRITICAL: Wait for processes to fully initialize with retry logic
        import time
        startup_wait = 5.0  # Increased from 2.0s - give more time for process initialization
        retry_attempts = 3
        print(f"DEBUG: Waiting {startup_wait}s for replication processes to initialize...")
        
        # Check for immediate failures after 0.5s to catch startup errors early
        time.sleep(0.5)
        if not self._check_replication_process_health():
            print("WARNING: Process failed immediately during startup - capturing early error details")
            error_details = self._get_process_error_details()
            print(f"DEBUG: Early failure details: {error_details}")
        
        # Continue with full startup wait
        time.sleep(startup_wait - 0.5)
        
        # Verify processes started successfully with retry logic
        for attempt in range(retry_attempts):
            if self._check_replication_process_health():
                print("DEBUG: Replication processes started successfully")
                break
            elif attempt < retry_attempts - 1:
                print(f"WARNING: Process health check failed on attempt {attempt + 1}/{retry_attempts}, retrying...")
                # Try to restart failed processes
                self._restart_failed_processes()
                time.sleep(2.0)  # Wait before retry
            else:
                # Final attempt failed - capture detailed error information
                error_details = self._get_process_error_details()
                raise RuntimeError(f"Replication processes failed to start properly after {retry_attempts} attempts. Details: {error_details}")

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
            # Give more time for database migration to complete - increased timeout
            assert_wait(wait_for_final_database, max_wait_time=20.0)  # Increased from 10s to 20s
            self.ch.database = db_name
            print(f"DEBUG: Successfully found final database '{db_name}' in ClickHouse")
        except Exception as e:
            # Migration didn't complete in time - use whatever database is available
            print(f"WARNING: Database migration timeout after 20s: {e}")
            fallback_db = determine_database_context()
            if fallback_db:
                self.ch.database = fallback_db
                print(f"DEBUG: Set ClickHouse context to fallback database '{self.ch.database}'")
            else:
                print(f"ERROR: No ClickHouse database available for context '{db_name}'")
                # Still set the expected database name - it might appear later
                self.ch.database = db_name

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

    def wait_for_table_sync(self, table_name, expected_count=None, database=None, max_wait_time=60.0):
        """Wait for table to be synced to ClickHouse with database transition handling"""
        def table_exists_with_context_switching():
            # Check if replication processes are still alive - fail fast if processes died
            process_health = self._check_replication_process_health()
            if not process_health:
                return False
            
            # Update database context to handle transitions
            target_db = database or TEST_DB_NAME
            actual_db = self.update_clickhouse_database_context(target_db)
            
            if actual_db is None:
                # No database available yet - this is expected during startup
                return False
                
            try:
                tables = self.ch.get_tables(actual_db)
                if table_name in tables:
                    return True
                    
                # Reduced debug output to minimize log noise
                return False
                
            except Exception as e:
                # Reduced debug output - only log significant errors
                if "Connection refused" not in str(e) and "timeout" not in str(e).lower():
                    print(f"WARNING: Error checking tables in '{actual_db}': {e}")
                return False
        
        # First wait for table to exist
        assert_wait(table_exists_with_context_switching, max_wait_time=max_wait_time)
        
        # Then wait for data count if specified
        if expected_count is not None:
            def data_count_matches():
                try:
                    # Update context again in case database changed during table creation
                    target_db = database or TEST_DB_NAME
                    self.update_clickhouse_database_context(target_db)
                    
                    actual_count = len(self.ch.select(table_name))
                    return actual_count == expected_count
                except Exception as e:
                    # Handle transient connection issues during parallel execution
                    if "Connection refused" not in str(e) and "timeout" not in str(e).lower():
                        print(f"WARNING: Error checking data count: {e}")
                    return False
                    
            assert_wait(data_count_matches, max_wait_time=max_wait_time)

    def wait_for_data_sync(
        self, table_name, where_clause, expected_value=None, field="*", max_wait_time=45.0
    ):
        """Wait for specific data to be synced with configurable timeout"""
        if expected_value is not None:
            if field == "*":
                assert_wait(
                    lambda: len(self.ch.select(table_name, where=where_clause)) > 0,
                    max_wait_time=max_wait_time
                )
            else:
                def condition():
                    try:
                        results = self.ch.select(table_name, where=where_clause)
                        if len(results) > 0:
                            actual_value = results[0][field]
                            # Handle type conversions for comparison (e.g., Decimal vs float)
                            try:
                                # Try numeric comparison first
                                return float(actual_value) == float(expected_value)
                            except (TypeError, ValueError):
                                # Fall back to direct comparison for non-numeric values
                                return actual_value == expected_value
                        return False
                    except Exception as e:
                        # Log errors but continue trying - connection issues are common during sync
                        if "Connection refused" not in str(e) and "timeout" not in str(e).lower():
                            print(f"DEBUG: Data sync check error: {e}")
                        return False
                        
                try:
                    assert_wait(condition, max_wait_time=max_wait_time)
                except AssertionError as e:
                    # Provide helpful diagnostic information on failure
                    try:
                        results = self.ch.select(table_name, where=where_clause)
                        if results:
                            actual_value = results[0][field] if results else "<no data>"
                            print(f"ERROR: Data sync failed - Expected {expected_value}, got {actual_value}")
                            print(f"ERROR: Query: SELECT * FROM {table_name} WHERE {where_clause}")
                            print(f"ERROR: Results: {results[:3]}..." if len(results) > 3 else f"ERROR: Results: {results}")
                        else:
                            print(f"ERROR: No data found for query: SELECT * FROM {table_name} WHERE {where_clause}")
                    except Exception as debug_e:
                        print(f"ERROR: Could not gather sync failure diagnostics: {debug_e}")
                    raise
        else:
            assert_wait(lambda: len(self.ch.select(table_name, where=where_clause)) > 0, max_wait_time=max_wait_time)

    def wait_for_condition(self, condition, max_wait_time=30.0):
        """Wait for a condition to be true with timeout - increased for parallel infrastructure"""
        assert_wait(condition, max_wait_time=max_wait_time)
        
    def ensure_database_exists(self, db_name=None):
        """Ensure MySQL database exists before operations - critical for dynamic isolation"""
        if db_name is None:
            from tests.conftest import TEST_DB_NAME
            db_name = TEST_DB_NAME
            
        try:
            # Try to use the database
            self.mysql.set_database(db_name)
            print(f"DEBUG: Database '{db_name}' exists and set as current")
        except Exception as e:
            print(f"DEBUG: Database '{db_name}' does not exist: {e}")
            # Database doesn't exist, create it
            try:
                # Import the helper functions
                from tests.conftest import mysql_create_database, mysql_drop_database
                
                # Clean slate - drop if it exists in some form, then create fresh
                mysql_drop_database(self.mysql, db_name)
                mysql_create_database(self.mysql, db_name)
                self.mysql.set_database(db_name)
                print(f"DEBUG: Created and set database '{db_name}'")
            except Exception as create_error:
                print(f"ERROR: Failed to create database '{db_name}': {create_error}")
                raise
                
    def _check_replication_process_health(self):
        """Check if replication processes are still healthy, return False if any process failed"""
        processes_healthy = True
        
        if self.binlog_runner:
            if self.binlog_runner.process is None:
                print("WARNING: Binlog runner process is None")
                processes_healthy = False
            elif self.binlog_runner.process.poll() is not None:
                exit_code = self.binlog_runner.process.poll()
                print(f"WARNING: Binlog runner has exited with code {exit_code}")
                # Capture subprocess output for debugging
                self._log_subprocess_output("binlog_runner", self.binlog_runner)
                processes_healthy = False
                
        if self.db_runner:
            if self.db_runner.process is None:
                print("WARNING: DB runner process is None")
                processes_healthy = False
            elif self.db_runner.process.poll() is not None:
                exit_code = self.db_runner.process.poll()
                print(f"WARNING: DB runner has exited with code {exit_code}")
                # Capture subprocess output for debugging
                self._log_subprocess_output("db_runner", self.db_runner)
                processes_healthy = False
                
        return processes_healthy
    
    def _restart_failed_processes(self):
        """Attempt to restart any failed processes"""
        if self.binlog_runner and (self.binlog_runner.process is None or self.binlog_runner.process.poll() is not None):
            print("DEBUG: Attempting to restart failed binlog runner...")
            try:
                if self.binlog_runner.process:
                    self.binlog_runner.stop()
                self.binlog_runner.run()
                print("DEBUG: Binlog runner restarted successfully")
            except Exception as e:
                print(f"ERROR: Failed to restart binlog runner: {e}")
                
        if self.db_runner and (self.db_runner.process is None or self.db_runner.process.poll() is not None):
            print("DEBUG: Attempting to restart failed db runner...")
            try:
                if self.db_runner.process:
                    self.db_runner.stop()
                self.db_runner.run()
                print("DEBUG: DB runner restarted successfully")
            except Exception as e:
                print(f"ERROR: Failed to restart db runner: {e}")
    
    def _log_subprocess_output(self, runner_name, runner):
        """Log subprocess output for debugging failed processes"""
        try:
            if hasattr(runner, 'log_file') and runner.log_file and hasattr(runner.log_file, 'name'):
                log_file_path = runner.log_file.name
                if os.path.exists(log_file_path):
                    with open(log_file_path, 'r') as f:
                        output = f.read()
                        if output.strip():
                            print(f"ERROR: {runner_name} subprocess output:")
                            # Show last 20 lines to avoid log spam
                            lines = output.strip().split('\n')
                            for line in lines[-20:]:
                                print(f"  {runner_name}: {line}")
                        else:
                            print(f"WARNING: {runner_name} subprocess produced no output")
                else:
                    print(f"WARNING: {runner_name} log file does not exist: {log_file_path}")
            else:
                print(f"WARNING: {runner_name} has no accessible log file")
        except Exception as e:
            print(f"ERROR: Failed to read {runner_name} subprocess output: {e}")
    
    def _get_process_error_details(self):
        """Gather detailed error information for failed process startup"""
        error_details = []
        
        if self.binlog_runner:
            if self.binlog_runner.process is None:
                error_details.append("Binlog runner: process is None")
            else:
                exit_code = self.binlog_runner.process.poll()
                error_details.append(f"Binlog runner: exit code {exit_code}")
                # Capture subprocess logs if available
                if hasattr(self.binlog_runner, 'log_file') and self.binlog_runner.log_file:
                    try:
                        self.binlog_runner.log_file.seek(0)
                        log_content = self.binlog_runner.log_file.read()
                        if log_content.strip():
                            error_details.append(f"Binlog logs: {log_content[-200:]}")  # Last 200 chars
                    except Exception as e:
                        error_details.append(f"Binlog log read error: {e}")
                
        if self.db_runner:
            if self.db_runner.process is None:
                error_details.append("DB runner: process is None")
            else:
                exit_code = self.db_runner.process.poll()
                error_details.append(f"DB runner: exit code {exit_code}")
                # Capture subprocess logs if available
                if hasattr(self.db_runner, 'log_file') and self.db_runner.log_file:
                    try:
                        self.db_runner.log_file.seek(0)
                        log_content = self.db_runner.log_file.read()
                        if log_content.strip():
                            error_details.append(f"DB logs: {log_content[-200:]}")  # Last 200 chars
                    except Exception as e:
                        error_details.append(f"DB log read error: {e}")
        
        # Add environment info
        from tests.conftest import TEST_DB_NAME
        error_details.append(f"Database: {TEST_DB_NAME}")
        
        # Add config info
        if hasattr(self, 'config_file'):
            error_details.append(f"Config: {self.config_file}")
            
        return "; ".join(error_details)
            
    def update_clickhouse_database_context(self, db_name=None):
        """Update ClickHouse client to use correct database context"""
        if db_name is None:
            from tests.conftest import TEST_DB_NAME
            db_name = TEST_DB_NAME
            
        # Get available databases
        try:
            databases = self.ch.get_databases()
            print(f"DEBUG: Available ClickHouse databases: {databases}")
            
            # Try final database first, then temporary
            if db_name in databases:
                self.ch.database = db_name
                print(f"DEBUG: Set ClickHouse context to final database: {db_name}")
                return db_name
            elif f"{db_name}_tmp" in databases:
                self.ch.database = f"{db_name}_tmp"  
                print(f"DEBUG: Set ClickHouse context to temporary database: {db_name}_tmp")
                return f"{db_name}_tmp"
            else:
                # Neither exists - this may happen during transitions
                print(f"WARNING: Neither {db_name} nor {db_name}_tmp found in ClickHouse")
                print(f"DEBUG: Available databases were: {databases}")
                return None
        except Exception as e:
            print(f"ERROR: Failed to update ClickHouse database context: {e}")
            return None

    def start_isolated_replication(self, config_file=None, db_name=None, target_mappings=None):
        """
        Standardized method to start replication with isolated configuration.
        
        This eliminates the need to manually call create_dynamic_config everywhere.
        
        Args:
            config_file: Base config file path (defaults to self.config_file)
            db_name: Database name for replication (defaults to TEST_DB_NAME)
            target_mappings: Optional dict of source -> target database mappings
        """
        from tests.utils.dynamic_config import create_dynamic_config
        
        # Use default config if not specified
        if config_file is None:
            config_file = self.config_file
        
        # Create isolated configuration
        isolated_config = create_dynamic_config(
            base_config_path=config_file,
            target_mappings=target_mappings
        )
        
        # Start replication with isolated config
        self.start_replication(config_file=isolated_config, db_name=db_name)
        
        # Handle ClickHouse database lifecycle transitions
        self.update_clickhouse_database_context(db_name)
        
        return isolated_config

    def create_isolated_target_database_name(self, source_db_name, target_suffix="target"):
        """
        Helper method to create isolated target database names for mapping tests.
        
        Args:
            source_db_name: Source database name (used for reference)  
            target_suffix: Suffix for target database name
            
        Returns:
            Isolated target database name
        """
        from tests.utils.dynamic_config import get_config_manager
        config_manager = get_config_manager()
        return config_manager.get_isolated_target_database_name(source_db_name, target_suffix)

    def create_dynamic_config_with_target_mapping(self, source_db_name, target_db_name):
        """
        Helper method to create dynamic config with target database mapping.
        
        Args:
            source_db_name: Source database name
            target_db_name: Target database name
            
        Returns:
            Path to created dynamic config file
        """
        from tests.utils.dynamic_config import create_dynamic_config
        return create_dynamic_config(
            base_config_path=self.config_file,
            target_mappings={source_db_name: target_db_name}
        )
