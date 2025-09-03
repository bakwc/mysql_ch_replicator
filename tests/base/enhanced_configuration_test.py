"""Enhanced base class for configuration scenario tests with robust process and database management"""

import os
import time
import tempfile
from typing import Optional, Dict, Any

import pytest

from tests.base.base_replication_test import BaseReplicationTest
from tests.base.data_test_mixin import DataTestMixin
from tests.base.schema_test_mixin import SchemaTestMixin
from tests.conftest import RunAllRunner, assert_wait, read_logs
from tests.utils.dynamic_config import create_dynamic_config


class EnhancedConfigurationTest(BaseReplicationTest, DataTestMixin, SchemaTestMixin):
    """Enhanced base class for configuration scenario tests
    
    Provides:
    - Automatic config file isolation and cleanup
    - Robust process health monitoring  
    - Consistent database context management
    - Simplified test setup/teardown
    - Comprehensive error handling and reporting
    """
    
    # Remove __init__ to be compatible with pytest class collection
    # Instead, initialize in setup method
        
    @pytest.fixture(autouse=True)
    def setup_enhanced_configuration_test(self, clean_environment):
        """Enhanced setup for configuration tests with automatic cleanup"""
        # Initialize base test components (clean_environment provides cfg, mysql, ch)
        self.cfg, self.mysql, self.ch = clean_environment
        self.config_file = getattr(self.cfg, "config_file", "tests/configs/replicator/tests_config.yaml")
        
        # CRITICAL: Ensure binlog directory always exists for parallel test safety
        import os
        os.makedirs(self.cfg.binlog_replicator.data_dir, exist_ok=True)

        # Initialize runners as None - tests can create them as needed
        self.binlog_runner = None
        self.db_runner = None
        
        # Initialize enhanced configuration tracking
        self.config_files_created = []
        self.run_all_runners = []
        self.custom_config_content = None
        self.process_health_monitoring = True
        
        yield
        
        # Enhanced cleanup - automatically handles all created resources
        self._cleanup_enhanced_resources()
    
    def create_config_test(self, base_config_file: str, config_modifications: Optional[Dict[str, Any]] = None, 
                          use_run_all_runner: bool = False) -> str:
        """Create an isolated config for testing with automatic cleanup tracking
        
        Args:
            base_config_file: Base configuration file to start from
            config_modifications: Dictionary of config keys to modify (e.g., {"ignore_deletes": True})
            use_run_all_runner: If True, creates RunAllRunner instead of individual runners
            
        Returns:
            Path to the created isolated config file
        """
        
        # CRITICAL FIX: Ensure MySQL and ClickHouse databases are specified in the configuration
        # The replication processes need to know which databases to connect to
        from tests.conftest import TEST_DB_NAME
        db_name = TEST_DB_NAME  # Current isolated database name (e.g., test_db_w3_abc123)
        
        # Merge MySQL and ClickHouse database settings with any provided modifications
        database_settings = {
            "mysql": {"database": db_name},
            "clickhouse": {"database": db_name}  # ClickHouse should use same database name
        }
        
        if config_modifications:
            config_modifications = dict(config_modifications)  # Make a copy
            
            # Merge with existing mysql settings
            if "mysql" in config_modifications:
                database_settings["mysql"].update(config_modifications["mysql"])
            
            # Merge with existing clickhouse settings  
            if "clickhouse" in config_modifications:
                database_settings["clickhouse"].update(config_modifications["clickhouse"])
                
            config_modifications.update(database_settings)
        else:
            config_modifications = database_settings
        
        print(f"DEBUG: Creating config with MySQL database: {db_name}")
        print(f"DEBUG: Config modifications: {config_modifications}")
        
        # Create isolated config with proper database and directory isolation
        isolated_config_file = create_dynamic_config(
            base_config_path=base_config_file,
            custom_settings=config_modifications
        )
        
        # Track for automatic cleanup
        self.config_files_created.append(isolated_config_file)
        
        print(f"DEBUG: Created isolated config file: {isolated_config_file}")
        if config_modifications:
            print(f"DEBUG: Applied modifications: {config_modifications}")
            
        return isolated_config_file
    
    def start_config_replication(self, config_file: str, use_run_all_runner: bool = False, 
                               db_name: Optional[str] = None) -> None:
        """Start replication processes with enhanced monitoring and error handling
        
        Args:
            config_file: Path to isolated config file
            use_run_all_runner: Use RunAllRunner instead of individual runners
            db_name: Database name override (uses TEST_DB_NAME by default)
        """
        
        from tests.conftest import TEST_DB_NAME
        db_name = db_name or TEST_DB_NAME
        
        print(f"DEBUG: === STARTING CONFIG REPLICATION ===")
        print(f"DEBUG: Config file: {config_file}")
        print(f"DEBUG: Database name: {db_name}")
        print(f"DEBUG: Use RunAllRunner: {use_run_all_runner}")
        
        # Enhanced config file debugging
        try:
            import os
            print(f"DEBUG: Config file exists: {os.path.exists(config_file)}")
            print(f"DEBUG: Config file size: {os.path.getsize(config_file) if os.path.exists(config_file) else 'N/A'} bytes")
            
            # Show config file contents for debugging
            with open(config_file, 'r') as f:
                config_content = f.read()
                print(f"DEBUG: Config file contents:")
                for i, line in enumerate(config_content.split('\n')[:20], 1):  # First 20 lines
                    print(f"DEBUG:   {i:2d}: {line}")
                if len(config_content.split('\n')) > 20:
                    print(f"DEBUG:   ... (truncated, total {len(config_content.split('\n'))} lines)")
                    
        except Exception as config_e:
            print(f"ERROR: Could not read config file: {config_e}")
        
        # CRITICAL FIX: Ensure both MySQL and ClickHouse databases exist BEFORE starting processes
        print(f"DEBUG: Ensuring MySQL database '{db_name}' exists before starting replication...")
        try:
            self.ensure_database_exists(db_name)
            print(f"DEBUG: ✅ MySQL database ensured successfully")
        except Exception as mysql_e:
            print(f"ERROR: Failed to ensure MySQL database: {mysql_e}")
            raise
        
        print(f"DEBUG: About to create ClickHouse database '{db_name}'...")
        try:
            self._create_clickhouse_database(db_name)
            print(f"DEBUG: ✅ ClickHouse database creation attempt completed")
        except Exception as ch_e:
            print(f"ERROR: Failed to create ClickHouse database: {ch_e}")
            import traceback
            print(f"ERROR: ClickHouse creation traceback: {traceback.format_exc()}")
            # Don't raise - let's see what happens
        
        # Enhanced process startup debugging
        try:
            if use_run_all_runner:
                # Use RunAllRunner for complex scenarios
                print(f"DEBUG: Creating RunAllRunner with config: {config_file}")
                runner = RunAllRunner(cfg_file=config_file)
                
                print(f"DEBUG: Starting RunAllRunner...")
                runner.run()
                self.run_all_runners.append(runner)
                
                print(f"DEBUG: RunAllRunner started successfully")
                print(f"DEBUG: Runner process info: {getattr(runner, 'process', 'No process attr')}")
                
                # Check if process started successfully
                if hasattr(runner, 'process') and runner.process:
                    poll_result = runner.process.poll()
                    if poll_result is not None:
                        print(f"ERROR: RunAllRunner process exited immediately with code: {poll_result}")
                    else:
                        print(f"DEBUG: RunAllRunner process running with PID: {runner.process.pid}")
                
            else:
                # Use individual runners (existing BaseReplicationTest pattern)
                print(f"DEBUG: Starting individual runners with config: {config_file}")
                self.start_replication(config_file=config_file)
                print(f"DEBUG: Individual runners started successfully")
                
                # Check individual runner health
                if hasattr(self, 'binlog_runner') and self.binlog_runner and self.binlog_runner.process:
                    poll_result = self.binlog_runner.process.poll()
                    if poll_result is not None:
                        print(f"ERROR: Binlog runner exited immediately with code: {poll_result}")
                    else:
                        print(f"DEBUG: Binlog runner PID: {self.binlog_runner.process.pid}")
                        
                if hasattr(self, 'db_runner') and self.db_runner and self.db_runner.process:
                    poll_result = self.db_runner.process.poll()
                    if poll_result is not None:
                        print(f"ERROR: DB runner exited immediately with code: {poll_result}")
                    else:
                        print(f"DEBUG: DB runner PID: {self.db_runner.process.pid}")
            
        except Exception as startup_e:
            print(f"ERROR: Exception during process startup: {startup_e}")
            import traceback
            print(f"ERROR: Startup traceback: {traceback.format_exc()}")
            raise
        
        # Brief pause to let processes initialize
        import time
        time.sleep(2)
        
        # Wait for database to appear in ClickHouse with enhanced error handling
        print(f"DEBUG: Waiting for database '{db_name}' to appear in ClickHouse...")
        self._wait_for_database_with_health_check(db_name)
        
        # Set ClickHouse database context consistently
        print(f"DEBUG: Setting ClickHouse database context...")
        self._set_clickhouse_context(db_name)
        
        print(f"DEBUG: Configuration replication setup completed for database: {db_name}")
        print(f"DEBUG: === CONFIG REPLICATION STARTED ===")
        
        # Final process health check after setup
        print(f"DEBUG: Final process health check after startup:")
        self._check_process_health()
        
        # Additional debugging - check binlog directory and state files
        self._debug_binlog_and_state_files(config_file)
        
        # CRITICAL: Debug database filtering configuration
        self._debug_database_filtering(config_file, db_name)
        
        # CRITICAL FIX: Clean state files to ensure fresh start
        self._ensure_fresh_binlog_start(config_file)
        
        # CRITICAL: Debug actual replication process configuration
        self._debug_replication_process_config(config_file, db_name)
    
    def wait_for_config_sync(self, table_name: str, expected_count: Optional[int] = None, 
                           max_wait_time: float = 45.0) -> None:
        """Wait for table sync with enhanced error reporting and process health monitoring
        
        Args:
            table_name: Name of table to wait for
            expected_count: Expected record count (optional)
            max_wait_time: Maximum wait time in seconds
        """
        
        def enhanced_table_check():
            print(f"DEBUG: === ENHANCED TABLE CHECK START ===")
            print(f"DEBUG: Looking for table: {table_name}, Expected count: {expected_count}")
            
            # Check process health first with enhanced debugging
            if self.process_health_monitoring:
                process_healthy = self._check_process_health()
                if not process_healthy:
                    print(f"ERROR: Process health check FAILED - processes may have exited")
                    # Continue checking anyway to gather more debugging info
            
            # Update database context in case of transitions
            self._update_database_context_if_needed()
            
            # Enhanced debugging of database and table state
            try:
                # Check current ClickHouse connection and database context
                current_db = getattr(self.ch, 'database', 'UNKNOWN')
                print(f"DEBUG: Current ClickHouse database context: {current_db}")
                
                # Check all available databases
                all_databases = self.ch.get_databases()
                print(f"DEBUG: Available ClickHouse databases: {all_databases}")
                
                # Check if our target database exists in any form
                target_found = False
                for db in all_databases:
                    if current_db in db or db in current_db:
                        target_found = True
                        print(f"DEBUG: Found related database: {db}")
                
                if not target_found:
                    print(f"ERROR: Target database '{current_db}' not found in available databases")
                    return False
                
                # Check tables in current database
                tables = self.ch.get_tables()
                print(f"DEBUG: Available tables in {current_db}: {tables}")
                
                # Enhanced MySQL state debugging
                try:
                    mysql_tables = self.mysql.get_tables()
                    print(f"DEBUG: Available MySQL tables: {mysql_tables}")
                    
                    if table_name.replace(f"_{self._get_worker_test_suffix()}", "") in [t.replace(f"_{self._get_worker_test_suffix()}", "") for t in mysql_tables]:
                        print(f"DEBUG: Corresponding MySQL table exists (with worker suffix variations)")
                        
                        # Check table record count in MySQL
                        try:
                            with self.mysql.get_connection() as (conn, cursor):
                                cursor.execute(f"SELECT COUNT(*) FROM `{table_name}`")
                                mysql_count = cursor.fetchone()[0]
                                print(f"DEBUG: MySQL table '{table_name}' has {mysql_count} records")
                        except Exception as count_e:
                            print(f"DEBUG: Could not count MySQL records: {count_e}")
                    else:
                        print(f"WARNING: No corresponding MySQL table found")
                    
                    # CRITICAL: Check MySQL binlog configuration
                    try:
                        with self.mysql.get_connection() as (conn, cursor):
                            cursor.execute("SHOW VARIABLES LIKE 'log_bin'")
                            binlog_status = cursor.fetchall()
                            print(f"DEBUG: MySQL binlog enabled: {binlog_status}")
                            
                            cursor.execute("SHOW VARIABLES LIKE 'binlog_format'")
                            binlog_format = cursor.fetchall()
                            print(f"DEBUG: MySQL binlog format: {binlog_format}")
                            
                            # Check if there are recent binlog events
                            try:
                                cursor.execute("SHOW BINLOG EVENTS LIMIT 5")
                                binlog_events = cursor.fetchall()
                                print(f"DEBUG: Recent binlog events count: {len(binlog_events)}")
                                if binlog_events:
                                    print(f"DEBUG: Sample binlog event: {binlog_events[0]}")
                            except Exception as binlog_e:
                                print(f"DEBUG: Could not check binlog events: {binlog_e}")
                            
                    except Exception as binlog_config_e:
                        print(f"DEBUG: Could not check MySQL binlog configuration: {binlog_config_e}")
                        
                except Exception as mysql_e:
                    print(f"DEBUG: Could not check MySQL tables: {mysql_e}")
                
                # Check if table exists in ClickHouse
                if table_name not in tables:
                    print(f"DEBUG: Table '{table_name}' NOT FOUND. This indicates replication is not processing events.")
                    
                    # Additional debugging - check for any tables with similar names
                    similar_tables = [t for t in tables if table_name.split('_')[0] in t or table_name.split('_')[-1] in t]
                    if similar_tables:
                        print(f"DEBUG: Found similar table names: {similar_tables}")
                    else:
                        print(f"DEBUG: No similar table names found")
                    
                    return False
                
                # If table exists, check record count
                if expected_count is not None:
                    actual_count = len(self.ch.select(table_name))
                    print(f"DEBUG: Table found! Record count - Expected: {expected_count}, Actual: {actual_count}")
                    
                    if actual_count != expected_count:
                        print(f"DEBUG: Table sync IN PROGRESS. Waiting for more records...")
                        return False
                        
                print(f"DEBUG: SUCCESS - Table '{table_name}' found with correct record count")
                return True
                
            except Exception as e:
                print(f"ERROR: Exception during enhanced table check: {e}")
                print(f"ERROR: Exception type: {type(e).__name__}")
                import traceback
                print(f"ERROR: Traceback: {traceback.format_exc()}")
                return False
            finally:
                print(f"DEBUG: === ENHANCED TABLE CHECK END ===")
        
        # Wait with enhanced error handling
        try:
            assert_wait(enhanced_table_check, max_wait_time=max_wait_time)
            print(f"DEBUG: Table '{table_name}' sync completed successfully")
            
            if expected_count is not None:
                actual_count = len(self.ch.select(table_name))
                print(f"DEBUG: Final record count verified - Expected: {expected_count}, Actual: {actual_count}")
                
        except Exception as e:
            # Enhanced error reporting
            self._provide_detailed_error_context(table_name, expected_count, e)
            raise
    
    def verify_config_test_result(self, table_name: str, verification_queries: Dict[str, Any]) -> None:
        """Verify test results with comprehensive validation
        
        Args:
            table_name: Table to verify
            verification_queries: Dict of verification descriptions and query/expected result pairs
            
        Example:
            verify_config_test_result("users", {
                "record_count": (lambda: len(ch.select("users")), 3),
                "specific_record": (lambda: ch.select("users", where="name='John'"), [{"name": "John", "age": 25}])
            })
        """
        
        print(f"DEBUG: Starting verification for table: {table_name}")
        
        for description, (query_func, expected) in verification_queries.items():
            try:
                actual = query_func()
                assert actual == expected, f"Verification '{description}' failed. Expected: {expected}, Actual: {actual}"
                print(f"DEBUG: ✅ Verification '{description}' passed")
                
            except Exception as e:
                print(f"DEBUG: ❌ Verification '{description}' failed: {e}")
                # Provide context for debugging
                self._provide_verification_context(table_name, description, e)
                raise
        
        print(f"DEBUG: All verifications completed successfully for table: {table_name}")
    
    def _wait_for_database_with_health_check(self, db_name: str) -> None:
        """Wait for database with process health monitoring"""
        
        def database_exists_with_health():
            # Check process health first
            if self.process_health_monitoring:
                if not self._check_process_health():
                    return False
            
            # Check for database existence (handle _tmp transitions)
            databases = self.ch.get_databases()
            final_exists = db_name in databases
            temp_exists = f"{db_name}_tmp" in databases
            
            if final_exists or temp_exists:
                found_db = db_name if final_exists else f"{db_name}_tmp"
                print(f"DEBUG: Found database: {found_db}")
                return True
                
            print(f"DEBUG: Database not found. Available: {databases}")
            return False
        
        assert_wait(database_exists_with_health, max_wait_time=45.0)
    
    def _set_clickhouse_context(self, db_name: str) -> None:
        """Set ClickHouse database context with _tmp transition handling"""
        
        databases = self.ch.get_databases()
        
        if db_name in databases:
            self.ch.database = db_name
            print(f"DEBUG: Set ClickHouse context to final database: {db_name}")
        elif f"{db_name}_tmp" in databases:
            self.ch.database = f"{db_name}_tmp"
            print(f"DEBUG: Set ClickHouse context to temporary database: {db_name}_tmp")
        else:
            print(f"WARNING: Neither {db_name} nor {db_name}_tmp found. Available: {databases}")
            # Try to set anyway for error context
            self.ch.database = db_name
    
    def _update_database_context_if_needed(self) -> None:
        """Update database context if _tmp → final transition occurred"""
        
        if hasattr(self, 'ch') and hasattr(self.ch, 'database'):
            current_db = self.ch.database
            
            if current_db and current_db.endswith('_tmp'):
                # Check if final database now exists
                final_db = current_db.replace('_tmp', '')
                databases = self.ch.get_databases()
                
                if final_db in databases:
                    self.ch.database = final_db
                    print(f"DEBUG: Updated ClickHouse context: {current_db} → {final_db}")
    
    def _check_process_health(self) -> bool:
        """Check if replication processes are still healthy with detailed debugging"""
        
        healthy = True
        active_processes = 0
        
        print(f"DEBUG: === PROCESS HEALTH CHECK ===")
        
        if hasattr(self, 'binlog_runner') and self.binlog_runner:
            if self.binlog_runner.process:
                poll_result = self.binlog_runner.process.poll()
                if poll_result is not None:
                    print(f"ERROR: Binlog runner EXITED with code {poll_result}")
                    # Try to read stderr/stdout for error details
                    try:
                        if hasattr(self.binlog_runner.process, 'stderr') and self.binlog_runner.process.stderr:
                            stderr_output = self.binlog_runner.process.stderr.read()
                            print(f"ERROR: Binlog runner stderr: {stderr_output}")
                    except Exception as e:
                        print(f"DEBUG: Could not read binlog runner stderr: {e}")
                    healthy = False
                else:
                    print(f"DEBUG: Binlog runner is RUNNING (PID: {self.binlog_runner.process.pid})")
                    active_processes += 1
            else:
                print(f"WARNING: Binlog runner exists but no process object")
        else:
            print(f"DEBUG: No binlog_runner found")
        
        if hasattr(self, 'db_runner') and self.db_runner:
            if self.db_runner.process:
                poll_result = self.db_runner.process.poll()
                if poll_result is not None:
                    print(f"ERROR: DB runner EXITED with code {poll_result}")
                    # Try to read stderr/stdout for error details
                    try:
                        if hasattr(self.db_runner.process, 'stderr') and self.db_runner.process.stderr:
                            stderr_output = self.db_runner.process.stderr.read()
                            print(f"ERROR: DB runner stderr: {stderr_output}")
                    except Exception as e:
                        print(f"DEBUG: Could not read db runner stderr: {e}")
                    healthy = False
                else:
                    print(f"DEBUG: DB runner is RUNNING (PID: {self.db_runner.process.pid})")
                    active_processes += 1
            else:
                print(f"WARNING: DB runner exists but no process object")
        else:
            print(f"DEBUG: No db_runner found")
        
        for i, runner in enumerate(self.run_all_runners):
            if hasattr(runner, 'process') and runner.process:
                poll_result = runner.process.poll()
                if poll_result is not None:
                    print(f"ERROR: RunAll runner {i} EXITED with code {poll_result}")
                    healthy = False
                else:
                    print(f"DEBUG: RunAll runner {i} is RUNNING (PID: {runner.process.pid})")
                    active_processes += 1
            else:
                print(f"WARNING: RunAll runner {i} has no process object")
        
        print(f"DEBUG: Process health summary - Active: {active_processes}, Healthy: {healthy}")
        print(f"DEBUG: === END PROCESS HEALTH CHECK ===")
        
        return healthy
    
    def _get_worker_test_suffix(self):
        """Helper to get current worker/test suffix for debugging"""
        try:
            from tests.utils.dynamic_config import get_config_manager
            config_manager = get_config_manager()
            worker_id = config_manager.get_worker_id()
            test_id = config_manager.get_test_id()
            return f"{worker_id}_{test_id}"
        except:
            return "unknown"
    
    def _debug_binlog_and_state_files(self, config_file: str) -> None:
        """Debug binlog directory and replication state files"""
        print(f"DEBUG: === BINLOG & STATE FILE DEBUG ===")
        
        try:
            import yaml
            import os
            
            # Load config to get binlog directory
            with open(config_file, 'r') as f:
                config = yaml.safe_load(f)
            
            binlog_dir = config.get('binlog_replicator', {}).get('data_dir', '/app/binlog')
            print(f"DEBUG: Configured binlog directory: {binlog_dir}")
            
            # Check if binlog directory exists and contents
            if os.path.exists(binlog_dir):
                print(f"DEBUG: Binlog directory exists")
                try:
                    files = os.listdir(binlog_dir)
                    print(f"DEBUG: Binlog directory contents: {files}")
                    
                    # Check for state files
                    state_files = [f for f in files if 'state' in f.lower()]
                    if state_files:
                        print(f"DEBUG: Found state files: {state_files}")
                        
                        # Try to read state file contents
                        for state_file in state_files[:2]:  # Check first 2 state files
                            state_path = os.path.join(binlog_dir, state_file)
                            try:
                                with open(state_path, 'r') as sf:
                                    state_content = sf.read()[:200]  # First 200 chars
                                    print(f"DEBUG: State file {state_file}: {state_content}")
                            except Exception as state_e:
                                print(f"DEBUG: Could not read state file {state_file}: {state_e}")
                    else:
                        print(f"DEBUG: No state files found in binlog directory")
                        
                except Exception as list_e:
                    print(f"DEBUG: Could not list binlog directory contents: {list_e}")
            else:
                print(f"DEBUG: Binlog directory DOES NOT EXIST: {binlog_dir}")
                
                # Check parent directory
                parent_dir = os.path.dirname(binlog_dir)
                if os.path.exists(parent_dir):
                    parent_contents = os.listdir(parent_dir)
                    print(f"DEBUG: Parent directory {parent_dir} contents: {parent_contents}")
                else:
                    print(f"DEBUG: Parent directory {parent_dir} also does not exist")
                    
        except Exception as debug_e:
            print(f"DEBUG: Error during binlog/state debug: {debug_e}")
            
        print(f"DEBUG: === END BINLOG & STATE FILE DEBUG ===")
    
    def _debug_database_filtering(self, config_file: str, expected_db_name: str) -> None:
        """Debug database filtering configuration to identify why binlog events aren't processed"""
        print(f"DEBUG: === DATABASE FILTERING DEBUG ===")
        
        try:
            import yaml
            
            # Load and analyze config
            with open(config_file, 'r') as f:
                config = yaml.safe_load(f)
            
            print(f"DEBUG: Expected database name: {expected_db_name}")
            
            # Check database filtering configuration
            databases_filter = config.get('databases', '')
            print(f"DEBUG: Config databases filter: '{databases_filter}'")
            
            # Analyze if filter matches expected database
            if databases_filter:
                if databases_filter == '*':
                    print(f"DEBUG: Filter '*' should match all databases - OK")
                elif '*test*' in databases_filter:
                    if 'test' in expected_db_name:
                        print(f"DEBUG: Filter '*test*' should match '{expected_db_name}' - OK")
                    else:
                        print(f"ERROR: Filter '*test*' does NOT match '{expected_db_name}' - DATABASE FILTER MISMATCH!")
                elif expected_db_name in databases_filter:
                    print(f"DEBUG: Exact database name match found - OK")
                else:
                    print(f"ERROR: Database filter '{databases_filter}' does NOT match expected '{expected_db_name}' - FILTER MISMATCH!")
            else:
                print(f"WARNING: No databases filter configured - may process all databases")
            
            # Check MySQL connection configuration
            mysql_config = config.get('mysql', {})
            print(f"DEBUG: MySQL config: {mysql_config}")
            
            # Check if there are any target database mappings that might interfere
            target_databases = config.get('target_databases', {})
            print(f"DEBUG: Target database mappings: {target_databases}")
            
            if target_databases:
                print(f"WARNING: Target database mappings exist - may cause routing issues")
                # Check if our expected database is mapped
                for source, target in target_databases.items():
                    if expected_db_name in source or source in expected_db_name:
                        print(f"DEBUG: Found mapping for our database: {source} -> {target}")
            else:
                print(f"DEBUG: No target database mappings - direct replication expected")
                
            # Check binlog replicator configuration
            binlog_config = config.get('binlog_replicator', {})
            print(f"DEBUG: Binlog replicator config: {binlog_config}")
            
            # CRITICAL: Check if processes should be reading from beginning
            data_dir = binlog_config.get('data_dir', '/app/binlog')
            print(f"DEBUG: Binlog data directory: {data_dir}")
            
            # If this is the first run, processes should start from beginning
            # Check if there are existing state files that might cause position issues
            import os
            if os.path.exists(data_dir):
                state_files = [f for f in os.listdir(data_dir) if 'state' in f.lower()]
                if state_files:
                    print(f"WARNING: Found existing state files: {state_files}")
                    print(f"WARNING: Processes may resume from existing position instead of processing test data")
                    
                    # This could be the root cause - processes resume from old position
                    # and miss the test data that was inserted before they started
                    for state_file in state_files:
                        try:
                            state_path = os.path.join(data_dir, state_file)
                            with open(state_path, 'r') as sf:
                                state_content = sf.read()
                                print(f"DEBUG: State file {state_file} content: {state_content[:300]}")
                                
                                # Look for binlog position information
                                if 'binlog' in state_content.lower() or 'position' in state_content.lower():
                                    print(f"CRITICAL: State file contains binlog position - processes may skip test data!")
                        except Exception as state_read_e:
                            print(f"DEBUG: Could not read state file {state_file}: {state_read_e}")
                else:
                    print(f"DEBUG: No existing state files - processes should start from beginning")
            else:
                print(f"DEBUG: Binlog directory doesn't exist yet - processes should create it")
                
        except Exception as debug_e:
            print(f"ERROR: Database filtering debug failed: {debug_e}")
            import traceback
            print(f"ERROR: Debug traceback: {traceback.format_exc()}")
            
        print(f"DEBUG: === END DATABASE FILTERING DEBUG ===")
    
    def _ensure_fresh_binlog_start(self, config_file: str) -> None:
        """Ensure replication starts from beginning by cleaning state files"""
        print(f"DEBUG: === ENSURING FRESH BINLOG START ===")
        
        try:
            import yaml
            import os
            
            # Load config to get binlog directory
            with open(config_file, 'r') as f:
                config = yaml.safe_load(f)
            
            data_dir = config.get('binlog_replicator', {}).get('data_dir', '/app/binlog')
            print(f"DEBUG: Checking binlog directory: {data_dir}")
            
            if os.path.exists(data_dir):
                # Find and remove state files to ensure fresh start
                files = os.listdir(data_dir)
                state_files = [f for f in files if 'state' in f.lower() or f.endswith('.json')]
                
                if state_files:
                    print(f"DEBUG: Found {len(state_files)} state files to clean: {state_files}")
                    
                    for state_file in state_files:
                        try:
                            state_path = os.path.join(data_dir, state_file)
                            os.remove(state_path)
                            print(f"DEBUG: Removed state file: {state_file}")
                        except Exception as remove_e:
                            print(f"WARNING: Could not remove state file {state_file}: {remove_e}")
                    
                    print(f"DEBUG: State files cleaned - processes will start from beginning")
                else:
                    print(f"DEBUG: No state files found - fresh start already ensured")
            else:
                print(f"DEBUG: Binlog directory doesn't exist - will be created fresh")
                
        except Exception as cleanup_e:
            print(f"ERROR: State file cleanup failed: {cleanup_e}")
            print(f"WARNING: Processes may resume from existing position")
            
        print(f"DEBUG: === END FRESH BINLOG START ===")
    
    def _provide_detailed_error_context(self, table_name: str, expected_count: Optional[int], error: Exception) -> None:
        """Provide detailed context when table sync fails"""
        
        print(f"ERROR: Table sync failed for '{table_name}': {error}")
        
        try:
            # Database context
            databases = self.ch.get_databases()
            print(f"DEBUG: Available databases: {databases}")
            print(f"DEBUG: Current database context: {getattr(self.ch, 'database', 'None')}")
            
            # Table context
            if hasattr(self.ch, 'database') and self.ch.database:
                tables = self.ch.get_tables()
                print(f"DEBUG: Available tables in {self.ch.database}: {tables}")
                
                if table_name in tables:
                    actual_count = len(self.ch.select(table_name))
                    print(f"DEBUG: Table exists with {actual_count} records (expected: {expected_count})")
            
            # Process health
            self._check_process_health()
            
        except Exception as context_error:
            print(f"ERROR: Failed to provide error context: {context_error}")
    
    def _provide_verification_context(self, table_name: str, description: str, error: Exception) -> None:
        """Provide context when verification fails"""
        
        print(f"ERROR: Verification '{description}' failed for table '{table_name}': {error}")
        
        try:
            # Show current table contents for debugging
            records = self.ch.select(table_name)
            print(f"DEBUG: Current table contents ({len(records)} records):")
            for i, record in enumerate(records[:5]):  # Show first 5 records
                print(f"DEBUG:   Record {i}: {record}")
            
            if len(records) > 5:
                print(f"DEBUG:   ... and {len(records) - 5} more records")
                
        except Exception as context_error:
            print(f"ERROR: Failed to provide verification context: {context_error}")
    
    def _cleanup_enhanced_resources(self) -> None:
        """Enhanced cleanup - automatically handles all resources"""
        
        print("DEBUG: Starting enhanced resource cleanup...")
        
        # Stop all RunAllRunner instances
        for runner in self.run_all_runners:
            try:
                if hasattr(runner, 'stop'):
                    runner.stop()
                    print(f"DEBUG: Stopped RunAll runner")
            except Exception as e:
                print(f"WARNING: Failed to stop RunAll runner: {e}")
        
        # Stop individual runners (similar to BaseReplicationTest cleanup)
        try:
            if self.db_runner:
                self.db_runner.stop()
                self.db_runner = None
            if self.binlog_runner:
                self.binlog_runner.stop()
                self.binlog_runner = None
            print("DEBUG: Stopped individual replication runners")
        except Exception as e:
            print(f"WARNING: Failed to stop individual runners: {e}")
        
        # Clean up config files
        for config_file in self.config_files_created:
            try:
                if os.path.exists(config_file):
                    os.unlink(config_file)
                    print(f"DEBUG: Removed config file: {config_file}")
            except Exception as e:
                print(f"WARNING: Failed to remove config file {config_file}: {e}")
        
        print("DEBUG: Enhanced resource cleanup completed")
    
    def _debug_replication_process_config(self, config_file: str, expected_db_name: str) -> None:
        """Debug what configuration the replication processes are actually receiving"""
        print(f"DEBUG: === REPLICATION PROCESS CONFIG DEBUG ===")
        
        try:
            import yaml
            import time
            
            # Load the exact config file that processes will use
            with open(config_file, 'r') as f:
                config = yaml.safe_load(f)
            
            print(f"DEBUG: Checking configuration that will be used by replication processes...")
            print(f"DEBUG: Config file path: {config_file}")
            
            # Check critical configuration that affects binlog processing
            mysql_config = config.get('mysql', {})
            print(f"DEBUG: MySQL configuration:")
            print(f"  - Host: {mysql_config.get('host', 'localhost')}")
            print(f"  - Port: {mysql_config.get('port', 3306)}")
            print(f"  - Database: {mysql_config.get('database', 'Not specified!')}")
            print(f"  - User: {mysql_config.get('user', 'root')}")
            
            # Critical: Check if database matches expected
            config_database = mysql_config.get('database')
            if config_database != expected_db_name:
                print(f"CRITICAL ERROR: Database mismatch!")
                print(f"  Expected: {expected_db_name}")
                print(f"  Config:   {config_database}")
            else:
                print(f"DEBUG: Database configuration MATCHES expected: {expected_db_name}")
            
            # Check binlog replicator specific settings
            replication_config = config.get('replication', {})
            print(f"DEBUG: Replication configuration:")
            print(f"  - Resume stream: {replication_config.get('resume_stream', True)}")
            print(f"  - Initial only: {replication_config.get('initial_only', False)}")
            print(f"  - Include tables: {replication_config.get('include_tables', [])}")
            print(f"  - Exclude tables: {replication_config.get('exclude_tables', [])}")
            
            # Critical: Check databases filter
            databases_filter = config.get('databases', '')
            print(f"DEBUG: Database filter: '{databases_filter}'")
            
            if databases_filter and databases_filter != '*':
                filter_matches = False
                if expected_db_name in databases_filter:
                    filter_matches = True
                    print(f"DEBUG: Database filter includes our target database - OK")
                elif '*test*' in databases_filter and 'test' in expected_db_name:
                    filter_matches = True
                    print(f"DEBUG: Wildcard filter '*test*' matches our database - OK")
                
                if not filter_matches:
                    print(f"CRITICAL ERROR: Database filter '{databases_filter}' will BLOCK our database '{expected_db_name}'!")
            else:
                print(f"DEBUG: Database filter allows all databases or not specified - OK")
            
            # Check ClickHouse configuration
            ch_config = config.get('clickhouse', {})
            print(f"DEBUG: ClickHouse configuration:")
            print(f"  - Host: {ch_config.get('host', 'localhost')}")
            print(f"  - Port: {ch_config.get('port', 9123)}")
            print(f"  - Database: {ch_config.get('database', 'default')}")
            
            # Check target database mappings
            target_mappings = config.get('target_databases', {})
            print(f"DEBUG: Target database mappings: {target_mappings}")
            
            # Give processes a moment to fully start up
            print(f"DEBUG: Waiting 3 seconds for processes to fully initialize...")
            time.sleep(3)
            
            # Final check - verify processes are still running
            print(f"DEBUG: Final process status check:")
            self._check_process_health()
            
        except Exception as e:
            print(f"ERROR: Failed to debug process configuration: {e}")
            import traceback
            print(f"ERROR: Config debug traceback: {traceback.format_exc()}")
        
        print(f"DEBUG: === END REPLICATION PROCESS CONFIG DEBUG ===")
    
    def _create_clickhouse_database(self, database_name: str) -> None:
        """Create ClickHouse database for the test
        
        Args:
            database_name: Name of ClickHouse database to create
        """
        print(f"DEBUG: === CREATING CLICKHOUSE DATABASE ===")
        
        try:
            # Validate we have a ClickHouse connection
            print(f"DEBUG: Checking ClickHouse connection availability...")
            print(f"DEBUG: self.ch type: {type(self.ch)}")
            print(f"DEBUG: self.ch attributes: {dir(self.ch)}")
            
            # Use the ClickHouse API instance from the test
            print(f"DEBUG: Creating ClickHouse database: {database_name}")
            
            # Check if database already exists
            existing_databases = self.ch.get_databases()
            print(f"DEBUG: Existing ClickHouse databases: {existing_databases}")
            
            if database_name in existing_databases:
                print(f"DEBUG: ClickHouse database '{database_name}' already exists - OK")
                return
            
            # Use the dedicated create_database method or execute_command
            print(f"DEBUG: Using ClickHouse API create_database method")
            
            try:
                # Try the dedicated method first if available
                if hasattr(self.ch, 'create_database'):
                    print(f"DEBUG: Calling create_database({database_name})")
                    self.ch.create_database(database_name)
                else:
                    # Fallback to execute_command method
                    create_db_query = f"CREATE DATABASE IF NOT EXISTS {database_name}"
                    print(f"DEBUG: Calling execute_command: {create_db_query}")
                    self.ch.execute_command(create_db_query)
                    
                print(f"DEBUG: Successfully executed ClickHouse database creation")
            except Exception as exec_e:
                print(f"DEBUG: Database creation execution failed: {exec_e}")
                # Try alternative method
                create_db_query = f"CREATE DATABASE IF NOT EXISTS {database_name}"
                print(f"DEBUG: Trying alternative query method: {create_db_query}")
                self.ch.query(create_db_query)
                print(f"DEBUG: Alternative query method succeeded")
            
            # Verify creation
            updated_databases = self.ch.get_databases()
            print(f"DEBUG: Databases after creation: {updated_databases}")
            
            if database_name in updated_databases:
                print(f"DEBUG: ✅ Database creation verified - {database_name} exists")
            else:
                print(f"ERROR: ❌ Database creation failed - {database_name} not found in: {updated_databases}")
                
        except AttributeError as attr_e:
            print(f"ERROR: ClickHouse connection not available: {attr_e}")
            print(f"ERROR: self.ch = {getattr(self, 'ch', 'NOT FOUND')}")
            import traceback
            print(f"ERROR: AttributeError traceback: {traceback.format_exc()}")
        except Exception as e:
            print(f"ERROR: Failed to create ClickHouse database '{database_name}': {e}")
            import traceback
            print(f"ERROR: Database creation traceback: {traceback.format_exc()}")
            # Don't raise - let the test continue and see what happens
        
        print(f"DEBUG: === END CLICKHOUSE DATABASE CREATION ===")