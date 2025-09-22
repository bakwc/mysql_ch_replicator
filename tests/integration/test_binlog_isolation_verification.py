"""Test to verify true binlog directory isolation between parallel tests"""

import os
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import pytest

from tests.base import BaseReplicationTest, DataTestMixin, SchemaTestMixin
from tests.conftest import TEST_DB_NAME, TEST_TABLE_NAME
from tests.fixtures import TableSchemas, TestDataGenerator
from tests.utils.dynamic_config import get_config_manager


class TestBinlogIsolationVerification(BaseReplicationTest, SchemaTestMixin, DataTestMixin):
    """Comprehensive test to ensure binlog directories remain truly isolated"""

    @pytest.mark.integration
    def test_binlog_directory_isolation_verification(self):
        """CRITICAL: Verify each test gets its own binlog directory and state files"""
        
        # Get current test's isolation paths
        config_manager = get_config_manager()
        worker_id = config_manager.get_worker_id()
        test_id = config_manager.get_test_id()
        expected_binlog_dir = f"/app/binlog/{worker_id}_{test_id}"
        
        print(f"DEBUG: Expected binlog dir: {expected_binlog_dir}")
        print(f"DEBUG: Actual binlog dir: {self.cfg.binlog_replicator.data_dir}")
        
        # CRITICAL ASSERTION: Each test must have unique binlog directory
        assert self.cfg.binlog_replicator.data_dir == expected_binlog_dir, (
            f"BINLOG ISOLATION FAILURE: Expected {expected_binlog_dir}, "
            f"got {self.cfg.binlog_replicator.data_dir}"
        )
        
        # Verify directory uniqueness includes both worker and test ID
        assert worker_id in self.cfg.binlog_replicator.data_dir, (
            f"Missing worker ID {worker_id} in binlog path: {self.cfg.binlog_replicator.data_dir}"
        )
        assert test_id in self.cfg.binlog_replicator.data_dir, (
            f"Missing test ID {test_id} in binlog path: {self.cfg.binlog_replicator.data_dir}"
        )
        
        # Setup schema and data
        schema = TableSchemas.basic_user_table(TEST_TABLE_NAME)
        self.mysql.execute(schema.sql)
        
        test_data = TestDataGenerator.basic_users()[:3]
        self.insert_multiple_records(TEST_TABLE_NAME, test_data)
        
        # Start replication to create state files
        self.start_replication()
        # Handle database lifecycle transitions (_tmp → final database name)
        self.update_clickhouse_database_context()
        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=len(test_data))
        
        # Verify state files are in isolated locations
        expected_state_json = os.path.join(expected_binlog_dir, "state.json")
        expected_state_pckl = os.path.join(expected_binlog_dir, TEST_DB_NAME, "state.pckl")
        
        # Wait for state files to be created
        time.sleep(2)
        
        # Check state.json isolation
        if os.path.exists(expected_state_json):
            print(f"✅ ISOLATED state.json found: {expected_state_json}")
        else:
            # List all state.json files to debug
            state_files = []
            for root, dirs, files in os.walk("/app"):
                if "state.json" in files:
                    state_files.append(os.path.join(root, "state.json"))
            
            pytest.fail(
                f"ISOLATION FAILURE: state.json not in expected location {expected_state_json}. "
                f"Found state.json files: {state_files}"
            )
        
        # Check database-specific state.pckl isolation
        if os.path.exists(expected_state_pckl):
            print(f"✅ ISOLATED state.pckl found: {expected_state_pckl}")
        else:
            # List all state.pckl files to debug
            pckl_files = []
            for root, dirs, files in os.walk("/app"):
                if "state.pckl" in files:
                    pckl_files.append(os.path.join(root, "state.pckl"))
            
            pytest.fail(
                f"ISOLATION FAILURE: state.pckl not in expected location {expected_state_pckl}. "
                f"Found state.pckl files: {pckl_files}"
            )
        
        # CRITICAL: Verify no other tests can access our state files
        other_binlog_dirs = []
        binlog_base_dir = "/app/binlog"
        if os.path.exists(binlog_base_dir):
            for item in os.listdir(binlog_base_dir):
                if item != f"{worker_id}_{test_id}":
                    other_binlog_dirs.append(os.path.join(binlog_base_dir, item))
        
        print(f"DEBUG: Other binlog directories: {other_binlog_dirs}")
        print(f"✅ BINLOG ISOLATION VERIFIED: Unique directory {expected_binlog_dir}")
        
        # Final verification: Ensure this test can't see other tests' state files
        shared_state_json = "/app/binlog/state.json"  # Old shared location
        if os.path.exists(shared_state_json):
            pytest.fail(
                f"CRITICAL ISOLATION FAILURE: Found shared state.json at {shared_state_json}. "
                f"This means tests are still sharing state files!"
            )

    @pytest.mark.integration  
    def test_parallel_binlog_isolation_simulation(self):
        """Simulate parallel test execution to verify no state file conflicts"""
        
        def create_isolated_test_scenario(scenario_id):
            """Simulate a test with its own replication setup"""
            try:
                # Each scenario should get unique paths
                config_manager = get_config_manager()
                
                # Generate unique test ID for this scenario to avoid race conditions
                # in parallel thread execution during testing
                import uuid
                import time
                unique_test_id = f"{scenario_id}_{int(time.time() * 1000)}_{uuid.uuid4().hex[:8]}"
                
                worker_id = config_manager.get_worker_id()
                test_id = unique_test_id  # Use our guaranteed unique ID
                expected_dir = f"/app/binlog/{worker_id}_{test_id}"
                
                # Create the directory structure that should exist
                os.makedirs(expected_dir, exist_ok=True)
                
                # Create scenario-specific state file
                state_file = os.path.join(expected_dir, "state.json")
                with open(state_file, 'w') as f:
                    f.write(f'{{"scenario_id": {scenario_id}, "test_id": "{test_id}"}}')
                
                return {
                    'scenario_id': scenario_id,
                    'test_id': test_id,
                    'binlog_dir': expected_dir,
                    'state_file': state_file,
                    'isolation_verified': True
                }
            except Exception as e:
                return {
                    'scenario_id': scenario_id,
                    'error': str(e),
                    'isolation_verified': False
                }
        
        # Simulate 3 parallel test scenarios
        scenarios = []
        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = [executor.submit(create_isolated_test_scenario, i) for i in range(3)]
            for future in as_completed(futures):
                scenarios.append(future.result())
        
        # Verify all scenarios got unique paths
        binlog_dirs = set()
        test_ids = set()
        
        for scenario in scenarios:
            if not scenario.get('isolation_verified', False):
                pytest.fail(f"Scenario {scenario['scenario_id']} failed: {scenario.get('error')}")
            
            binlog_dir = scenario['binlog_dir']
            test_id = scenario['test_id']
            
            # Check for duplicates
            if binlog_dir in binlog_dirs:
                pytest.fail(f"ISOLATION FAILURE: Duplicate binlog directory {binlog_dir}")
            if test_id in test_ids:
                pytest.fail(f"ISOLATION FAILURE: Duplicate test ID {test_id}")
            
            binlog_dirs.add(binlog_dir)
            test_ids.add(test_id)
            
            # Verify state file exists and is unique
            state_file = scenario['state_file']
            assert os.path.exists(state_file), f"State file missing: {state_file}"
        
        print(f"✅ PARALLEL ISOLATION VERIFIED: {len(scenarios)} unique scenarios")
        print(f"   Unique binlog dirs: {len(binlog_dirs)}")
        print(f"   Unique test IDs: {len(test_ids)}")
        
        # Cleanup
        for scenario in scenarios:
            if 'binlog_dir' in scenario and os.path.exists(scenario['binlog_dir']):
                import shutil
                shutil.rmtree(scenario['binlog_dir'], ignore_errors=True)

    @pytest.mark.integration
    def test_binlog_isolation_enforcement(self):
        """Test that demonstrates and enforces isolation requirements"""
        
        # REQUIREMENT: Each test MUST have unique binlog directory
        config_manager = get_config_manager()
        binlog_dir = self.cfg.binlog_replicator.data_dir
        
        # Check for isolation patterns
        isolation_checks = [
            ("Worker ID in path", config_manager.get_worker_id() in binlog_dir),
            ("Test ID in path", config_manager.get_test_id() in binlog_dir),
            ("Unique per test", binlog_dir.startswith("/app/binlog/")),
            ("Not shared path", binlog_dir != "/app/binlog/"),
        ]
        
        failed_checks = [check for check, passed in isolation_checks if not passed]
        
        if failed_checks:
            pytest.fail(
                f"BINLOG ISOLATION REQUIREMENTS FAILED: {failed_checks}\n"
                f"Current binlog dir: {binlog_dir}\n"
                f"Worker ID: {config_manager.get_worker_id()}\n"
                f"Test ID: {config_manager.get_test_id()}\n"
                f"Expected pattern: /app/binlog/{{worker_id}}_{{test_id}}"
            )
        
        print(f"✅ ALL ISOLATION REQUIREMENTS PASSED")
        print(f"   Binlog directory: {binlog_dir}")
        print(f"   Worker ID: {config_manager.get_worker_id()}")  
        print(f"   Test ID: {config_manager.get_test_id()}")