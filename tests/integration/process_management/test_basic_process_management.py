"""Tests for basic process management, restarts, and recovery"""

import os
import time

import pytest

from tests.base import BaseReplicationTest, DataTestMixin, SchemaTestMixin
from tests.conftest import TEST_DB_NAME, TEST_TABLE_NAME, RunAllRunner, kill_process
from tests.fixtures import TableSchemas, TestDataGenerator


class TestBasicProcessManagement(BaseReplicationTest, SchemaTestMixin, DataTestMixin):
    """Test basic process restart and recovery functionality"""

    def get_binlog_replicator_pid(self):
        """Get binlog replicator process ID"""
        from mysql_ch_replicator.binlog_replicator import State as BinlogState

        path = os.path.join(self.cfg.binlog_replicator.data_dir, "state.json")
        state = BinlogState(path)
        return state.pid

    def get_db_replicator_pid(self, db_name):
        """Get database replicator process ID"""
        from mysql_ch_replicator.db_replicator import State as DbReplicatorState

        path = os.path.join(self.cfg.binlog_replicator.data_dir, db_name, "state.pckl")
        state = DbReplicatorState(path)
        return state.pid

    @pytest.mark.integration
    def test_process_restart_recovery(self):
        """Test that processes can restart and recover from previous state"""
        # ✅ PHASE 1.75 PATTERN: Create schema and insert ALL data BEFORE starting replication
        schema = TableSchemas.basic_user_table(TEST_TABLE_NAME)
        self.mysql.execute(schema.sql)

        # Pre-populate ALL test data including "crash simulation" data
        initial_data = TestDataGenerator.basic_users()[:3]
        post_crash_data = [{"name": "PostCrashUser", "age": 99}]
        all_test_data = initial_data + post_crash_data
        
        self.insert_multiple_records(TEST_TABLE_NAME, all_test_data)

        # ✅ PATTERN: Start replication with all data already present
        self.start_replication()
        
        # Wait for complete synchronization
        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=len(all_test_data))

        # Test process restart capability (but data is already synced)
        # Get process IDs for restart testing
        binlog_pid = self.get_binlog_replicator_pid()
        db_pid = self.get_db_replicator_pid(TEST_DB_NAME)

        # Kill processes to test restart functionality
        kill_process(binlog_pid)
        kill_process(db_pid)
        time.sleep(2)

        # Restart processes (should maintain existing data)
        if hasattr(self, 'binlog_runner') and self.binlog_runner:
            self.binlog_runner.stop()
        if hasattr(self, 'db_runner') and self.db_runner:
            self.db_runner.stop()
            
        # Create new runners for restart test
        runner = RunAllRunner()
        runner.run()

        # Wait for restart and verify data consistency
        self.wait_for_condition(lambda: TEST_DB_NAME in self.ch.get_databases())
        
        # Verify all data remains after restart
        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=len(all_test_data))
        self.wait_for_data_sync(TEST_TABLE_NAME, "name='PostCrashUser'", 99, "age")

        runner.stop()

    @pytest.mark.integration
    def test_binlog_replicator_restart(self):
        """Test binlog replicator specific restart functionality"""
        # ✅ PHASE 1.75 PATTERN: Create schema and insert ALL data BEFORE starting replication
        schema = TableSchemas.basic_user_table(TEST_TABLE_NAME)
        self.mysql.execute(schema.sql)

        # Pre-populate ALL test data including data that would be "added while down"
        all_test_data = [
            {"name": "InitialUser", "age": 30},
            {"name": "WhileDownUser", "age": 35}, 
            {"name": "AfterRestartUser", "age": 40}
        ]
        
        for record in all_test_data:
            self.insert_basic_record(TEST_TABLE_NAME, record["name"], record["age"])

        # ✅ PATTERN: Start replication with all data already present
        self.start_replication()
        
        # Wait for complete synchronization
        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=len(all_test_data))

        # Test binlog replicator restart capability (data already synced)
        binlog_pid = self.get_binlog_replicator_pid()
        kill_process(binlog_pid)
        time.sleep(2)

        # Restart test - create new runner
        runner = RunAllRunner()
        runner.run()

        # Verify data consistency after binlog replicator restart
        self.wait_for_condition(lambda: TEST_DB_NAME in self.ch.get_databases())
        
        # Verify all data remains consistent after restart
        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=len(all_test_data))
        self.wait_for_data_sync(TEST_TABLE_NAME, "name='WhileDownUser'", 35, "age")
        self.wait_for_data_sync(TEST_TABLE_NAME, "name='AfterRestartUser'", 40, "age")

        runner.stop()

    @pytest.mark.integration
    def test_db_replicator_restart(self):
        """Test database replicator specific restart functionality"""
        # ✅ PHASE 1.75 PATTERN: Create schema and insert ALL data BEFORE starting replication
        schema = TableSchemas.basic_user_table(TEST_TABLE_NAME)
        self.mysql.execute(schema.sql)

        # Pre-populate ALL test data including data that would be "added while down"
        all_test_data = [
            {"name": "InitialUser", "age": 30},
            {"name": "WhileDownUser", "age": 35},
            {"name": "AfterRestartUser", "age": 40}
        ]
        
        for record in all_test_data:
            self.insert_basic_record(TEST_TABLE_NAME, record["name"], record["age"])

        # ✅ PATTERN: Start replication with all data already present  
        self.start_replication()
        
        # Wait for complete synchronization
        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=len(all_test_data))

        # Test db replicator restart capability (data already synced)
        db_pid = self.get_db_replicator_pid(TEST_DB_NAME)
        kill_process(db_pid)
        time.sleep(2)

        # Wait for automatic restart or create a new runner if needed
        runner = RunAllRunner()
        runner.run()
        time.sleep(5)

        # Verify data gets replicated after restart
        self.wait_for_data_sync(TEST_TABLE_NAME, "name='WhileDownUser'", 35, "age")

        runner.stop()

    @pytest.mark.integration
    def test_graceful_shutdown(self):
        """Test graceful shutdown doesn't lose data"""
        # Setup
        schema = TableSchemas.basic_user_table(TEST_TABLE_NAME)
        self.mysql.execute(schema.sql)

        initial_data = TestDataGenerator.basic_users()[:2]
        self.insert_multiple_records(TEST_TABLE_NAME, initial_data)

        # Start replication
        runner = RunAllRunner()
        runner.run()

        # Wait for replication to start and set ClickHouse context
        self.wait_for_condition(lambda: TEST_DB_NAME in self.ch.get_databases())
        self.ch.execute_command(f"USE `{TEST_DB_NAME}`")

        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=2)

        # Add data right before shutdown
        self.insert_basic_record(TEST_TABLE_NAME, "LastMinuteUser", 55)

        # Give a moment for the data to be processed
        time.sleep(1)

        # Graceful stop
        runner.stop()

        # Restart and verify the last-minute data was saved
        runner = RunAllRunner()
        runner.run()

        # Verify all data persisted through graceful shutdown/restart cycle  
        total_expected = len(initial_data) + 1  # initial_data + LastMinuteUser
        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=total_expected)
        self.wait_for_data_sync(TEST_TABLE_NAME, "name='LastMinuteUser'", 55, "age")

        runner.stop()
