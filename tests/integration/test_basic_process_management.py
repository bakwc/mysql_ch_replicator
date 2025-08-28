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
        # Setup initial data
        schema = TableSchemas.basic_user_table(TEST_TABLE_NAME)
        self.mysql.execute(schema.sql)

        initial_data = TestDataGenerator.basic_users()[:3]
        self.insert_multiple_records(TEST_TABLE_NAME, initial_data)

        # Start replication
        runner = RunAllRunner()
        runner.run()

        # Wait for initial replication
        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=3)

        # Get process IDs before restart
        binlog_pid = self.get_binlog_replicator_pid()
        db_pid = self.get_db_replicator_pid(TEST_DB_NAME)

        # Kill processes to simulate crash
        kill_process(binlog_pid)
        kill_process(db_pid)

        # Wait a bit for processes to actually stop
        time.sleep(2)

        # Add more data while processes are down
        self.insert_basic_record(TEST_TABLE_NAME, "PostCrashUser", 99)

        # Restart runner (should recover from state)
        runner.stop()  # Make sure it's fully stopped
        runner = RunAllRunner()
        runner.run()

        # Verify recovery - new data should be replicated
        self.wait_for_data_sync(TEST_TABLE_NAME, "name='PostCrashUser'", 99, "age")

        # Verify total count
        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=4)

        runner.stop()

    @pytest.mark.integration
    def test_binlog_replicator_restart(self):
        """Test binlog replicator specific restart functionality"""
        # Setup
        schema = TableSchemas.basic_user_table(TEST_TABLE_NAME)
        self.mysql.execute(schema.sql)

        self.insert_basic_record(TEST_TABLE_NAME, "InitialUser", 30)

        # Start replication
        runner = RunAllRunner()
        runner.run()

        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=1)

        # Kill only binlog replicator
        binlog_pid = self.get_binlog_replicator_pid()
        kill_process(binlog_pid)

        # Add data while binlog replicator is down
        self.insert_basic_record(TEST_TABLE_NAME, "WhileDownUser", 35)

        # Wait for automatic restart (runner should restart it)
        time.sleep(5)

        # Add more data after restart
        self.insert_basic_record(TEST_TABLE_NAME, "AfterRestartUser", 40)

        # Verify all data is eventually replicated
        self.wait_for_data_sync(TEST_TABLE_NAME, "name='WhileDownUser'", 35, "age")
        self.wait_for_data_sync(TEST_TABLE_NAME, "name='AfterRestartUser'", 40, "age")

        runner.stop()

    @pytest.mark.integration
    def test_db_replicator_restart(self):
        """Test database replicator specific restart functionality"""
        # Setup
        schema = TableSchemas.basic_user_table(TEST_TABLE_NAME)
        self.mysql.execute(schema.sql)

        self.insert_basic_record(TEST_TABLE_NAME, "InitialUser", 30)

        # Start replication
        runner = RunAllRunner()
        runner.run()

        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=1)

        # Kill only db replicator
        db_pid = self.get_db_replicator_pid(TEST_DB_NAME)
        kill_process(db_pid)

        # Add data while db replicator is down
        self.insert_basic_record(TEST_TABLE_NAME, "WhileDownUser", 35)

        # Wait for automatic restart
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

        self.wait_for_data_sync(TEST_TABLE_NAME, "name='LastMinuteUser'", 55, "age")

        runner.stop()
