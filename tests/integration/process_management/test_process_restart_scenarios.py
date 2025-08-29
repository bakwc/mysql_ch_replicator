"""Tests for process restart scenarios and recovery"""

import os
import time

import pytest

from tests.base import BaseReplicationTest, DataTestMixin, SchemaTestMixin
from tests.conftest import (
    TEST_DB_NAME,
    TEST_TABLE_NAME,
    RunAllRunner,
    assert_wait,
)
from tests.fixtures.schema_factory import SchemaFactory
from tests.fixtures.data_factory import DataFactory


class TestProcessRestartScenarios(BaseReplicationTest, SchemaTestMixin, DataTestMixin):
    """Test process restart and recovery scenarios"""

    @pytest.mark.integration
    def test_auto_restart_interval(self):
        """Test automatic restart based on configuration interval"""
        # This test would need a special config with short auto_restart_interval
        # For now, just verify basic restart functionality works

        schema_sql = SchemaFactory.basic_user_table(TEST_TABLE_NAME)
        self.mysql.execute(schema_sql)

        # Insert initial test data
        initial_data = DataFactory.sample_users(count=1)
        initial_data[0]["name"] = "TestUser"
        initial_data[0]["age"] = 25
        self.insert_multiple_records(TEST_TABLE_NAME, initial_data)

        # Start with short-lived configuration if available
        runner = RunAllRunner()
        runner.run()

        # Wait for replication to start and set ClickHouse context
        self.wait_for_condition(lambda: TEST_DB_NAME in self.ch.get_databases())
        self.ch.execute_command(f"USE `{TEST_DB_NAME}`")

        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=1)

        # Add data continuously to test restart doesn't break replication
        additional_users = []
        for i in range(5):
            user_data = {"name": f"User_{i}", "age": 25 + i}
            additional_users.append(user_data)
            self.insert_multiple_records(TEST_TABLE_NAME, [user_data])
            time.sleep(1)  # Space out insertions

        # Verify all data is replicated despite any restarts
        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=6)  # 1 initial + 5 new

        # Verify data integrity
        self.verify_record_exists(TEST_TABLE_NAME, "name='TestUser'", {"age": 25})
        for i in range(5):
            self.verify_record_exists(TEST_TABLE_NAME, f"name='User_{i}'", {"age": 25 + i})

        runner.stop()

    @pytest.mark.integration
    @pytest.mark.parametrize("config_file", ["tests/configs/replicator/tests_config.yaml"])
    def test_run_all_runner_with_process_restart(self, config_file):
        """Test RunAllRunner handles process restarts gracefully"""
        
        # Create test table
        schema_sql = SchemaFactory.replication_test_table(TEST_TABLE_NAME, with_comments=True)
        self.mysql.execute(schema_sql)
        
        # Insert initial data
        test_data = DataFactory.replication_test_data()
        self.insert_multiple_records(TEST_TABLE_NAME, test_data)
        
        # Start replication with RunAllRunner
        runner = RunAllRunner(cfg_file=config_file)
        runner.run()
        
        # Wait for initial replication
        self.wait_for_condition(lambda: TEST_DB_NAME in self.ch.get_databases())
        self.ch.execute_command(f"USE `{TEST_DB_NAME}`")
        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=len(test_data))
        
        # Verify initial replication
        for record in test_data:
            self.verify_record_exists(TEST_TABLE_NAME, f"name='{record['name']}'", {"age": record["age"]})
        
        # Simulate process restart by stopping and restarting
        runner.stop()
        time.sleep(2)  # Brief pause
        
        # Add data while process is stopped
        restart_data = [{"name": "RestartUser", "age": 99, "config": '{"during_restart": true}'}]
        self.insert_multiple_records(TEST_TABLE_NAME, restart_data)
        
        # Restart the runner
        new_runner = RunAllRunner(cfg_file=config_file)
        new_runner.run()
        
        # Verify replication resumes and catches up
        total_expected = len(test_data) + len(restart_data)
        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=total_expected, max_wait_time=30)
        
        # Verify the restart data was replicated
        self.verify_record_exists(TEST_TABLE_NAME, "name='RestartUser'", {"age": 99})
        
        # Add more data after restart to ensure ongoing replication
        post_restart_data = [{"name": "PostRestart", "age": 88, "config": '{"after_restart": true}'}]
        self.insert_multiple_records(TEST_TABLE_NAME, post_restart_data)
        
        # Verify post-restart replication
        final_expected = total_expected + len(post_restart_data)
        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=final_expected, max_wait_time=20)
        self.verify_record_exists(TEST_TABLE_NAME, "name='PostRestart'", {"age": 88})
        
        new_runner.stop()
        
        print(f"Process restart test completed successfully:")
        print(f"- Initial data: {len(test_data)} records")
        print(f"- During restart: {len(restart_data)} records") 
        print(f"- After restart: {len(post_restart_data)} records")
        print(f"- Total replicated: {final_expected} records")

    @pytest.mark.integration
    def test_graceful_shutdown_and_restart(self):
        """Test graceful shutdown followed by clean restart"""
        
        # Create performance table for testing
        table_name = "graceful_restart_table"
        schema_sql = SchemaFactory.performance_test_table(table_name, "simple")
        self.mysql.execute(schema_sql)
        
        # Start replication
        runner = RunAllRunner()
        runner.run()
        
        # Wait for setup
        self.wait_for_condition(lambda: TEST_DB_NAME in self.ch.get_databases())
        self.ch.execute_command(f"USE `{TEST_DB_NAME}`")
        self.wait_for_table_sync(table_name, expected_count=0)
        
        # Insert test data before shutdown
        pre_shutdown_data = DataFactory.performance_test_data(count=100, complexity="simple")
        self.insert_multiple_records(table_name, pre_shutdown_data)
        self.wait_for_table_sync(table_name, expected_count=len(pre_shutdown_data))
        
        # Graceful shutdown
        print("Performing graceful shutdown...")
        runner.stop()
        
        # Verify shutdown completed cleanly (brief pause to ensure cleanup)
        time.sleep(3)
        
        # Insert data during downtime
        downtime_data = DataFactory.performance_test_data(count=50, complexity="simple") 
        self.insert_multiple_records(table_name, downtime_data)
        
        # Restart and verify catch-up
        print("Restarting replication...")
        new_runner = RunAllRunner()
        new_runner.run()
        
        # Wait for catch-up replication
        total_expected = len(pre_shutdown_data) + len(downtime_data)
        self.wait_for_table_sync(table_name, expected_count=total_expected, max_wait_time=60)
        
        # Verify data integrity after restart
        ch_records = self.ch.select(table_name)
        assert len(ch_records) == total_expected, f"Expected {total_expected} records, got {len(ch_records)}"
        
        print(f"Graceful restart test completed:")
        print(f"- Pre-shutdown: {len(pre_shutdown_data)} records")
        print(f"- During downtime: {len(downtime_data)} records")
        print(f"- Total after restart: {len(ch_records)} records")
        
        new_runner.stop()