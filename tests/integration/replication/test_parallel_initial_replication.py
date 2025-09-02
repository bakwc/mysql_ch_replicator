"""Tests for parallel initial replication scenarios"""

import pytest

from tests.base import BaseReplicationTest, DataTestMixin, SchemaTestMixin
from tests.conftest import TEST_DB_NAME, TEST_TABLE_NAME, RunAllRunner
from tests.fixtures import TableSchemas, TestDataGenerator


class TestParallelInitialReplication(
    BaseReplicationTest, SchemaTestMixin, DataTestMixin
):
    """Test parallel initial replication scenarios"""

    @pytest.mark.integration
    @pytest.mark.parametrize(
        "config_file",
        [
            "tests/configs/replicator/tests_config.yaml",
            "tests/configs/replicator/tests_config_parallel.yaml",
        ],
    )
    def test_parallel_initial_replication(self, config_file):
        """Test parallel initial replication with multiple workers"""
        # Setup complex table with multiple records
        schema = TableSchemas.complex_employee_table(TEST_TABLE_NAME)
        self.mysql.execute(schema.sql)

        # Insert test data that can be processed in parallel
        test_data = TestDataGenerator.complex_employee_records()
        self.insert_multiple_records(TEST_TABLE_NAME, test_data)

        # Add more records to make parallel processing worthwhile
        for i in range(10):
            self.insert_basic_record(TEST_TABLE_NAME, f"Employee_{i}", 25 + i)

        # ✅ CRITICAL FIX: Use isolated config for parallel processing
        from tests.utils.dynamic_config import create_dynamic_config
        
        isolated_config = create_dynamic_config(base_config_path=config_file)
        
        try:
            runner = RunAllRunner(cfg_file=isolated_config)
            runner.run()

            # Wait for replication to complete
            self.wait_for_table_sync(TEST_TABLE_NAME)

            # Verify all data is replicated correctly
            expected_count = len(test_data) + 10
            self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=expected_count)

            # Verify specific records
            self.verify_record_exists(TEST_TABLE_NAME, "name='Employee_5'", {"age": 30})

            runner.stop()
        
        finally:
            # ✅ CLEANUP: Remove isolated config file
            import os
            if os.path.exists(isolated_config):
                os.unlink(isolated_config)

    @pytest.mark.integration
    def test_parallel_initial_replication_record_versions_advanced(self):
        """
        Test that record versions are properly consolidated from worker states
        after parallel initial replication with large dataset.
        """
        import time

        from tests.conftest import BinlogReplicatorRunner, DbReplicatorRunner

        # ✅ CRITICAL FIX: Use isolated config instead of hardcoded parallel config
        from tests.utils.dynamic_config import create_dynamic_config
        
        config_file = create_dynamic_config(
            base_config_path="tests/configs/replicator/tests_config_parallel.yaml"
        )

        # Manually load config to check parallel settings
        self.cfg.load(config_file)

        # Ensure we have parallel replication configured
        assert self.cfg.initial_replication_threads > 1, (
            "This test requires initial_replication_threads > 1"
        )

        # Create a table with sufficient records for parallel processing
        schema = TableSchemas.basic_user_table(TEST_TABLE_NAME)
        self.mysql.execute(
            schema.sql.replace(
                "PRIMARY KEY (id)", "version int NOT NULL DEFAULT 1, PRIMARY KEY (id)"
            )
        )

        # Insert a large number of records to ensure parallel processing
        # Use a single connection context to ensure all operations use the same connection
        with self.mysql.get_connection() as (connection, cursor):
            for i in range(1, 1001):
                cursor.execute(
                    f"INSERT INTO `{TEST_TABLE_NAME}` (name, age, version) VALUES ('User{i}', {20 + i % 50}, {i});"
                )
                if i % 100 == 0:  # Commit every 100 records
                    connection.commit()

            # Ensure final commit for any remaining uncommitted records (records 901-1000)
            connection.commit()

        # Run initial replication only with parallel workers
        db_replicator_runner = DbReplicatorRunner(TEST_DB_NAME, cfg_file=config_file)
        db_replicator_runner.run()

        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=1000)

        db_replicator_runner.stop()

        # Verify database and table were created
        assert TEST_DB_NAME in self.ch.get_databases()
        self.ch.execute_command(f"USE `{TEST_DB_NAME}`")
        assert TEST_TABLE_NAME in self.ch.get_tables()

        # Verify all records were replicated
        records = self.ch.select(TEST_TABLE_NAME)
        assert len(records) == 1000

        # Check the max _version in the ClickHouse table for version handling
        versions_query = self.ch.query(
            f"SELECT MAX(_version) FROM `{TEST_DB_NAME}`.`{TEST_TABLE_NAME}`"
        )
        max_version_in_ch = versions_query.result_rows[0][0]
        assert max_version_in_ch >= 200, (
            f"Expected max _version to be at least 200, got {max_version_in_ch}"
        )

        # Now test realtime replication to verify versions continue correctly
        # Start binlog replication
        binlog_replicator_runner = BinlogReplicatorRunner(cfg_file=config_file)
        binlog_replicator_runner.run()

        time.sleep(3.0)

        # Start DB replicator in realtime mode
        realtime_db_replicator = DbReplicatorRunner(TEST_DB_NAME, cfg_file=config_file)
        realtime_db_replicator.run()

        # Insert a new record with version 1001
        self.mysql.execute(
            f"INSERT INTO `{TEST_TABLE_NAME}` (name, age, version) VALUES ('UserRealtime', 99, 1001);",
            commit=True,
        )

        # Wait for the record to be replicated
        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=1001)

        # Verify the new record was replicated correctly
        realtime_record = self.ch.select(TEST_TABLE_NAME, where="name='UserRealtime'")[
            0
        ]
        assert realtime_record["age"] == 99
        assert realtime_record["version"] == 1001

        # Check that the _version column in CH is a reasonable value
        versions_query = self.ch.query(
            f"SELECT _version FROM `{TEST_DB_NAME}`.`{TEST_TABLE_NAME}` WHERE name='UserRealtime'"
        )
        ch_version = versions_query.result_rows[0][0]

        # With parallel workers (default is 4), each worker would process ~250 records
        # So the version for the new record should be slightly higher than 250
        # but definitely lower than 1000
        assert ch_version > 0, (
            f"ClickHouse _version should be > 0, but got {ch_version}"
        )

        # We expect version to be roughly: (total_records / num_workers) + 1
        # For 1000 records and 4 workers, expect around 251
        expected_version_approx = 1000 // self.cfg.initial_replication_threads + 1
        # Allow some flexibility in the exact expected value
        assert abs(ch_version - expected_version_approx) < 50, (
            f"ClickHouse _version should be close to {expected_version_approx}, but got {ch_version}"
        )

        # Clean up
        binlog_replicator_runner.stop()
        realtime_db_replicator.stop()
        db_replicator_runner.stop()
        
        # ✅ CLEANUP: Remove isolated config file
        import os
        if os.path.exists(config_file):
            os.unlink(config_file)
