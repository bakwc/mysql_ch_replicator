"""Tests for parallel worker scenarios and realtime processing"""

import pytest

from tests.base import BaseReplicationTest, DataTestMixin, SchemaTestMixin
from tests.conftest import (
    TEST_DB_NAME,
    TEST_TABLE_NAME,
    RunAllRunner,
    mysql_create_database,
    mysql_drop_database,
)
from tests.fixtures import TableSchemas, TestDataGenerator


class TestParallelWorkerScenarios(BaseReplicationTest, SchemaTestMixin, DataTestMixin):
    """Test parallel worker and realtime replication scenarios"""

    @pytest.mark.integration
    def test_parallel_record_versions(self):
        """Test parallel processing maintains record versions correctly"""
        # Create table with records that will get version numbers
        schema = TableSchemas.basic_user_table(TEST_TABLE_NAME)
        self.mysql.execute(schema.sql)

        # Insert initial batch
        initial_data = TestDataGenerator.basic_users()
        self.insert_multiple_records(TEST_TABLE_NAME, initial_data)

        # Start parallel replication
        runner = RunAllRunner(
            cfg_file="tests/configs/replicator/tests_config_parallel.yaml"
        )
        runner.run()

        # Wait for replication to start and set ClickHouse database context
        self.wait_for_condition(lambda: TEST_DB_NAME in self.ch.get_databases())
        self.ch.database = TEST_DB_NAME

        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=len(initial_data))

        # Update some records (this should create new versions)
        self.update_record(TEST_TABLE_NAME, "name='Ivan'", {"age": 43})
        self.update_record(TEST_TABLE_NAME, "name='Peter'", {"age": 34})

        # Wait for updates to be processed
        self.wait_for_data_sync(TEST_TABLE_NAME, "name='Ivan'", 43, "age")
        self.wait_for_data_sync(TEST_TABLE_NAME, "name='Peter'", 34, "age")

        # Verify record counts are still correct (ReplacingMergeTree handles versions)
        self.verify_counts_match(TEST_TABLE_NAME)

        runner.stop()

    @pytest.mark.integration
    def test_worker_failure_recovery(self):
        """Test that worker failures don't break overall replication"""
        # Setup large dataset that requires multiple workers
        schema = TableSchemas.basic_user_table(TEST_TABLE_NAME)
        self.mysql.execute(schema.sql)

        # Insert many records to distribute across workers
        for i in range(50):
            self.insert_basic_record(TEST_TABLE_NAME, f"User_{i:03d}", 20 + (i % 50))

        # Start parallel replication
        runner = RunAllRunner(
            cfg_file="tests/configs/replicator/tests_config_parallel.yaml"
        )
        runner.run()

        # Wait for replication to start and set ClickHouse database context
        self.wait_for_condition(lambda: TEST_DB_NAME in self.ch.get_databases())
        self.ch.database = TEST_DB_NAME

        # Wait for initial replication
        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=50)

        # Continue adding data while replication is running
        for i in range(50, 75):
            self.insert_basic_record(TEST_TABLE_NAME, f"User_{i:03d}", 20 + (i % 50))

        # Verify all data eventually gets replicated
        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=75)

        # Verify specific records from different ranges
        self.verify_record_exists(TEST_TABLE_NAME, "name='User_010'", {"age": 30})
        self.verify_record_exists(TEST_TABLE_NAME, "name='User_060'", {"age": 30})

        runner.stop()

    @pytest.mark.integration
    def test_multiple_databases_parallel(self):
        """Test parallel processing across multiple databases"""
        # Create second database
        test_db_2 = "test_db_parallel_2"
        mysql_drop_database(self.mysql, test_db_2)
        mysql_create_database(self.mysql, test_db_2)

        try:
            # Setup tables in both databases
            self.mysql.set_database(TEST_DB_NAME)
            schema1 = TableSchemas.basic_user_table(TEST_TABLE_NAME)
            self.mysql.execute(schema1.sql)
            self.insert_multiple_records(
                TEST_TABLE_NAME, TestDataGenerator.basic_users()[:3]
            )

            self.mysql.set_database(test_db_2)
            schema2 = TableSchemas.basic_user_table("users_db2")
            self.mysql.execute(schema2.sql)
            self.insert_multiple_records(
                "users_db2", TestDataGenerator.basic_users()[3:]
            )

            # Start parallel replication for both databases
            runner = RunAllRunner(
                cfg_file="tests/configs/replicator/tests_config_parallel.yaml"
            )
            runner.run()

            # Wait for replication to start and set ClickHouse context
            self.wait_for_condition(lambda: TEST_DB_NAME in self.ch.get_databases())
            self.ch.execute_command(f"USE `{TEST_DB_NAME}`")

            # Verify both databases are replicated
            self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=3)

            # Switch to second database and verify (wait for it to be created first)
            self.wait_for_condition(lambda: test_db_2 in self.ch.get_databases())
            self.ch.database = test_db_2
            self.wait_for_table_sync("users_db2", expected_count=2)

            runner.stop()

        finally:
            # Cleanup
            mysql_drop_database(self.mysql, test_db_2)
            self.ch.drop_database(test_db_2)

    @pytest.mark.integration
    def test_parallel_with_spatial_data(self):
        """Test parallel processing with complex spatial data types"""
        # Setup spatial table
        schema = TableSchemas.spatial_table(TEST_TABLE_NAME)
        self.mysql.execute(schema.sql)

        # Insert spatial data
        spatial_data = TestDataGenerator.spatial_records()
        for record in spatial_data:
            self.mysql.execute(
                f"""INSERT INTO `{TEST_TABLE_NAME}` (name, age, coordinate) 
                   VALUES ('{record["name"]}', {record["age"]}, {record["coordinate"]});""",
                commit=True,
            )

        # Add more spatial records for parallel processing
        for i in range(10):
            self.mysql.execute(
                f"""INSERT INTO `{TEST_TABLE_NAME}` (name, age, coordinate) 
                   VALUES ('SpatialUser_{i}', {25 + i}, POINT({10.0 + i}, {20.0 + i}));""",
                commit=True,
            )

        # Start parallel replication
        runner = RunAllRunner(
            cfg_file="tests/configs/replicator/tests_config_parallel.yaml"
        )
        runner.run()

        # Wait for replication to start and set ClickHouse database context
        self.wait_for_condition(lambda: TEST_DB_NAME in self.ch.get_databases())
        self.ch.database = TEST_DB_NAME

        # Verify spatial data replication
        expected_count = len(spatial_data) + 10
        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=expected_count)

        # Verify specific spatial records
        self.verify_record_exists(TEST_TABLE_NAME, "name='Ivan'", {"age": 42})
        self.verify_record_exists(TEST_TABLE_NAME, "name='SpatialUser_5'", {"age": 30})

        runner.stop()

    @pytest.mark.integration
    def test_parallel_with_reserved_keywords(self):
        """Test parallel processing with reserved keyword table names"""
        # Create table with reserved keyword name
        schema = TableSchemas.reserved_keyword_table("group")
        self.mysql.execute(schema.sql)

        # Insert test data
        reserved_data = TestDataGenerator.reserved_keyword_records()
        self.insert_multiple_records("group", reserved_data)

        # Start parallel replication
        runner = RunAllRunner(
            cfg_file="tests/configs/replicator/tests_config_parallel.yaml"
        )
        runner.run()

        # Wait for replication to start and set ClickHouse database context
        self.wait_for_condition(lambda: TEST_DB_NAME in self.ch.get_databases())
        self.ch.database = TEST_DB_NAME

        # Verify reserved keyword table is handled correctly
        self.wait_for_table_sync("group", expected_count=len(reserved_data))

        # Verify specific records
        self.verify_record_exists("group", "name='Peter'", {"age": 33})

        runner.stop()
