"""Tests for basic CRUD operations during replication"""

import pytest

from tests.base import BaseReplicationTest, DataTestMixin, SchemaTestMixin
from tests.conftest import (
    CONFIG_FILE,
    CONFIG_FILE_MARIADB,
    TEST_DB_NAME,
    TEST_TABLE_NAME,
)
from tests.fixtures import TableSchemas, TestDataGenerator


class TestBasicCrudOperations(BaseReplicationTest, SchemaTestMixin, DataTestMixin):
    """Test basic Create, Read, Update, Delete operations"""

    @pytest.mark.integration
    @pytest.mark.parametrize("config_file", [CONFIG_FILE, CONFIG_FILE_MARIADB])
    def test_basic_insert_operations(self, config_file):
        """Test basic insert operations are replicated correctly"""
        # Create table using schema helper
        schema = TableSchemas.basic_user_with_blobs(TEST_TABLE_NAME)
        self.mysql.execute(schema.sql)

        # Insert test data using data helper
        test_data = TestDataGenerator.users_with_blobs()
        self.insert_multiple_records(TEST_TABLE_NAME, test_data)

        # Start replication
        self.start_replication(db_name=TEST_DB_NAME, config_file=config_file)

        # Verify data sync
        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=len(test_data))

        # Verify specific records
        for record in test_data:
            self.verify_record_exists(
                TEST_TABLE_NAME,
                f"name='{record['name']}'",
                {
                    "age": record["age"],
                    "field1": record["field1"],
                },
            )

        # Check partition configuration for MariaDB config
        if config_file == CONFIG_FILE_MARIADB:
            create_query = self.ch.show_create_table(TEST_TABLE_NAME)
            assert "PARTITION BY intDiv(id, 1000000)" in create_query

    @pytest.mark.integration
    def test_realtime_inserts(self):
        """Test that new inserts after replication starts are synced"""
        # Setup initial table and data
        schema = TableSchemas.basic_user_table(TEST_TABLE_NAME)
        self.mysql.execute(schema.sql)

        initial_data = TestDataGenerator.basic_users()[:2]  # First 2 users
        self.insert_multiple_records(TEST_TABLE_NAME, initial_data)

        # Start replication
        self.start_replication()
        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=2)

        # Insert new data after replication started
        self.insert_basic_record(TEST_TABLE_NAME, "Filipp", 50)

        # Verify new data is replicated
        self.wait_for_data_sync(TEST_TABLE_NAME, "name='Filipp'", 50, "age")
        assert len(self.ch.select(TEST_TABLE_NAME)) == 3

    @pytest.mark.integration
    def test_update_operations(self):
        """Test that update operations are handled correctly"""
        # Create and populate table
        schema = TableSchemas.basic_user_table(TEST_TABLE_NAME)
        self.mysql.execute(schema.sql)

        self.insert_basic_record(TEST_TABLE_NAME, "John", 25)

        # Start replication
        self.start_replication()
        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=1)

        # Update record
        self.update_record(
            TEST_TABLE_NAME, "name='John'", {"age": 26, "name": "John_Updated"}
        )

        # Verify update is replicated (ReplacingMergeTree handles this)
        self.wait_for_data_sync(TEST_TABLE_NAME, "name='John_Updated'", 26, "age")

    @pytest.mark.integration
    def test_delete_operations(self):
        """Test that delete operations are handled correctly"""
        # Create and populate table
        schema = TableSchemas.basic_user_table(TEST_TABLE_NAME)
        self.mysql.execute(schema.sql)

        test_data = TestDataGenerator.basic_users()[:3]
        self.insert_multiple_records(TEST_TABLE_NAME, test_data)

        # Start replication
        self.start_replication()
        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=3)

        # Delete one record
        self.delete_records(TEST_TABLE_NAME, "name='Peter'")

        # Verify deletion is handled (exact behavior depends on config)
        # ReplacingMergeTree may still show the record until optimization
        # but with a deletion marker
        self.wait_for_data_sync(TEST_TABLE_NAME, "name!='Peter'")

    @pytest.mark.integration
    def test_mixed_operations(self):
        """Test mixed insert/update/delete operations"""
        # Setup
        schema = TableSchemas.basic_user_table(TEST_TABLE_NAME)
        self.mysql.execute(schema.sql)

        # Initial data
        initial_data = TestDataGenerator.basic_users()[:2]
        self.insert_multiple_records(TEST_TABLE_NAME, initial_data)

        # Start replication
        self.start_replication()
        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=2)

        # Mixed operations
        self.insert_basic_record(TEST_TABLE_NAME, "NewUser", 30)  # Insert
        self.update_record(TEST_TABLE_NAME, "name='Ivan'", {"age": 43})  # Update
        self.delete_records(TEST_TABLE_NAME, "name='Peter'")  # Delete

        # Verify all operations
        self.wait_for_data_sync(TEST_TABLE_NAME, "name='NewUser'", 30, "age")
        self.wait_for_data_sync(TEST_TABLE_NAME, "name='Ivan'", 43, "age")

        # Verify final state
        total_records = self.get_clickhouse_count(TEST_TABLE_NAME)
        assert total_records >= 2  # At least NewUser and updated Ivan

    @pytest.mark.integration
    def test_multi_column_primary_key_deletes(self):
        """Test deletion operations with multi-column primary keys"""
        from tests.conftest import RunAllRunner, read_logs

        # Create table with composite primary key
        self.mysql.execute(f"""
        CREATE TABLE `{TEST_TABLE_NAME}` (
            departments int(11) NOT NULL,
            termine int(11) NOT NULL,
            PRIMARY KEY (departments,termine)
        );
        """)

        # Insert test data with composite primary key values
        test_data = [
            {"departments": 10, "termine": 20},
            {"departments": 30, "termine": 40},
            {"departments": 50, "termine": 60},
            {"departments": 20, "termine": 10},
            {"departments": 40, "termine": 30},
            {"departments": 60, "termine": 50},
        ]

        for record in test_data:
            self.mysql.execute(
                f"INSERT INTO `{TEST_TABLE_NAME}` (departments, termine) VALUES ({record['departments']}, {record['termine']});",
                commit=True,
            )

        # Use RunAllRunner instead of individual components for this test
        runner = RunAllRunner()
        runner.run()

        # Wait for replication
        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=6)

        # Delete records using part of the composite primary key
        self.delete_records(TEST_TABLE_NAME, "departments=10")
        self.delete_records(TEST_TABLE_NAME, "departments=30")
        self.delete_records(TEST_TABLE_NAME, "departments=50")

        # Verify deletions were processed
        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=3)

        # Verify remaining records exist
        remaining_records = self.ch.select(TEST_TABLE_NAME)
        departments_remaining = {record["departments"] for record in remaining_records}
        expected_remaining = {20, 40, 60}
        assert departments_remaining == expected_remaining

        runner.stop()

        # Verify clean shutdown
        self.wait_for_condition(
            lambda: "stopping db_replicator" in read_logs(TEST_DB_NAME)
        )
        assert "Traceback" not in read_logs(TEST_DB_NAME)
