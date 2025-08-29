"""Tests for column management DDL operations (ADD/DROP/ALTER column)"""

import pytest

from tests.base import IsolatedBaseReplicationTest, DataTestMixin, SchemaTestMixin
from tests.conftest import TEST_TABLE_NAME


class TestColumnManagement(IsolatedBaseReplicationTest, SchemaTestMixin, DataTestMixin):
    """Test column management DDL operations during replication"""

    @pytest.mark.integration
    def test_add_column_first_after_and_drop_column(self):
        """Test ADD COLUMN FIRST/AFTER and DROP COLUMN operations"""
        # Create initial table
        self.mysql.execute(f"""
        CREATE TABLE `{TEST_TABLE_NAME}` (
            id int NOT NULL AUTO_INCREMENT,
            name varchar(255),
            age int,
            PRIMARY KEY (id)
        );
        """)

        # Insert initial data
        self.insert_multiple_records(
            TEST_TABLE_NAME,
            [
                {"name": "John", "age": 30},
                {"name": "Jane", "age": 25},
            ]
        )

        # Start replication
        self.start_replication()
        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=2)

        # Test ADD COLUMN FIRST
        self.mysql.execute(
            f"ALTER TABLE `{TEST_TABLE_NAME}` ADD COLUMN priority int DEFAULT 1 FIRST;",
            commit=True,
        )

        # Test ADD COLUMN AFTER
        self.mysql.execute(
            f"ALTER TABLE `{TEST_TABLE_NAME}` ADD COLUMN email varchar(255) AFTER name;",
            commit=True,
        )

        # Test ADD COLUMN at end (no position specified)
        self.mysql.execute(
            f"ALTER TABLE `{TEST_TABLE_NAME}` ADD COLUMN status varchar(50) DEFAULT 'active';",
            commit=True,
        )

        # Wait for DDL to replicate
        self.wait_for_ddl_replication()

        # Insert new data to test new columns
        self.mysql.execute(
            f"INSERT INTO `{TEST_TABLE_NAME}` (priority, name, email, age, status) VALUES (2, 'Bob', 'bob@example.com', 35, 'inactive');",
            commit=True,
        )

        # Update existing records with new columns
        self.mysql.execute(
            f"UPDATE `{TEST_TABLE_NAME}` SET email = 'john@example.com', priority = 3 WHERE name = 'John';",
            commit=True,
        )

        # Verify new data structure
        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=3)
        self.verify_record_exists(
            TEST_TABLE_NAME,
            "name='Bob'",
            {"priority": 2, "email": "bob@example.com", "status": "inactive"}
        )
        self.verify_record_exists(
            TEST_TABLE_NAME,
            "name='John'",
            {"priority": 3, "email": "john@example.com"}
        )

        # Test DROP COLUMN
        self.mysql.execute(
            f"ALTER TABLE `{TEST_TABLE_NAME}` DROP COLUMN priority;",
            commit=True,
        )

        # Wait for DROP to replicate
        self.wait_for_ddl_replication()

        # Insert data without the dropped column
        self.mysql.execute(
            f"INSERT INTO `{TEST_TABLE_NAME}` (name, email, age, status) VALUES ('Alice', 'alice@example.com', 28, 'active');",
            commit=True,
        )

        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=4)
        self.verify_record_exists(
            TEST_TABLE_NAME,
            "name='Alice'",
            {"email": "alice@example.com", "age": 28}
        )