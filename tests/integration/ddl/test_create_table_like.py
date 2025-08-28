"""Integration test for CREATE TABLE ... LIKE replication"""

import pytest

from tests.base import BaseReplicationTest, DataTestMixin, SchemaTestMixin
from tests.conftest import TEST_DB_NAME


class TestCreateTableLike(BaseReplicationTest, SchemaTestMixin, DataTestMixin):
    """Verify CREATE TABLE ... LIKE is replicated and usable."""

    @pytest.mark.integration
    def test_create_table_like_replication(self):
        # Create a source table with a handful of types and constraints
        self.mysql.execute(
            """
            CREATE TABLE `source_table` (
                id INT NOT NULL AUTO_INCREMENT,
                name VARCHAR(255) NOT NULL,
                age INT UNSIGNED,
                email VARCHAR(100) UNIQUE,
                status ENUM('active','inactive','pending') DEFAULT 'active',
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                data JSON,
                PRIMARY KEY (id)
            );
            """
        )

        # Seed some data
        self.insert_multiple_records(
            "source_table",
            [
                {
                    "name": "Alice",
                    "age": 30,
                    "email": "alice@example.com",
                    "status": "active",
                    "data": '{"tags":["a","b"]}',
                }
            ],
        )

        # Create a new table using LIKE
        self.mysql.execute("""
            CREATE TABLE `derived_table` LIKE `source_table`;
        """)

        # Start replication
        self.start_replication(db_name=TEST_DB_NAME)

        # Wait for both tables to exist in CH
        self.wait_for_table_sync("source_table", expected_count=1)
        self.wait_for_table_sync("derived_table", expected_count=0)

        # Insert data into both tables to verify end-to-end
        self.insert_multiple_records(
            "source_table",
            [
                {
                    "name": "Carol",
                    "age": 28,
                    "email": "carol@example.com",
                    "status": "pending",
                    "data": '{"score":10}',
                }
            ],
        )
        self.insert_multiple_records(
            "derived_table",
            [
                {
                    "name": "Bob",
                    "age": 25,
                    "email": "bob@example.com",
                    "status": "inactive",
                    "data": '{"ok":true}',
                }
            ],
        )

        # Verify data in CH
        self.wait_for_table_sync("source_table", expected_count=2)
        self.wait_for_table_sync("derived_table", expected_count=1)
        self.verify_record_exists("source_table", "name='Alice'", {"age": 30})
        self.verify_record_exists("derived_table", "name='Bob'", {"age": 25})
