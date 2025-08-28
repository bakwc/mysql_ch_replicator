"""Integration test for multi-op ALTER statements (ADD/DROP in one statement)"""

import pytest

from tests.base import BaseReplicationTest, DataTestMixin, SchemaTestMixin
from tests.conftest import TEST_DB_NAME, TEST_TABLE_NAME


class TestMultiAlterStatements(BaseReplicationTest, SchemaTestMixin, DataTestMixin):
    """Validate parser and replication for multi-op ALTER statements."""

    @pytest.mark.integration
    def test_multi_add_and_multi_drop(self):
        # Base table
        self.mysql.execute(
            f"""
            CREATE TABLE `{TEST_TABLE_NAME}` (
                id INT NOT NULL AUTO_INCREMENT,
                name VARCHAR(255),
                age INT,
                PRIMARY KEY (id)
            );
            """
        )

        # Seed
        self.insert_multiple_records(
            TEST_TABLE_NAME,
            [
                {"name": "Ivan", "age": 42},
            ],
        )

        # Start replication
        self.start_replication(db_name=TEST_DB_NAME)
        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=1)

        # Multi-ADD in a single statement
        self.mysql.execute(
            f"""
            ALTER TABLE `{TEST_TABLE_NAME}`
              ADD `last_name` VARCHAR(255),
              ADD COLUMN city VARCHAR(255);
            """
        )

        # Insert row with new columns present
        self.insert_multiple_records(
            TEST_TABLE_NAME,
            [
                {"name": "Mary", "age": 24, "last_name": "Smith", "city": "London"},
            ],
        )

        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=2)
        self.verify_record_exists(
            TEST_TABLE_NAME, "name='Mary'", {"last_name": "Smith", "city": "London"}
        )

        # Multi-DROP in a single statement
        self.mysql.execute(
            f"""
            ALTER TABLE `{TEST_TABLE_NAME}`
              DROP COLUMN last_name,
              DROP COLUMN city;
            """
        )

        # Insert another row to verify table still functional after multi-drop
        self.insert_multiple_records(
            TEST_TABLE_NAME,
            [
                {"name": "John", "age": 30},
            ],
        )

        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=3)

        # Confirm columns were dropped (selecting them should be impossible)
        # Just verify the last inserted record exists by name/age
        self.verify_record_exists(TEST_TABLE_NAME, "name='John'", {"age": 30})
