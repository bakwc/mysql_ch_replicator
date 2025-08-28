"""Integration test for JSON with non-Latin (e.g., Cyrillic) keys"""

import json

import pytest

from tests.base import BaseReplicationTest, DataTestMixin, SchemaTestMixin
from tests.conftest import TEST_DB_NAME, TEST_TABLE_NAME


class TestJsonUnicodeKeys(BaseReplicationTest, SchemaTestMixin, DataTestMixin):
    """Verify JSON with non-Latin keys replicates and parses correctly."""

    @pytest.mark.integration
    def test_json_unicode(self):
        # Table with JSON column
        self.mysql.execute(
            f"""
            CREATE TABLE `{TEST_TABLE_NAME}` (
                `id` int unsigned NOT NULL AUTO_INCREMENT,
                name varchar(255),
                data json,
                PRIMARY KEY (id)
            );
            """
        )

        # Insert JSON rows with Cyrillic keys
        self.mysql.execute(
            f"""
            INSERT INTO `{TEST_TABLE_NAME}` (name, data) VALUES
            ('Ivan', '{{"а": "б", "в": [1,2,3]}}');
            """,
            commit=True,
        )

        # Start replication
        self.start_replication(db_name=TEST_DB_NAME)
        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=1)

        # Second row with different ordering/values
        self.mysql.execute(
            f"""
            INSERT INTO `{TEST_TABLE_NAME}` (name, data) VALUES
            ('Peter', '{{"в": "б", "а": [3,2,1]}}');
            """,
            commit=True,
        )
        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=2)

        # Validate by decoding JSON returned from ClickHouse
        ivan = self.ch.select(TEST_TABLE_NAME, "name='Ivan'")[0]
        peter = self.ch.select(TEST_TABLE_NAME, "name='Peter'")[0]
        ivan_json = json.loads(ivan["data"])
        peter_json = json.loads(peter["data"])

        assert ivan_json["в"] == [1, 2, 3]
        assert peter_json["в"] == "б"
