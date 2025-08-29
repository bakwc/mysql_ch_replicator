"""Tests for datetime and date type replication"""

import datetime

import pytest

from tests.base import BaseReplicationTest, DataTestMixin, SchemaTestMixin
from tests.conftest import TEST_TABLE_NAME
from tests.fixtures import TableSchemas, TestDataGenerator


class TestDatetimeTypes(BaseReplicationTest, SchemaTestMixin, DataTestMixin):
    """Test replication of datetime and date types"""

    @pytest.mark.integration
    def test_datetime_and_date_types(self):
        """Test datetime and date type handling"""
        # Setup datetime table
        schema = TableSchemas.datetime_test_table(TEST_TABLE_NAME)
        self.mysql.execute(schema.sql)

        # Insert datetime test data
        datetime_data = TestDataGenerator.datetime_records()
        self.insert_multiple_records(TEST_TABLE_NAME, datetime_data)

        # Start replication
        self.start_replication()

        # Verify datetime replication
        expected_count = len(datetime_data)
        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=expected_count)

        # Verify specific datetime values
        self.verify_record_exists(
            TEST_TABLE_NAME, "name='Ivan'", {"test_date": datetime.date(2015, 5, 28)}
        )

        # Verify NULL datetime handling
        self.verify_record_exists(
            TEST_TABLE_NAME, "name='Ivan' AND modified_date IS NULL"
        )

        # Verify non-NULL datetime (ClickHouse returns timezone-aware datetime)
        from datetime import timezone
        expected_datetime = datetime.datetime(2023, 1, 8, 3, 11, 9, tzinfo=timezone.utc)
        self.verify_record_exists(
            TEST_TABLE_NAME,
            "name='Givi'",
            {"modified_date": expected_datetime},
        )