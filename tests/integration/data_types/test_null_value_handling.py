"""Tests for NULL value handling across data types"""

import pytest

from tests.base import IsolatedBaseReplicationTest, DataTestMixin, SchemaTestMixin
from tests.conftest import TEST_TABLE_NAME


class TestNullValueHandling(IsolatedBaseReplicationTest, SchemaTestMixin, DataTestMixin):
    """Test replication of NULL values across different data types"""

    @pytest.mark.integration
    def test_null_value_handling(self):
        """Test NULL value handling across different data types"""
        # Create table with nullable columns of different types
        self.mysql.execute(f"""
        CREATE TABLE `{TEST_TABLE_NAME}` (
            id int NOT NULL AUTO_INCREMENT,
            name varchar(255),
            nullable_int int NULL,
            nullable_decimal decimal(10,2) NULL,
            nullable_text text NULL,
            nullable_datetime datetime NULL,
            nullable_bool boolean NULL,
            PRIMARY KEY (id)
        );
        """)

        # Insert NULL test data
        null_data = [
            {
                "name": "All NULL Values",
                "nullable_int": None,
                "nullable_decimal": None,
                "nullable_text": None,
                "nullable_datetime": None,
                "nullable_bool": None
            },
            {
                "name": "Some NULL Values",
                "nullable_int": 42,
                "nullable_decimal": None,
                "nullable_text": "Not null text",
                "nullable_datetime": None,
                "nullable_bool": True
            },
            {
                "name": "No NULL Values",
                "nullable_int": 100,
                "nullable_decimal": 123.45,
                "nullable_text": "All fields have values",
                "nullable_datetime": "2023-01-01 12:00:00",
                "nullable_bool": False
            }
        ]

        self.insert_multiple_records(TEST_TABLE_NAME, null_data)

        # Start replication
        self.start_replication()
        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=3)

        # Verify all NULL values
        self.verify_record_exists(
            TEST_TABLE_NAME, 
            "name='All NULL Values' AND nullable_int IS NULL AND nullable_decimal IS NULL"
        )

        # Verify mixed NULL/non-NULL values
        self.verify_record_exists(
            TEST_TABLE_NAME,
            "name='Some NULL Values' AND nullable_int IS NOT NULL AND nullable_decimal IS NULL",
            {"nullable_int": 42}
        )

        # Verify no NULL values
        self.verify_record_exists(
            TEST_TABLE_NAME,
            "name='No NULL Values' AND nullable_int IS NOT NULL",
            {"nullable_int": 100, "nullable_bool": 0}  # False = 0 in ClickHouse
        )

        # Verify NULL handling for different data types
        self.verify_record_exists(
            TEST_TABLE_NAME, "name='All NULL Values' AND nullable_text IS NULL"
        )

        self.verify_record_exists(
            TEST_TABLE_NAME, "name='All NULL Values' AND nullable_datetime IS NULL"
        )

        self.verify_record_exists(
            TEST_TABLE_NAME, "name='All NULL Values' AND nullable_bool IS NULL"
        )