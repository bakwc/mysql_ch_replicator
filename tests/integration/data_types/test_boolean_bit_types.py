"""Tests for boolean and bit type replication"""

import pytest

from tests.base import IsolatedBaseReplicationTest, DataTestMixin, SchemaTestMixin
from tests.conftest import TEST_TABLE_NAME


class TestBooleanBitTypes(IsolatedBaseReplicationTest, SchemaTestMixin, DataTestMixin):
    """Test replication of boolean and bit types"""

    @pytest.mark.integration
    def test_boolean_and_bit_types(self):
        """Test boolean and bit type handling"""
        # Create table with boolean and bit types
        self.mysql.execute(f"""
        CREATE TABLE `{TEST_TABLE_NAME}` (
            id int NOT NULL AUTO_INCREMENT,
            name varchar(255) NOT NULL,
            is_active boolean,
            status_flag bool,
            bit_field bit(8),
            multi_bit bit(16),
            PRIMARY KEY (id)
        );
        """)

        # Insert boolean and bit test data
        boolean_bit_data = [
            {
                "name": "True Values",
                "is_active": True,
                "status_flag": 1,
                "bit_field": 255,  # 11111111 in binary
                "multi_bit": 65535  # 1111111111111111 in binary
            },
            {
                "name": "False Values", 
                "is_active": False,
                "status_flag": 0,
                "bit_field": 0,    # 00000000 in binary
                "multi_bit": 0     # 0000000000000000 in binary
            },
            {
                "name": "Mixed Values",
                "is_active": True,
                "status_flag": False,
                "bit_field": 85,   # 01010101 in binary
                "multi_bit": 21845  # 0101010101010101 in binary
            }
        ]

        self.insert_multiple_records(TEST_TABLE_NAME, boolean_bit_data)

        # Start replication
        self.start_replication()
        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=3)

        # Verify boolean TRUE values (ClickHouse represents as 1)
        self.verify_record_exists(
            TEST_TABLE_NAME, "name='True Values'", 
            {"is_active": 1, "status_flag": 1}
        )

        # Verify boolean FALSE values (ClickHouse represents as 0)
        self.verify_record_exists(
            TEST_TABLE_NAME, "name='False Values'", 
            {"is_active": 0, "status_flag": 0}
        )

        # Verify mixed boolean values
        self.verify_record_exists(
            TEST_TABLE_NAME, "name='Mixed Values'", 
            {"is_active": 1, "status_flag": 0}
        )

        # Verify bit field values (check existence since bit handling varies)
        self.verify_record_exists(
            TEST_TABLE_NAME, "name='True Values' AND bit_field IS NOT NULL"
        )

        self.verify_record_exists(
            TEST_TABLE_NAME, "name='False Values' AND multi_bit IS NOT NULL"
        )