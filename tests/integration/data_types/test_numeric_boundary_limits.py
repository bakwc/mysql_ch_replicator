"""Numeric boundary limits and edge case testing"""

from decimal import Decimal

import pytest

from tests.base import BaseReplicationTest, DataTestMixin, SchemaTestMixin
from tests.conftest import TEST_TABLE_NAME


class TestNumericBoundaryLimits(BaseReplicationTest, SchemaTestMixin, DataTestMixin):
    """Test numeric types and their boundary limits"""

    @pytest.mark.integration 
    def test_numeric_types_and_limits(self):
        """Test numeric types and their boundary limits"""
        # Create table with various numeric types and limits
        self.mysql.execute(f"""
        CREATE TABLE `{TEST_TABLE_NAME}` (
            id int NOT NULL AUTO_INCREMENT,
            name varchar(255),
            tiny_signed tinyint,
            tiny_unsigned tinyint unsigned,
            small_signed smallint,
            small_unsigned smallint unsigned,
            medium_signed mediumint,
            medium_unsigned mediumint unsigned,
            int_signed int,
            int_unsigned int unsigned,
            big_signed bigint,
            big_unsigned bigint unsigned,
            decimal_val decimal(10,2),
            float_val float,
            double_val double,
            PRIMARY KEY (id)
        );
        """)

        # Test boundary values for each numeric type
        boundary_data = [
            {
                "name": "Min Values",
                "tiny_signed": -128,
                "tiny_unsigned": 0,
                "small_signed": -32768,
                "small_unsigned": 0,
                "medium_signed": -8388608,
                "medium_unsigned": 0,
                "int_signed": -2147483648,
                "int_unsigned": 0,
                "big_signed": -9223372036854775808,
                "big_unsigned": 0,
                "decimal_val": Decimal("-99999999.99"),
                "float_val": -3.4028235e+38,
                "double_val": -1.7976931348623157e+308,
            },
            {
                "name": "Max Values",
                "tiny_signed": 127,
                "tiny_unsigned": 255,
                "small_signed": 32767,
                "small_unsigned": 65535,
                "medium_signed": 8388607,
                "medium_unsigned": 16777215,
                "int_signed": 2147483647,
                "int_unsigned": 4294967295,
                "big_signed": 9223372036854775807,
                "big_unsigned": 18446744073709551615,
                "decimal_val": Decimal("99999999.99"),
                "float_val": 3.4028235e+38,
                "double_val": 1.7976931348623157e+308,
            },
            {
                "name": "Zero Values",
                "tiny_signed": 0,
                "tiny_unsigned": 0,
                "small_signed": 0,
                "small_unsigned": 0,
                "medium_signed": 0,
                "medium_unsigned": 0,
                "int_signed": 0,
                "int_unsigned": 0,
                "big_signed": 0,
                "big_unsigned": 0,
                "decimal_val": Decimal("0.00"),
                "float_val": 0.0,
                "double_val": 0.0,
            },
        ]

        # Insert boundary test data
        self.insert_multiple_records(TEST_TABLE_NAME, boundary_data)

        # Start replication
        self.start_replication()
        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=3)

        # Verify boundary values are replicated correctly
        self.verify_record_exists(
            TEST_TABLE_NAME,
            "name='Min Values'",
            {"tiny_signed": -128, "big_signed": -9223372036854775808},
        )

        self.verify_record_exists(
            TEST_TABLE_NAME,
            "name='Max Values'",
            {"tiny_unsigned": 255, "big_unsigned": 18446744073709551615},
        )

        self.verify_record_exists(
            TEST_TABLE_NAME, "name='Zero Values'", {"int_signed": 0, "double_val": 0.0}
        )

    @pytest.mark.integration
    def test_precision_and_scale_decimals(self):
        """Test decimal precision and scale variations"""
        # Create table with different decimal precisions
        self.mysql.execute(f"""
        CREATE TABLE `{TEST_TABLE_NAME}` (
            id int NOT NULL AUTO_INCREMENT,
            name varchar(255),
            small_decimal decimal(5,2),
            medium_decimal decimal(10,4),
            large_decimal decimal(20,8),
            no_scale decimal(10,0),
            PRIMARY KEY (id)
        );
        """)

        # Test various decimal precisions and scales
        decimal_data = [
            {
                "name": "Small Precision",
                "small_decimal": Decimal("999.99"),
                "medium_decimal": Decimal("123456.7890"),
                "large_decimal": Decimal("123456789012.12345678"),
                "no_scale": Decimal("1234567890"),
            },
            {
                "name": "Edge Cases",
                "small_decimal": Decimal("0.01"),
                "medium_decimal": Decimal("0.0001"),
                "large_decimal": Decimal("0.00000001"),
                "no_scale": Decimal("1"),
            },
            {
                "name": "Negative Values",
                "small_decimal": Decimal("-999.99"),
                "medium_decimal": Decimal("-123456.7890"),
                "large_decimal": Decimal("-123456789012.12345678"),
                "no_scale": Decimal("-1234567890"),
            },
        ]

        self.insert_multiple_records(TEST_TABLE_NAME, decimal_data)

        # Start replication
        self.start_replication()
        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=3)

        # Verify decimal precision preservation
        self.verify_record_exists(
            TEST_TABLE_NAME,
            "name='Small Precision'",
            {"small_decimal": Decimal("999.99"), "no_scale": Decimal("1234567890")},
        )

        self.verify_record_exists(
            TEST_TABLE_NAME,
            "name='Edge Cases'",
            {"small_decimal": Decimal("0.01"), "large_decimal": Decimal("0.00000001")},
        )

        self.verify_record_exists(
            TEST_TABLE_NAME,
            "name='Negative Values'",
            {"medium_decimal": Decimal("-123456.7890")},
        )