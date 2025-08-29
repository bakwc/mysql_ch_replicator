"""Comprehensive numeric data types testing including boundary limits and unsigned values"""

from decimal import Decimal

import pytest

from tests.base import BaseReplicationTest, DataTestMixin, SchemaTestMixin
from tests.conftest import TEST_TABLE_NAME


class TestNumericComprehensive(BaseReplicationTest, SchemaTestMixin, DataTestMixin):
    """Test comprehensive numeric types including boundaries and unsigned limits"""

    @pytest.mark.integration
    def test_decimal_and_numeric_types(self):
        """Test decimal and numeric type handling from basic data types"""
        # Create table with decimal and numeric types
        self.mysql.execute(f"""
        CREATE TABLE `{TEST_TABLE_NAME}` (
            id int NOT NULL AUTO_INCREMENT,
            name varchar(255) NOT NULL,
            salary decimal(10,2),
            rate decimal(5,4),
            percentage decimal(3,2),
            score float,
            weight double,
            precision_val numeric(15,5),
            PRIMARY KEY (id)
        );
        """)

        # Insert test data with various decimal and numeric values
        test_data = [
            {
                "name": "John Doe",
                "salary": Decimal("50000.50"),
                "rate": Decimal("9.5000"), 
                "percentage": Decimal("8.75"),
                "score": 87.5,
                "weight": 155.75,
                "precision_val": Decimal("1234567890.12345")
            },
            {
                "name": "Jane Smith",
                "salary": Decimal("75000.00"),
                "rate": Decimal("8.2500"),
                "percentage": Decimal("9.50"),
                "score": 92.0,
                "weight": 140.25,
                "precision_val": Decimal("9876543210.54321")
            },
            {
                "name": "Zero Values",
                "salary": Decimal("0.00"),
                "rate": Decimal("0.0000"),
                "percentage": Decimal("0.00"),
                "score": 0.0,
                "weight": 0.0,
                "precision_val": Decimal("0.00000")
            },
        ]

        self.insert_multiple_records(TEST_TABLE_NAME, test_data)

        # Start replication
        self.start_replication()
        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=3)

        # Verify decimal values
        self.verify_record_exists(
            TEST_TABLE_NAME, "name='John Doe'", 
            {"salary": Decimal("50000.50"), "rate": Decimal("9.5000")}
        )

        # Verify zero values
        self.verify_record_exists(
            TEST_TABLE_NAME, "name='Zero Values'", 
            {"salary": Decimal("0.00")}
        )

    @pytest.mark.integration 
    def test_numeric_boundary_limits(self):
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
            decimal_max decimal(65,2),
            decimal_high_precision decimal(10,8),
            float_val float,
            double_val double,
            PRIMARY KEY (id)
        );
        """)

        # Insert boundary values
        boundary_data = [
            {
                "name": "Maximum Values",
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
                "decimal_max": Decimal("999999999999999999999999999999999999999999999999999999999999999.99"),
                "decimal_high_precision": Decimal("99.99999999"),
                "float_val": 3.402823466e+38,
                "double_val": 1.7976931348623157e+308
            },
            {
                "name": "Minimum Values",
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
                "decimal_max": Decimal("-999999999999999999999999999999999999999999999999999999999999999.99"),
                "decimal_high_precision": Decimal("-99.99999999"),
                "float_val": -3.402823466e+38,
                "double_val": -1.7976931348623157e+308
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
                "decimal_max": Decimal("0.00"),
                "decimal_high_precision": Decimal("0.00000000"),
                "float_val": 0.0,
                "double_val": 0.0
            }
        ]

        self.insert_multiple_records(TEST_TABLE_NAME, boundary_data)

        # Start replication
        self.start_replication()
        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=3)

        # Verify maximum values
        self.verify_record_exists(
            TEST_TABLE_NAME, "name='Maximum Values'", 
            {"tiny_signed": 127, "tiny_unsigned": 255}
        )

        # Verify minimum values  
        self.verify_record_exists(
            TEST_TABLE_NAME, "name='Minimum Values'", 
            {"tiny_signed": -128, "small_signed": -32768}
        )

        # Verify zero values
        self.verify_record_exists(
            TEST_TABLE_NAME, "name='Zero Values'", 
            {"int_signed": 0, "big_unsigned": 0}
        )

    @pytest.mark.integration
    def test_precision_and_scale_decimals(self):
        """Test decimal precision and scale variations"""
        # Create table with different decimal precisions
        self.mysql.execute(f"""
        CREATE TABLE `{TEST_TABLE_NAME}` (
            id int NOT NULL AUTO_INCREMENT,
            name varchar(255),
            dec_small decimal(3,1),
            dec_medium decimal(10,4),
            dec_large decimal(20,8),
            dec_max_precision decimal(65,30),
            PRIMARY KEY (id)
        );
        """)

        # Insert precision test data
        precision_data = [
            {
                "name": "Small Precision",
                "dec_small": Decimal("99.9"),
                "dec_medium": Decimal("999999.9999"), 
                "dec_large": Decimal("123456789012.12345678"),
                "dec_max_precision": Decimal("12345678901234567890123456789012345.123456789012345678901234567890")
            },
            {
                "name": "Edge Cases",
                "dec_small": Decimal("0.1"),
                "dec_medium": Decimal("0.0001"),
                "dec_large": Decimal("0.00000001"),
                "dec_max_precision": Decimal("0.000000000000000000000000000001")
            }
        ]

        self.insert_multiple_records(TEST_TABLE_NAME, precision_data)

        # Start replication
        self.start_replication()
        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=2)

        # Verify precision handling
        self.verify_record_exists(
            TEST_TABLE_NAME, "name='Small Precision'", 
            {"dec_small": Decimal("99.9")}
        )

        self.verify_record_exists(
            TEST_TABLE_NAME, "name='Edge Cases'", 
            {"dec_medium": Decimal("0.0001")}
        )

    @pytest.mark.integration
    def test_unsigned_extremes(self):
        """Test unsigned numeric extreme values"""
        # Create table with unsigned numeric types
        self.mysql.execute(f"""
        CREATE TABLE `{TEST_TABLE_NAME}` (
            `id` int unsigned NOT NULL AUTO_INCREMENT,
            name varchar(255),
            test1 smallint,
            test2 smallint unsigned,
            test3 TINYINT,
            test4 TINYINT UNSIGNED,
            test5 MEDIUMINT UNSIGNED,
            test6 INT UNSIGNED,
            test7 BIGINT UNSIGNED,
            test8 MEDIUMINT UNSIGNED NULL,
            PRIMARY KEY (id)
        );
        """)

        # Insert unsigned extreme values
        extreme_data = [
            {
                "name": "Unsigned Maximum",
                "test1": 32767,
                "test2": 65535,  # Max unsigned smallint
                "test3": 127,
                "test4": 255,    # Max unsigned tinyint
                "test5": 16777215,  # Max unsigned mediumint
                "test6": 4294967295,  # Max unsigned int  
                "test7": 18446744073709551615,  # Max unsigned bigint
                "test8": 16777215
            },
            {
                "name": "Unsigned Minimum",
                "test1": -32768,
                "test2": 0,      # Min unsigned (all unsigned mins are 0)
                "test3": -128,
                "test4": 0,
                "test5": 0,
                "test6": 0,
                "test7": 0,
                "test8": None    # NULL test
            }
        ]

        self.insert_multiple_records(TEST_TABLE_NAME, extreme_data)

        # Start replication
        self.start_replication()
        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=2)

        # Verify unsigned maximum values
        self.verify_record_exists(
            TEST_TABLE_NAME, "name='Unsigned Maximum'", 
            {"test2": 65535, "test4": 255}
        )

        # Verify unsigned minimum values and NULL handling
        self.verify_record_exists(
            TEST_TABLE_NAME, "name='Unsigned Minimum'", 
            {"test2": 0, "test4": 0}
        )

        # Verify NULL handling
        self.verify_record_exists(
            TEST_TABLE_NAME, "name='Unsigned Minimum' AND test8 IS NULL"
        )