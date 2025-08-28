"""Tests for handling basic MySQL data types during replication"""

import datetime
from decimal import Decimal

import pytest

from tests.base import BaseReplicationTest, DataTestMixin, SchemaTestMixin
from tests.conftest import TEST_TABLE_NAME
from tests.fixtures import TableSchemas, TestDataGenerator


class TestBasicDataTypes(BaseReplicationTest, SchemaTestMixin, DataTestMixin):
    """Test replication of basic MySQL data types"""

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

        # Verify non-NULL datetime
        self.verify_record_exists(
            TEST_TABLE_NAME,
            "name='Givi'",
            {"modified_date": datetime.datetime(2023, 1, 8, 3, 11, 9)},
        )

    @pytest.mark.integration
    def test_decimal_and_numeric_types(self):
        """Test decimal, float, and numeric type handling"""
        # Create table with various numeric types
        self.mysql.execute(f"""
        CREATE TABLE `{TEST_TABLE_NAME}` (
            id int NOT NULL AUTO_INCREMENT,
            name varchar(255),
            price decimal(10,2),
            rate float,
            percentage double,
            small_num tinyint,
            big_num bigint,
            PRIMARY KEY (id)
        );
        """)

        # Insert numeric test data
        numeric_data = [
            {
                "name": "Product1",
                "price": Decimal("123.45"),
                "rate": 1.23,
                "percentage": 99.9876,
                "small_num": 127,
                "big_num": 9223372036854775807,
            },
            {
                "name": "Product2",
                "price": Decimal("0.01"),
                "rate": 0.0,
                "percentage": 0.0001,
                "small_num": -128,
                "big_num": -9223372036854775808,
            },
        ]

        self.insert_multiple_records(TEST_TABLE_NAME, numeric_data)

        # Start replication
        self.start_replication()

        # Verify numeric data replication
        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=2)

        # Verify specific numeric values
        self.verify_record_exists(
            TEST_TABLE_NAME,
            "name='Product1'",
            {"price": Decimal("123.45"), "small_num": 127},
        )

        # Verify edge cases
        self.verify_record_exists(
            TEST_TABLE_NAME,
            "name='Product2'",
            {"price": Decimal("0.01"), "small_num": -128},
        )

    @pytest.mark.integration
    def test_text_and_blob_types(self):
        """Test TEXT, BLOB, and binary type handling"""
        # Create table with text/blob types
        self.mysql.execute(f"""
        CREATE TABLE `{TEST_TABLE_NAME}` (
            id int NOT NULL AUTO_INCREMENT,
            name varchar(255),
            short_text text,
            long_text longtext,
            binary_data blob,
            large_binary longblob,
            json_data json,
            PRIMARY KEY (id)
        );
        """)

        # Insert text/blob test data
        text_data = [
            {
                "name": "TextTest1",
                "short_text": "Short text content",
                "long_text": "Very long text content " * 100,  # Make it long
                "binary_data": b"binary_content_123",
                "large_binary": b"large_binary_content" * 50,
                "json_data": '{"key": "value", "number": 42}',
            },
            {
                "name": "TextTest2",
                "short_text": None,
                "long_text": "Unicode content: åäöüñç",
                "binary_data": None,
                "large_binary": b"",
                "json_data": '{"array": [1, 2, 3], "nested": {"inner": true}}',
            },
        ]

        self.insert_multiple_records(TEST_TABLE_NAME, text_data)

        # Start replication
        self.start_replication()

        # Verify text/blob replication
        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=2)

        # Verify text content
        self.verify_record_exists(
            TEST_TABLE_NAME, "name='TextTest1'", {"short_text": "Short text content"}
        )

        # Verify unicode handling
        self.verify_record_exists(
            TEST_TABLE_NAME,
            "name='TextTest2'",
            {"long_text": "Unicode content: åäöüñç"},
        )

        # Verify NULL handling
        self.verify_record_exists(
            TEST_TABLE_NAME, "name='TextTest2' AND short_text IS NULL"
        )

    @pytest.mark.integration
    def test_boolean_and_bit_types(self):
        """Test boolean and bit type handling"""
        # Create table with boolean/bit types
        self.mysql.execute(f"""
        CREATE TABLE `{TEST_TABLE_NAME}` (
            id int NOT NULL AUTO_INCREMENT,
            name varchar(255),
            is_active boolean,
            status_flag bit(1),
            multi_bit bit(8),
            tinyint_bool tinyint(1),
            PRIMARY KEY (id)
        );
        """)

        # Insert boolean test data
        boolean_data = [
            {
                "name": "BoolTest1",
                "is_active": True,
                "status_flag": 1,
                "multi_bit": 255,  # Max for 8-bit
                "tinyint_bool": 1,
            },
            {
                "name": "BoolTest2",
                "is_active": False,
                "status_flag": 0,
                "multi_bit": 0,
                "tinyint_bool": 0,
            },
        ]

        self.insert_multiple_records(TEST_TABLE_NAME, boolean_data)

        # Start replication
        self.start_replication()

        # Verify boolean replication
        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=2)

        # Verify boolean values
        self.verify_record_exists(
            TEST_TABLE_NAME, "name='BoolTest1'", {"is_active": True, "tinyint_bool": 1}
        )

        self.verify_record_exists(
            TEST_TABLE_NAME, "name='BoolTest2'", {"is_active": False, "tinyint_bool": 0}
        )

    @pytest.mark.integration
    def test_null_value_handling(self):
        """Test NULL value handling across different data types"""
        # Create table with nullable fields of various types
        self.mysql.execute(f"""
        CREATE TABLE `{TEST_TABLE_NAME}` (
            id int NOT NULL AUTO_INCREMENT,
            name varchar(255),
            age int NULL,
            price decimal(10,2) NULL,
            created_date datetime NULL,
            is_active boolean NULL,
            description text NULL,
            binary_data blob NULL,
            PRIMARY KEY (id)
        );
        """)

        # Insert records with NULL values
        null_data = [
            {
                "name": "NullTest1",
                "age": None,
                "price": None,
                "created_date": None,
                "is_active": None,
                "description": None,
                "binary_data": None,
            },
            {
                "name": "MixedNull",
                "age": 30,
                "price": Decimal("19.99"),
                "created_date": None,  # Some NULL, some not
                "is_active": True,
                "description": "Has description",
                "binary_data": None,
            },
        ]

        self.insert_multiple_records(TEST_TABLE_NAME, null_data)

        # Start replication
        self.start_replication()

        # Verify NULL handling
        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=2)

        # Verify NULL values are preserved
        self.verify_record_exists(TEST_TABLE_NAME, "name='NullTest1' AND age IS NULL")
        self.verify_record_exists(TEST_TABLE_NAME, "name='NullTest1' AND price IS NULL")
        self.verify_record_exists(
            TEST_TABLE_NAME, "name='NullTest1' AND created_date IS NULL"
        )

        # Verify mixed NULL/non-NULL
        self.verify_record_exists(TEST_TABLE_NAME, "name='MixedNull'", {"age": 30})
        self.verify_record_exists(
            TEST_TABLE_NAME, "name='MixedNull' AND created_date IS NULL"
        )
