"""Comprehensive data type tests covering remaining edge cases"""

import datetime
from decimal import Decimal

import pytest

from tests.base import DataTestMixin, IsolatedBaseReplicationTest, SchemaTestMixin
from tests.conftest import TEST_TABLE_NAME


class TestComprehensiveDataTypes(
    IsolatedBaseReplicationTest, SchemaTestMixin, DataTestMixin
):
    """Test comprehensive data type scenarios and edge cases"""

    @pytest.mark.integration
    def test_different_types_comprehensive_1(self):
        """Test comprehensive data types scenario 1 - Mixed basic types"""
        # Create table with diverse data types
        self.mysql.execute(f"""
        CREATE TABLE `{TEST_TABLE_NAME}` (
            id int NOT NULL AUTO_INCREMENT,
            name varchar(255),
            age tinyint unsigned,
            salary decimal(12,2),
            is_manager boolean,
            hire_date date,
            last_login datetime,
            work_hours time,
            birth_year year,
            notes text,
            profile_pic blob,
            PRIMARY KEY (id)
        );
        """)

        # Insert comprehensive test data
        test_data = [
            {
                "name": "Alice Johnson",
                "age": 32,
                "salary": 75000.50,
                "is_manager": True,
                "hire_date": datetime.date(2020, 3, 15),
                "last_login": datetime.datetime(2023, 6, 15, 9, 30, 45),
                "work_hours": datetime.time(8, 30, 0),
                "birth_year": 1991,
                "notes": "Experienced developer with strong leadership skills",
                "profile_pic": b"fake_image_binary_data_123",
            },
            {
                "name": "Bob Smith",
                "age": 28,
                "salary": 60000.00,
                "is_manager": False,
                "hire_date": datetime.date(2021, 7, 1),
                "last_login": datetime.datetime(2023, 6, 14, 17, 45, 30),
                "work_hours": datetime.time(9, 0, 0),
                "birth_year": 1995,
                "notes": None,  # NULL text field
                "profile_pic": None,  # NULL blob field
            },
            {
                "name": "Carol Davis",
                "age": 45,
                "salary": 95000.75,
                "is_manager": True,
                "hire_date": datetime.date(2018, 1, 10),
                "last_login": None,  # NULL datetime
                "work_hours": datetime.time(7, 45, 0),
                "birth_year": 1978,
                "notes": "Senior architect with 20+ years experience",
                "profile_pic": b"",  # Empty blob
            },
        ]

        self.insert_multiple_records(TEST_TABLE_NAME, test_data)

        # Start replication
        self.start_replication()
        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=3)

        # Verify comprehensive data replication
        self.verify_record_exists(
            TEST_TABLE_NAME,
            "name='Alice Johnson'",
            {
                "age": 32,
                "salary": 75000.50,
                "is_manager": True,
                "birth_year": 1991,
            },
        )

        self.verify_record_exists(
            TEST_TABLE_NAME,
            "name='Bob Smith'",
            {"age": 28, "is_manager": False, "birth_year": 1995},
        )

        # Verify comprehensive NULL handling across different data types
        self.verify_record_exists(
            TEST_TABLE_NAME,
            "name='Bob Smith' AND notes IS NULL",  # TEXT field NULL
        )
        self.verify_record_exists(
            TEST_TABLE_NAME,
            "name='Carol Davis' AND last_login IS NULL",  # DATETIME field NULL
        )

        # Verify comprehensive data type preservation for complex employee data
        self.verify_record_exists(
            TEST_TABLE_NAME,
            "name='Carol Davis'",
            {
                "age": 45,
                "is_manager": True,
                "birth_year": 1978,
                "notes": "Senior architect with 20+ years experience",
            },
        )

    @pytest.mark.integration
    def test_different_types_comprehensive_2(self):
        """Test comprehensive data types scenario 2 - Advanced numeric and string types"""
        # Create table with advanced types
        self.mysql.execute(f"""
        CREATE TABLE `{TEST_TABLE_NAME}` (
            id int NOT NULL AUTO_INCREMENT,
            product_name varchar(500),
            price_small decimal(5,2),
            price_large decimal(15,4),
            weight_kg float(7,3),
            dimensions_m double(10,6),
            quantity_tiny tinyint,
            quantity_small smallint,
            quantity_medium mediumint,
            quantity_large bigint,
            sku_code char(10),
            description longtext,
            metadata_small tinyblob,
            metadata_large longblob,
            status enum('draft','active','discontinued'),
            flags set('featured','sale','new','limited'),
            PRIMARY KEY (id)
        );
        """)

        # Insert advanced test data
        advanced_data = [
            {
                "product_name": "Premium Laptop Computer",
                "price_small": Decimal("999.99"),
                "price_large": Decimal("12345678901.2345"),
                "weight_kg": 2.156,
                "dimensions_m": 0.356789,
                "quantity_tiny": 127,
                "quantity_small": 32767,
                "quantity_medium": 8388607,
                "quantity_large": 9223372036854775807,
                "sku_code": "LAP001",
                "description": "High-performance laptop with advanced features"
                * 50,  # Long text
                "metadata_small": b"small_metadata_123",
                "metadata_large": b"large_metadata_content" * 100,  # Large blob
                "status": "active",
                "flags": "featured,new",
            },
            {
                "product_name": "Basic Mouse",
                "price_small": Decimal("19.99"),
                "price_large": Decimal("19.99"),
                "weight_kg": 0.085,
                "dimensions_m": 0.115000,
                "quantity_tiny": -128,  # Negative values
                "quantity_small": -32768,
                "quantity_medium": -8388608,
                "quantity_large": -9223372036854775808,
                "sku_code": "MOU001",
                "description": "Simple optical mouse",
                "metadata_small": None,
                "metadata_large": None,
                "status": "draft",
                "flags": "sale",
            },
            {
                "product_name": "Discontinued Keyboard",
                "price_small": Decimal("0.01"),  # Minimum decimal
                "price_large": Decimal("0.0001"),
                "weight_kg": 0.001,  # Very small float
                "dimensions_m": 0.000001,  # Very small double
                "quantity_tiny": 0,
                "quantity_small": 0,
                "quantity_medium": 0,
                "quantity_large": 0,
                "sku_code": "KEY999",
                "description": "",  # Empty string
                "metadata_small": b"",  # Empty blob
                "metadata_large": b"",
                "status": "discontinued",
                "flags": "limited",
            },
        ]

        self.insert_multiple_records(TEST_TABLE_NAME, advanced_data)

        # Start replication
        self.start_replication()
        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=3)

        # Verify advanced type replication
        self.verify_record_exists(
            TEST_TABLE_NAME,
            "product_name='Premium Laptop Computer'",
            {
                "price_small": Decimal("999.99"),
                "quantity_large": 9223372036854775807,
                "status": "active",
            },
        )

        self.verify_record_exists(
            TEST_TABLE_NAME,
            "product_name='Basic Mouse'",
            {
                "quantity_tiny": -128,
                "quantity_large": -9223372036854775808,
                "status": "draft",
            },
        )

        # Verify edge cases and empty values
        self.verify_record_exists(
            TEST_TABLE_NAME,
            "product_name='Discontinued Keyboard'",
            {"price_small": Decimal("0.01"), "status": "discontinued"},
        )
