"""Core functionality tests including multi-column operations and datetime exceptions"""

import datetime
from decimal import Decimal

import pytest

from tests.base import BaseReplicationTest, DataTestMixin, SchemaTestMixin
from tests.conftest import TEST_DB_NAME, TEST_TABLE_NAME


class TestCoreFunctionality(BaseReplicationTest, SchemaTestMixin, DataTestMixin):
    """Test core replication functionality including edge cases"""

    @pytest.mark.integration
    def test_multi_column_erase_operations(self):
        """Test multi-column erase operations during replication"""
        # Create table with multiple columns
        self.mysql.execute(f"""
        CREATE TABLE `{TEST_TABLE_NAME}` (
            id int NOT NULL AUTO_INCREMENT,
            name varchar(255),
            email varchar(255),
            age int,
            city varchar(100),
            country varchar(100),
            status varchar(50),
            PRIMARY KEY (id)
        );
        """)

        # Insert test data
        initial_data = [
            {
                "name": "John Doe",
                "email": "john@example.com",
                "age": 30,
                "city": "New York",
                "country": "USA",
                "status": "active",
            },
            {
                "name": "Jane Smith",
                "email": "jane@example.com", 
                "age": 25,
                "city": "London",
                "country": "UK",
                "status": "active",
            },
            {
                "name": "Bob Wilson",
                "email": "bob@example.com",
                "age": 35,
                "city": "Toronto",
                "country": "Canada", 
                "status": "inactive",
            },
        ]

        self.insert_multiple_records(TEST_TABLE_NAME, initial_data)

        # Start replication
        self.start_replication()
        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=3)

        # Test multi-column NULL updates (erase operations)
        self.mysql.execute(
            f"UPDATE `{TEST_TABLE_NAME}` SET email = NULL, city = NULL, country = NULL WHERE name = 'John Doe';",
            commit=True,
        )

        # Verify multi-column erase
        self.wait_for_record_update(
            TEST_TABLE_NAME,
            "name='John Doe'",
            {"email": None, "city": None, "country": None, "age": 30}
        )

        # Test partial multi-column erase
        self.mysql.execute(
            f"UPDATE `{TEST_TABLE_NAME}` SET email = NULL, status = 'suspended' WHERE name = 'Jane Smith';",
            commit=True,
        )

        self.wait_for_record_update(
            TEST_TABLE_NAME,
            "name='Jane Smith'",
            {"email": None, "status": "suspended", "city": "London"}
        )

        # Test multi-column restore
        self.mysql.execute(
            f"UPDATE `{TEST_TABLE_NAME}` SET email = 'john.doe@newdomain.com', city = 'Boston' WHERE name = 'John Doe';",
            commit=True,
        )

        self.wait_for_record_update(
            TEST_TABLE_NAME,
            "name='John Doe'",
            {"email": "john.doe@newdomain.com", "city": "Boston", "country": None}
        )

    @pytest.mark.integration
    def test_datetime_exception_handling(self):
        """Test datetime exception handling during replication"""
        # Create table with various datetime fields
        self.mysql.execute(f"""
        CREATE TABLE `{TEST_TABLE_NAME}` (
            id int NOT NULL AUTO_INCREMENT,
            name varchar(255),
            created_date datetime,
            updated_date timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            birth_date date,
            event_time time,
            event_year year,
            PRIMARY KEY (id)
        );
        """)

        # Start replication
        self.start_replication()
        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=0)

        # Test various datetime formats and edge cases
        datetime_test_cases = [
            {
                "name": "Standard Datetime",
                "created_date": datetime.datetime(2023, 6, 15, 14, 30, 45),
                "birth_date": datetime.date(1990, 1, 1),
                "event_time": datetime.time(9, 30, 0),
                "event_year": 2023,
            },
            {
                "name": "Edge Case Dates",
                "created_date": datetime.datetime(1970, 1, 1, 0, 0, 1),  # Unix epoch + 1s
                "birth_date": datetime.date(2000, 2, 29),  # Leap year
                "event_time": datetime.time(23, 59, 59),  # End of day
                "event_year": 1901,  # Min MySQL year
            },
            {
                "name": "Future Dates",
                "created_date": datetime.datetime(2050, 12, 31, 23, 59, 59),
                "birth_date": datetime.date(2025, 6, 15),
                "event_time": datetime.time(0, 0, 0),  # Start of day
                "event_year": 2155,  # Max MySQL year
            },
            {
                "name": "NULL Values",
                "created_date": None,
                "birth_date": None,
                "event_time": None,
                "event_year": None,
            },
        ]

        # Insert datetime test data
        self.insert_multiple_records(TEST_TABLE_NAME, datetime_test_cases)

        # Verify datetime replication
        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=4)

        # Verify specific datetime handling
        self.verify_record_exists(
            TEST_TABLE_NAME,
            "name='Standard Datetime'",
            {
                "created_date": datetime.datetime(2023, 6, 15, 14, 30, 45),
                "birth_date": datetime.date(1990, 1, 1),
                "event_year": 2023,
            },
        )

        # Verify edge case handling
        self.verify_record_exists(
            TEST_TABLE_NAME,
            "name='Edge Case Dates'",
            {
                "created_date": datetime.datetime(1970, 1, 1, 0, 0, 1),
                "birth_date": datetime.date(2000, 2, 29),
                "event_year": 1901,
            },
        )

        # Verify NULL datetime handling
        self.verify_record_exists(
            TEST_TABLE_NAME, "name='NULL Values' AND created_date IS NULL"
        )
        self.verify_record_exists(
            TEST_TABLE_NAME, "name='NULL Values' AND birth_date IS NULL"
        )

        # Test datetime updates
        self.mysql.execute(
            f"""UPDATE `{TEST_TABLE_NAME}` 
               SET created_date = '2024-01-01 12:00:00', 
                   birth_date = '1995-06-15'
               WHERE name = 'NULL Values';""",
            commit=True,
        )

        self.wait_for_record_update(
            TEST_TABLE_NAME,
            "name='NULL Values'",
            {
                "created_date": datetime.datetime(2024, 1, 1, 12, 0, 0),
                "birth_date": datetime.date(1995, 6, 15),
            },
        )

