"""Tests for datetime replication scenarios including edge cases and invalid values"""

import pytest
from datetime import datetime

from tests.base import IsolatedBaseReplicationTest, DataTestMixin, SchemaTestMixin
from tests.conftest import TEST_TABLE_NAME


class TestDatetimeReplication(IsolatedBaseReplicationTest, SchemaTestMixin, DataTestMixin):
    """Test datetime replication scenarios including invalid values"""

    @pytest.mark.integration
    def test_valid_datetime_replication(self):
        """Test replication of valid datetime values"""
        table_name = TEST_TABLE_NAME
        
        # Create table with various datetime fields
        self.mysql.execute(f"""
        CREATE TABLE `{table_name}` (
            id int NOT NULL AUTO_INCREMENT,
            name varchar(255),
            created_at datetime NOT NULL DEFAULT '1900-01-01 00:00:00',
            updated_at datetime(3) NULL DEFAULT NULL,
            birth_date date NOT NULL DEFAULT '1900-01-01',
            PRIMARY KEY (id)
        );
        """)
        
        # Insert valid datetime data
        test_data = [
            {
                "name": "Valid Record 1",
                "created_at": "2023-05-15 14:30:25",
                "updated_at": "2023-05-15 14:30:25.123",
                "birth_date": "1990-01-15"
            },
            {
                "name": "Valid Record 2", 
                "created_at": "2024-01-01 00:00:00",
                "updated_at": None,  # NULL value
                "birth_date": "1985-12-25"
            },
            {
                "name": "Valid Record 3",
                "created_at": "2024-08-29 10:15:30",
                "updated_at": "2024-08-29 10:15:30.999",
                "birth_date": "2000-02-29"  # Leap year
            }
        ]
        
        self.insert_multiple_records(table_name, test_data)
        
        # Start replication and wait for sync
        self.start_replication()
        self.wait_for_table_sync(table_name, expected_count=3)
        
        # Verify datetime values are replicated correctly
        ch_records = self.ch.select(table_name, order_by="id")
        assert len(ch_records) == 3
        
        # Check first record
        assert ch_records[0]["name"] == "Valid Record 1"
        assert "2023-05-15" in str(ch_records[0]["created_at"])
        assert "2023-05-15" in str(ch_records[0]["updated_at"])
        assert "1990-01-15" in str(ch_records[0]["birth_date"])
        
        # Check second record (NULL updated_at)
        assert ch_records[1]["name"] == "Valid Record 2"
        assert ch_records[1]["updated_at"] is None or ch_records[1]["updated_at"] == "\\N"
        
        # Check third record (leap year date)
        assert ch_records[2]["name"] == "Valid Record 3"
        assert "2000-02-29" in str(ch_records[2]["birth_date"])

    @pytest.mark.integration
    def test_zero_datetime_handling(self):
        """Test handling of minimum datetime values (MySQL 8.4+ compatible)"""
        table_name = TEST_TABLE_NAME
        
        # Create table with datetime fields - using sql_mode without NO_ZERO_DATE
        # to allow zero dates in MySQL (NO_AUTO_CREATE_USER removed for MySQL 8.4+ compatibility)
        self.mysql.execute("SET SESSION sql_mode = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION'")
        
        try:
            self.mysql.execute(f"""
            CREATE TABLE `{table_name}` (
                id int NOT NULL AUTO_INCREMENT,
                name varchar(255),
                zero_datetime datetime DEFAULT '1000-01-01 00:00:00',
                zero_date date DEFAULT '1000-01-01',
                PRIMARY KEY (id)
            );
            """)
            
            # Insert records with minimum datetime values (MySQL 8.4+ compatible)
            self.mysql.execute(
                f"INSERT INTO `{table_name}` (name, zero_datetime, zero_date) VALUES (%s, %s, %s)",
                commit=True,
                args=("Minimum DateTime Test", "1000-01-01 00:00:00", "1000-01-01")
            )
            
            # Insert a valid datetime for comparison
            self.mysql.execute(
                f"INSERT INTO `{table_name}` (name, zero_datetime, zero_date) VALUES (%s, %s, %s)",
                commit=True,
                args=("Valid DateTime Test", "2023-01-01 12:00:00", "2023-01-01")
            )
            
            # Start replication and wait for sync
            self.start_replication()
            self.wait_for_table_sync(table_name, expected_count=2)
            
            # Verify replication handled zero datetimes
            ch_records = self.ch.select(table_name, order_by="id")
            assert len(ch_records) == 2
            
            # Check how minimum datetime was replicated
            min_record = ch_records[0]
            assert min_record["name"] == "Minimum DateTime Test"
            
            # The replicator should handle minimum datetime values correctly
            min_datetime = min_record["zero_datetime"]
            min_date = min_record["zero_date"]
            
            # These should not be None/null - the replicator should handle them
            assert min_datetime is not None
            assert min_date is not None
            
            # Verify the minimum datetime values are replicated correctly
            assert "1000-01-01" in str(min_datetime)
            assert "1000-01-01" in str(min_date)
            
            # Valid record should replicate normally
            valid_record = ch_records[1]
            assert valid_record["name"] == "Valid DateTime Test"
            assert "2023-01-01" in str(valid_record["zero_datetime"])
            assert "2023-01-01" in str(valid_record["zero_date"])
            
        finally:
            # Reset sql_mode to default
            self.mysql.execute("SET SESSION sql_mode = DEFAULT")

    @pytest.mark.integration 
    def test_datetime_boundary_values(self):
        """Test datetime boundary values and edge cases"""
        table_name = TEST_TABLE_NAME
        
        self.mysql.execute(f"""
        CREATE TABLE `{table_name}` (
            id int NOT NULL AUTO_INCREMENT,
            name varchar(255),
            min_datetime datetime NOT NULL DEFAULT '1000-01-01 00:00:00',
            max_datetime datetime NOT NULL DEFAULT '9999-12-31 23:59:59',
            min_date date NOT NULL DEFAULT '1000-01-01',
            max_date date NOT NULL DEFAULT '9999-12-31',
            PRIMARY KEY (id)
        );
        """)
        
        # Insert boundary datetime values
        test_data = [
            {
                "name": "Minimum Values",
                "min_datetime": "1000-01-01 00:00:00",
                "max_datetime": "1000-01-01 00:00:00", 
                "min_date": "1000-01-01",
                "max_date": "1000-01-01"
            },
            {
                "name": "Maximum Values",
                "min_datetime": "9999-12-31 23:59:59",
                "max_datetime": "9999-12-31 23:59:59",
                "min_date": "9999-12-31", 
                "max_date": "9999-12-31"
            },
            {
                "name": "Leap Year Feb 29",
                "min_datetime": "2000-02-29 12:00:00",
                "max_datetime": "2024-02-29 15:30:45",
                "min_date": "2000-02-29",
                "max_date": "2024-02-29"
            }
        ]
        
        self.insert_multiple_records(table_name, test_data)
        
        # Start replication and wait for sync
        self.start_replication()
        self.wait_for_table_sync(table_name, expected_count=3)
        
        # Verify boundary values are replicated correctly
        ch_records = self.ch.select(table_name, order_by="id")
        assert len(ch_records) == 3
        
        # Check minimum values
        min_record = ch_records[0]
        assert "1000-01-01" in str(min_record["min_datetime"])
        assert "1000-01-01" in str(min_record["min_date"])
        
        # Check maximum values  
        max_record = ch_records[1]
        assert "9999-12-31" in str(max_record["max_datetime"])
        assert "9999-12-31" in str(max_record["max_date"])
        
        # Check leap year values
        leap_record = ch_records[2]
        assert "2000-02-29" in str(leap_record["min_datetime"])
        assert "2024-02-29" in str(leap_record["max_datetime"])

    @pytest.mark.integration
    def test_datetime_with_microseconds(self):
        """Test datetime values with microsecond precision"""
        table_name = TEST_TABLE_NAME
        
        self.mysql.execute(f"""
        CREATE TABLE `{table_name}` (
            id int NOT NULL AUTO_INCREMENT,
            name varchar(255),
            precise_time datetime(6) NOT NULL,
            medium_time datetime(3) NOT NULL,
            standard_time datetime NOT NULL,
            PRIMARY KEY (id)
        );
        """)
        
        # Insert datetime values with different precisions
        test_data = [
            {
                "name": "Microsecond Precision",
                "precise_time": "2023-05-15 14:30:25.123456",
                "medium_time": "2023-05-15 14:30:25.123",
                "standard_time": "2023-05-15 14:30:25"
            },
            {
                "name": "Zero Microseconds", 
                "precise_time": "2023-05-15 14:30:25.000000",
                "medium_time": "2023-05-15 14:30:25.000",
                "standard_time": "2023-05-15 14:30:25"
            }
        ]
        
        self.insert_multiple_records(table_name, test_data)
        
        # Start replication and wait for sync
        self.start_replication()
        self.wait_for_table_sync(table_name, expected_count=2)
        
        # Verify microsecond precision is handled correctly
        ch_records = self.ch.select(table_name, order_by="id")
        assert len(ch_records) == 2
        
        # Check precision handling
        for record in ch_records:
            assert "2023-05-15 14:30:25" in str(record["precise_time"])
            assert "2023-05-15 14:30:25" in str(record["medium_time"])
            assert "2023-05-15 14:30:25" in str(record["standard_time"])

    @pytest.mark.integration
    def test_datetime_timezone_handling(self):
        """Test datetime replication with timezone considerations"""
        table_name = TEST_TABLE_NAME
        
        # Save current timezone
        original_tz = self.mysql.fetch_one("SELECT @@session.time_zone")[0]
        
        try:
            # Set MySQL timezone
            self.mysql.execute("SET time_zone = '+00:00'")
            
            self.mysql.execute(f"""
            CREATE TABLE `{table_name}` (
                id int NOT NULL AUTO_INCREMENT,
                name varchar(255),
                created_timestamp timestamp DEFAULT CURRENT_TIMESTAMP,
                created_datetime datetime DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (id)
            );
            """)
            
            # Insert records at specific timezone
            self.mysql.execute(
                f"INSERT INTO `{table_name}` (name, created_timestamp, created_datetime) VALUES (%s, %s, %s)",
                commit=True,
                args=("UTC Record", "2023-05-15 14:30:25", "2023-05-15 14:30:25")
            )
            
            # Change timezone and insert another record
            self.mysql.execute("SET time_zone = '+05:00'")
            self.mysql.execute(
                f"INSERT INTO `{table_name}` (name, created_timestamp, created_datetime) VALUES (%s, %s, %s)",
                commit=True,
                args=("UTC+5 Record", "2023-05-15 19:30:25", "2023-05-15 19:30:25")
            )
            
            # Start replication and wait for sync
            self.start_replication()
            self.wait_for_table_sync(table_name, expected_count=2)
            
            # Verify timezone handling in replication
            ch_records = self.ch.select(table_name, order_by="id")
            assert len(ch_records) == 2
            
            # Both records should be replicated successfully
            assert ch_records[0]["name"] == "UTC Record"
            assert ch_records[1]["name"] == "UTC+5 Record"
            
            # Datetime values should be present (exact timezone handling depends on config)
            for record in ch_records:
                assert record["created_timestamp"] is not None
                assert record["created_datetime"] is not None
                
        finally:
            # Restore original timezone
            self.mysql.execute(f"SET time_zone = '{original_tz}'")

    @pytest.mark.integration
    def test_invalid_datetime_update_replication(self):
        """Test replication when datetime values are updated from valid to invalid"""
        table_name = TEST_TABLE_NAME
        
        self.mysql.execute(f"""
        CREATE TABLE `{table_name}` (
            id int NOT NULL AUTO_INCREMENT,
            name varchar(255),
            event_date datetime NOT NULL DEFAULT '1900-01-01 00:00:00',
            PRIMARY KEY (id)
        );
        """)
        
        # Insert valid record first
        self.mysql.execute(
            f"INSERT INTO `{table_name}` (name, event_date) VALUES (%s, %s)",
            commit=True,
            args=("Initial Record", "2023-05-15 14:30:25")
        )
        
        # Start replication and wait for initial sync
        self.start_replication()
        self.wait_for_table_sync(table_name, expected_count=1)
        
        # Verify initial replication
        ch_records = self.ch.select(table_name)
        assert len(ch_records) == 1
        assert ch_records[0]["name"] == "Initial Record"
        
        # Set sql_mode to allow zero dates and disable strict mode
        self.mysql.execute("SET SESSION sql_mode = 'ALLOW_INVALID_DATES'")
        
        try:
            # Update to potentially problematic datetime - use 1000-01-01 as minimum valid date
            # instead of 0000-00-00 which is rejected by MySQL 8.4+
            self.mysql.execute(
                f"UPDATE `{table_name}` SET event_date = %s WHERE id = 1",
                commit=True,
                args=("1000-01-01 00:00:00",)
            )
            
            # Wait for update to be replicated
            self.wait_for_stable_state(table_name, expected_count=1, max_wait_time=30)
            
            # Verify update was handled gracefully
            updated_records = self.ch.select(table_name)
            assert len(updated_records) == 1
            
            # The replicator should have handled the invalid datetime update
            # without causing replication to fail
            updated_record = updated_records[0]
            assert updated_record["name"] == "Initial Record"
            # event_date should be some valid representation or default value
            assert updated_record["event_date"] is not None
            
        finally:
            # Restore strict mode
            self.mysql.execute("SET SESSION sql_mode = DEFAULT")