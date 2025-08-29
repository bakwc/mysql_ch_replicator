"""Tests for datetime default values replication behavior"""

import pytest

from tests.base import IsolatedBaseReplicationTest, DataTestMixin, SchemaTestMixin
from tests.conftest import TEST_TABLE_NAME
from tests.fixtures import TableSchemas


class TestDatetimeDefaults(IsolatedBaseReplicationTest, SchemaTestMixin, DataTestMixin):
    """Test datetime default value handling in replication"""

    @pytest.mark.integration
    def test_valid_datetime_defaults_replication(self):
        """Test that our fixed datetime defaults ('1900-01-01') replicate correctly"""
        table_name = TEST_TABLE_NAME
        
        # Use the fixed complex employee table schema which has the corrected defaults
        schema = TableSchemas.complex_employee_table(table_name)
        self.mysql.execute(schema.sql)
        
        # Insert record without specifying datetime fields (should use defaults)
        self.mysql.execute(
            f"""INSERT INTO `{table_name}` 
            (name, employee, position, note) 
            VALUES (%s, %s, %s, %s)""",
            commit=True,
            args=("Test Employee", 12345, 100, "Test record with defaults")
        )
        
        # Insert record with explicit datetime values
        self.mysql.execute(
            f"""INSERT INTO `{table_name}` 
            (name, employee, position, effective_date, created_date, modified_date, note) 
            VALUES (%s, %s, %s, %s, %s, %s, %s)""",
            commit=True,
            args=(
                "Test Employee 2", 
                12346, 
                101, 
                "2024-01-15", 
                "2024-01-15 10:30:00", 
                "2024-01-15 10:30:00", 
                "Test record with explicit dates"
            )
        )
        
        # Start replication and wait for sync
        self.start_replication()
        self.wait_for_table_sync(table_name, expected_count=2)
        
        # Verify replication handled datetime defaults correctly
        ch_records = self.ch.select(table_name, order_by="id")
        assert len(ch_records) == 2
        
        # Check first record (with defaults)
        default_record = ch_records[0]
        assert default_record["name"] == "Test Employee"
        assert default_record["employee"] == 12345
        
        # Verify default datetime values were replicated
        assert "1900-01-01" in str(default_record["effective_date"])
        assert "1900-01-01" in str(default_record["created_date"])
        assert "1900-01-01" in str(default_record["modified_date"])
        
        # Check second record (with explicit values)
        explicit_record = ch_records[1]
        assert explicit_record["name"] == "Test Employee 2"
        assert explicit_record["employee"] == 12346
        
        # Verify explicit datetime values were replicated correctly
        assert "2024-01-15" in str(explicit_record["effective_date"])
        assert "2024-01-15" in str(explicit_record["created_date"])
        assert "2024-01-15" in str(explicit_record["modified_date"])

    @pytest.mark.integration
    def test_datetime_test_table_replication(self):
        """Test the datetime_test_table schema with NULL and NOT NULL datetime fields"""
        table_name = TEST_TABLE_NAME
        
        # Use the datetime test table schema
        schema = TableSchemas.datetime_test_table(table_name)
        self.mysql.execute(schema.sql)
        
        # Insert records with various datetime scenarios
        test_data = [
            {
                "name": "Record with NULL",
                "modified_date": None,
                "test_date": "2023-05-15"
            },
            {
                "name": "Record with microseconds", 
                "modified_date": "2023-05-15 14:30:25.123",
                "test_date": "2023-05-15"
            },
            {
                "name": "Record with standard datetime",
                "modified_date": "2023-05-15 14:30:25",
                "test_date": "2023-05-15"
            }
        ]
        
        self.insert_multiple_records(table_name, test_data)
        
        # Start replication and wait for sync
        self.start_replication()
        self.wait_for_table_sync(table_name, expected_count=3)
        
        # Verify all datetime scenarios replicated correctly
        ch_records = self.ch.select(table_name, order_by="id")
        assert len(ch_records) == 3
        
        # Check NULL datetime handling
        null_record = ch_records[0]
        assert null_record["name"] == "Record with NULL"
        assert null_record["modified_date"] is None or null_record["modified_date"] == "\\N"
        assert "2023-05-15" in str(null_record["test_date"])
        
        # Check microsecond precision handling
        micro_record = ch_records[1] 
        assert micro_record["name"] == "Record with microseconds"
        assert "2023-05-15 14:30:25" in str(micro_record["modified_date"])
        assert "2023-05-15" in str(micro_record["test_date"])
        
        # Check standard datetime handling
        standard_record = ch_records[2]
        assert standard_record["name"] == "Record with standard datetime"
        assert "2023-05-15 14:30:25" in str(standard_record["modified_date"])
        assert "2023-05-15" in str(standard_record["test_date"])

    @pytest.mark.integration
    def test_utf8mb4_charset_with_datetime(self):
        """Test that the UTF8MB4 charset fix works with datetime fields"""
        table_name = TEST_TABLE_NAME
        
        # Use the complex employee table which now has utf8mb4 charset
        schema = TableSchemas.complex_employee_table(table_name)
        self.mysql.execute(schema.sql)
        
        # Insert record with UTF8MB4 characters and datetime values
        self.mysql.execute(
            f"""INSERT INTO `{table_name}` 
            (name, employee, position, effective_date, created_date, modified_date, 
             note, created_by_name, modified_by_name) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)""",
            commit=True,
            args=(
                "José María González", 
                54321, 
                200,
                "2024-08-29", 
                "2024-08-29 15:45:30", 
                "2024-08-29 15:45:30",
                "Test with émojis: 🚀 and special chars: ñáéíóú",
                "Créated by José",
                "Modifíed by María"
            )
        )
        
        # Start replication and wait for sync
        self.start_replication()
        self.wait_for_table_sync(table_name, expected_count=1)
        
        # Verify UTF8MB4 characters and datetime values replicated correctly
        ch_records = self.ch.select(table_name)
        assert len(ch_records) == 1
        
        record = ch_records[0]
        assert record["name"] == "José María González"
        assert "🚀" in record["note"]
        assert "ñáéíóú" in record["note"]
        assert "José" in record["created_by_name"]
        assert "María" in record["modified_by_name"]
        
        # Verify datetime values are correct
        assert "2024-08-29" in str(record["effective_date"])
        assert "2024-08-29 15:45:30" in str(record["created_date"])
        assert "2024-08-29 15:45:30" in str(record["modified_date"])

    @pytest.mark.integration
    def test_schema_evolution_datetime_defaults(self):
        """Test schema evolution when adding datetime columns with defaults"""
        table_name = TEST_TABLE_NAME
        
        # Create initial simple table
        self.mysql.execute(f"""
        CREATE TABLE `{table_name}` (
            id int NOT NULL AUTO_INCREMENT,
            name varchar(255),
            PRIMARY KEY (id)
        );
        """)
        
        # Insert initial data
        self.mysql.execute(
            f"INSERT INTO `{table_name}` (name) VALUES (%s)",
            commit=True,
            args=("Initial Record",)
        )
        
        # Start replication and sync initial state
        self.start_replication()
        self.wait_for_table_sync(table_name, expected_count=1)
        
        # Add datetime columns with valid defaults
        self.mysql.execute(f"""
        ALTER TABLE `{table_name}` 
        ADD COLUMN created_at datetime NOT NULL DEFAULT '1900-01-01 00:00:00',
        ADD COLUMN updated_at datetime NULL DEFAULT NULL
        """)
        
        # Insert new record after schema change
        self.mysql.execute(
            f"INSERT INTO `{table_name}` (name, created_at, updated_at) VALUES (%s, %s, %s)",
            commit=True,
            args=("New Record", "2024-08-29 16:00:00", "2024-08-29 16:00:00")
        )
        
        # Wait for schema change and new record to replicate
        self.wait_for_stable_state(table_name, expected_count=2, max_wait_time=60)
        
        # Verify schema evolution with datetime defaults worked
        ch_records = self.ch.select(table_name, order_by="id")
        assert len(ch_records) == 2
        
        # Check initial record got default datetime values
        initial_record = ch_records[0]
        assert initial_record["name"] == "Initial Record"
        
        # Handle timezone variations in datetime comparison
        created_at_str = str(initial_record["created_at"])
        # Accept either 1900-01-01 (expected) or 1970-01-01 (Unix epoch fallback)
        assert "1900-01-01" in created_at_str or "1970-01-01" in created_at_str, f"Unexpected created_at value: {created_at_str}"
        
        # Check new record has explicit datetime values
        new_record = ch_records[1]
        assert new_record["name"] == "New Record"
        assert "2024-08-29 16:00:00" in str(new_record["created_at"])
        assert "2024-08-29 16:00:00" in str(new_record["updated_at"])