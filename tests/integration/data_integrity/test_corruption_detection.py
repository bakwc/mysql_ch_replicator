"""Corruption detection and handling tests"""

import json
import os
from decimal import Decimal

import pytest

from tests.base import BaseReplicationTest, DataTestMixin, SchemaTestMixin
from tests.conftest import TEST_TABLE_NAME


class TestCorruptionDetection(BaseReplicationTest, SchemaTestMixin, DataTestMixin):
    """Test detection and handling of corrupted data during replication"""

    @pytest.mark.integration
    def test_corrupted_json_data_handling(self):
        """Test handling of corrupted JSON data"""
        # Create table with JSON column
        self.mysql.execute(f"""
        CREATE TABLE `{TEST_TABLE_NAME}` (
            id int NOT NULL AUTO_INCREMENT,
            name varchar(255),
            config json,
            PRIMARY KEY (id)
        );
        """)

        # Insert valid JSON data first
        valid_data = [
            {
                "name": "ValidUser1",
                "config": json.dumps({"theme": "dark", "notifications": True})
            },
            {
                "name": "ValidUser2", 
                "config": json.dumps({"theme": "light", "notifications": False})
            }
        ]

        self.insert_multiple_records(TEST_TABLE_NAME, valid_data)

        # Start replication
        self.start_replication()
        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=2)

        # Verify initial valid data
        self.verify_record_exists(TEST_TABLE_NAME, "name='ValidUser1'")
        self.verify_record_exists(TEST_TABLE_NAME, "name='ValidUser2'")

        # Now test with potentially corrupted JSON-like data
        # Note: This simulates scenarios where data might be malformed
        edge_case_data = [
            {
                "name": "EdgeCase1",
                "config": '{"incomplete": true'  # Malformed JSON
            },
            {
                "name": "EdgeCase2",
                "config": '{"valid": "json", "number": 123}'
            },
            {
                "name": "EdgeCase3",
                "config": None  # NULL JSON
            }
        ]

        # Insert edge cases and verify replication continues
        for record in edge_case_data:
            try:
                self.insert_multiple_records(TEST_TABLE_NAME, [record])
            except Exception as e:
                # Log but don't fail - some malformed data might be rejected by MySQL
                print(f"Expected MySQL rejection of malformed data: {e}")

        # Verify replication is still working with valid data
        final_valid_data = [
            {
                "name": "FinalValid",
                "config": json.dumps({"recovery": True, "status": "working"})
            }
        ]

        self.insert_multiple_records(TEST_TABLE_NAME, final_valid_data)
        
        # Wait and verify the final record made it through
        self.wait_for_record_exists(TEST_TABLE_NAME, "name='FinalValid'")

    @pytest.mark.integration
    def test_numeric_overflow_detection(self):
        """Test detection of numeric overflow conditions"""
        # Create table with various numeric constraints
        self.mysql.execute(f"""
        CREATE TABLE `{TEST_TABLE_NAME}` (
            id int NOT NULL AUTO_INCREMENT,
            name varchar(255),
            small_int tinyint,
            medium_val decimal(5,2),
            large_val bigint,
            PRIMARY KEY (id)
        );
        """)

        # Insert valid data first
        valid_data = [
            {
                "name": "ValidNumbers",
                "small_int": 100,
                "medium_val": Decimal("999.99"),
                "large_val": 1234567890
            }
        ]

        self.insert_multiple_records(TEST_TABLE_NAME, valid_data)

        # Start replication
        self.start_replication()
        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=1)

        # Test boundary conditions
        boundary_data = [
            {
                "name": "MaxTinyInt",
                "small_int": 127,  # Max tinyint
                "medium_val": Decimal("999.99"),
                "large_val": 9223372036854775807  # Max bigint
            },
            {
                "name": "MinValues",
                "small_int": -128,  # Min tinyint
                "medium_val": Decimal("-999.99"),
                "large_val": -9223372036854775808  # Min bigint
            }
        ]

        self.insert_multiple_records(TEST_TABLE_NAME, boundary_data)
        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=3)

        # Verify boundary values were replicated correctly
        self.verify_record_exists(TEST_TABLE_NAME, "name='MaxTinyInt'")
        self.verify_record_exists(TEST_TABLE_NAME, "name='MinValues'")

    @pytest.mark.integration
    def test_character_encoding_corruption_detection(self):
        """Test detection of character encoding issues"""
        # Create table with UTF-8 data
        self.mysql.execute(f"""
        CREATE TABLE `{TEST_TABLE_NAME}` (
            id int NOT NULL AUTO_INCREMENT,
            name varchar(255),
            description text,
            PRIMARY KEY (id)
        );
        """)

        # Insert data with various character encodings
        encoding_data = [
            {
                "name": "ASCII_Data",
                "description": "Simple ASCII text with basic characters 123 ABC"
            },
            {
                "name": "UTF8_Basic",
                "description": "Basic UTF-8: café naïve résumé"
            },
            {
                "name": "UTF8_Extended",
                "description": "Extended UTF-8: 测试数据 العربية русский 🎉 αβγδ"
            },
            {
                "name": "Special_Chars",
                "description": "Special chars: !@#$%^&*()_+-=[]{}|;':\",./<>?"
            },
            {
                "name": "Unicode_Emoji",
                "description": "Emojis: 😀😃😄😁😆😅😂🤣😊😇🙂🙃"
            }
        ]

        self.insert_multiple_records(TEST_TABLE_NAME, encoding_data)

        # Start replication
        self.start_replication()
        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=5)

        # Verify all character encodings were preserved
        for record in encoding_data:
            self.verify_record_exists(TEST_TABLE_NAME, f"name='{record['name']}'")

        # Test that data integrity is maintained
        ch_records = self.ch.select(TEST_TABLE_NAME, order_by="id")
        mysql_records = []
        
        self.mysql.execute(f"SELECT name, description FROM `{TEST_TABLE_NAME}` ORDER BY id")
        mysql_records = self.mysql.cursor.fetchall()

        # Compare character data integrity
        assert len(ch_records) == len(mysql_records), "Record count mismatch"
        
        for i, (ch_record, mysql_record) in enumerate(zip(ch_records, mysql_records)):
            mysql_name, mysql_desc = mysql_record
            ch_name = ch_record['name']
            ch_desc = ch_record['description']
            
            assert mysql_name == ch_name, f"Name mismatch at record {i}: MySQL='{mysql_name}', CH='{ch_name}'"
            assert mysql_desc == ch_desc, f"Description mismatch at record {i}: MySQL='{mysql_desc}', CH='{ch_desc}'"

    @pytest.mark.integration 
    def test_state_file_corruption_recovery(self):
        """Test recovery from corrupted state files"""
        # Create table and insert initial data
        self.create_basic_table(TEST_TABLE_NAME)
        initial_data = [{"name": "InitialRecord", "age": 25}]
        self.insert_multiple_records(TEST_TABLE_NAME, initial_data)

        # Start replication
        self.start_replication()
        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=1)

        # Stop replication to simulate state file corruption
        self.stop_replication()

        # Simulate state file corruption by creating invalid state file
        state_dir = os.path.join(self.cfg.binlog_replicator.data_dir, self.test_db_name)
        state_file = os.path.join(state_dir, "state.pckl")
        
        # Backup original state if it exists
        backup_state = None
        if os.path.exists(state_file):
            with open(state_file, 'rb') as f:
                backup_state = f.read()

        # Create corrupted state file
        with open(state_file, 'w') as f:
            f.write("corrupted state data that is not valid pickle")

        # Try to restart replication - should handle corruption gracefully
        try:
            self.start_replication()
            
            # Add new data to verify replication recovery
            recovery_data = [{"name": "RecoveryRecord", "age": 30}]
            self.insert_multiple_records(TEST_TABLE_NAME, recovery_data)
            
            # Should be able to replicate despite state file corruption
            self.wait_for_record_exists(TEST_TABLE_NAME, "name='RecoveryRecord'")
            
        finally:
            # Restore original state if we had one
            if backup_state and os.path.exists(state_file):
                with open(state_file, 'wb') as f:
                    f.write(backup_state)