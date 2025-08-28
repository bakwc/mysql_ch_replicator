"""Duplicate event detection and handling tests"""

import time

import pytest

from tests.base import BaseReplicationTest, DataTestMixin, SchemaTestMixin
from tests.conftest import TEST_TABLE_NAME


class TestDuplicateDetection(BaseReplicationTest, SchemaTestMixin, DataTestMixin):
    """Test detection and handling of duplicate events during replication"""

    @pytest.mark.integration
    def test_duplicate_insert_detection(self):
        """Test detection and handling of duplicate INSERT events"""
        # Create table with unique constraints
        self.mysql.execute(f"""
        CREATE TABLE `{TEST_TABLE_NAME}` (
            id int NOT NULL AUTO_INCREMENT,
            email varchar(255) UNIQUE,
            username varchar(255) UNIQUE,
            name varchar(255),
            PRIMARY KEY (id)
        );
        """)

        # Insert initial data
        initial_data = [
            {
                "email": "user1@example.com",
                "username": "user1",
                "name": "First User"
            },
            {
                "email": "user2@example.com", 
                "username": "user2",
                "name": "Second User"
            }
        ]

        self.insert_multiple_records(TEST_TABLE_NAME, initial_data)

        # Start replication
        self.start_replication()
        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=2)

        # Verify initial data
        self.verify_record_exists(TEST_TABLE_NAME, "email='user1@example.com'", {"name": "First User"})
        self.verify_record_exists(TEST_TABLE_NAME, "email='user2@example.com'", {"name": "Second User"})

        # Attempt to insert duplicate email (should be handled gracefully by replication)
        try:
            duplicate_data = [
                {
                    "email": "user1@example.com",  # Duplicate email
                    "username": "user1_new",
                    "name": "Duplicate User"
                }
            ]
            
            # This should fail in MySQL due to unique constraint
            self.mysql.execute(
                f"INSERT INTO `{TEST_TABLE_NAME}` (email, username, name) VALUES (%s, %s, %s)",
                (duplicate_data[0]["email"], duplicate_data[0]["username"], duplicate_data[0]["name"]),
                commit=True
            )
        except Exception as e:
            # Expected: MySQL should reject duplicate
            print(f"Expected MySQL duplicate rejection: {e}")

        # Verify replication is still working after duplicate attempt
        new_valid_data = [
            {
                "email": "user3@example.com",
                "username": "user3", 
                "name": "Third User"
            }
        ]

        self.insert_multiple_records(TEST_TABLE_NAME, new_valid_data)
        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=3)

        # Verify the new valid record made it through
        self.verify_record_exists(TEST_TABLE_NAME, "email='user3@example.com'", {"name": "Third User"})

        # Ensure original records remain unchanged
        self.verify_record_exists(TEST_TABLE_NAME, "email='user1@example.com'", {"name": "First User"})

    @pytest.mark.integration
    def test_duplicate_update_event_handling(self):
        """Test handling of duplicate UPDATE events"""
        # Create table for update testing
        self.mysql.execute(f"""
        CREATE TABLE `{TEST_TABLE_NAME}` (
            id int NOT NULL AUTO_INCREMENT,
            code varchar(50) UNIQUE,
            value varchar(255),
            last_modified timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            PRIMARY KEY (id)
        );
        """)

        # Insert initial data
        initial_data = [
            {"code": "ITEM_001", "value": "Initial Value 1"},
            {"code": "ITEM_002", "value": "Initial Value 2"}
        ]

        self.insert_multiple_records(TEST_TABLE_NAME, initial_data)

        # Start replication
        self.start_replication()
        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=2)

        # Perform multiple rapid updates (could create duplicate events in binlog)
        update_sequence = [
            ("ITEM_001", "Updated Value 1A"),
            ("ITEM_001", "Updated Value 1B"), 
            ("ITEM_001", "Updated Value 1C"),
            ("ITEM_002", "Updated Value 2A"),
            ("ITEM_002", "Updated Value 2B")
        ]

        for code, new_value in update_sequence:
            self.mysql.execute(
                f"UPDATE `{TEST_TABLE_NAME}` SET value = %s WHERE code = %s",
                (new_value, code),
                commit=True
            )
            time.sleep(0.1)  # Small delay to separate events

        # Wait for replication to process all updates
        self.wait_for_stable_state(TEST_TABLE_NAME, expected_count=2, wait_time=5)

        # Verify final state - should have the last update values
        self.verify_record_exists(TEST_TABLE_NAME, "code='ITEM_001'", {"value": "Updated Value 1C"})
        self.verify_record_exists(TEST_TABLE_NAME, "code='ITEM_002'", {"value": "Updated Value 2B"})

    @pytest.mark.integration
    def test_idempotent_operation_handling(self):
        """Test that replication operations are idempotent"""
        # Create table for idempotency testing
        self.mysql.execute(f"""
        CREATE TABLE `{TEST_TABLE_NAME}` (
            id int NOT NULL,
            name varchar(255),
            status varchar(50),
            PRIMARY KEY (id)
        );
        """)

        # Start replication
        self.start_replication()
        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=0)

        # Perform a series of operations
        operations = [
            ("INSERT", {"id": 1, "name": "Test Record", "status": "active"}),
            ("UPDATE", {"id": 1, "name": "Updated Record", "status": "active"}),
            ("UPDATE", {"id": 1, "name": "Updated Record", "status": "modified"}),
            ("DELETE", {"id": 1}),
            ("INSERT", {"id": 1, "name": "Recreated Record", "status": "new"})
        ]

        for operation, data in operations:
            if operation == "INSERT":
                self.mysql.execute(
                    f"INSERT INTO `{TEST_TABLE_NAME}` (id, name, status) VALUES (%s, %s, %s)",
                    (data["id"], data["name"], data["status"]),
                    commit=True
                )
            elif operation == "UPDATE":
                self.mysql.execute(
                    f"UPDATE `{TEST_TABLE_NAME}` SET name = %s, status = %s WHERE id = %s",
                    (data["name"], data["status"], data["id"]),
                    commit=True
                )
            elif operation == "DELETE":
                self.mysql.execute(
                    f"DELETE FROM `{TEST_TABLE_NAME}` WHERE id = %s",
                    (data["id"],),
                    commit=True
                )
            
            time.sleep(0.2)  # Allow replication to process

        # Wait for final state
        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=1)

        # Verify final state matches expected result
        self.verify_record_exists(
            TEST_TABLE_NAME, 
            "id=1", 
            {"name": "Recreated Record", "status": "new"}
        )

    @pytest.mark.integration
    def test_binlog_position_duplicate_handling(self):
        """Test handling of events from duplicate binlog positions"""
        # Create table for binlog position testing
        self.mysql.execute(f"""
        CREATE TABLE `{TEST_TABLE_NAME}` (
            id int NOT NULL AUTO_INCREMENT,
            data varchar(255),
            created_at timestamp DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (id)
        );
        """)

        # Start replication
        self.start_replication()
        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=0)

        # Insert data in a transaction to create batch of events
        self.mysql.execute("BEGIN")
        
        batch_data = [
            "Batch Record 1",
            "Batch Record 2", 
            "Batch Record 3",
            "Batch Record 4",
            "Batch Record 5"
        ]

        for data in batch_data:
            self.mysql.execute(
                f"INSERT INTO `{TEST_TABLE_NAME}` (data) VALUES (%s)",
                (data,)
            )

        self.mysql.execute("COMMIT", commit=True)

        # Wait for replication
        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=5)

        # Verify all records were processed correctly (no duplicates)
        ch_records = self.ch.select(TEST_TABLE_NAME, order_by="id")
        assert len(ch_records) == 5, f"Expected 5 records, got {len(ch_records)}"

        # Verify data integrity
        for i, expected_data in enumerate(batch_data):
            assert ch_records[i]["data"] == expected_data, (
                f"Data mismatch at position {i}: expected '{expected_data}', got '{ch_records[i]['data']}'"
            )

        # Verify no duplicate IDs exist
        id_values = [record["id"] for record in ch_records]
        assert len(id_values) == len(set(id_values)), "Duplicate IDs found in replicated data"