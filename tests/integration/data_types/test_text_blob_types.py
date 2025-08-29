"""Tests for text and blob type replication"""

import pytest

from tests.base import BaseReplicationTest, DataTestMixin, SchemaTestMixin
from tests.conftest import TEST_TABLE_NAME


class TestTextBlobTypes(BaseReplicationTest, SchemaTestMixin, DataTestMixin):
    """Test replication of text and blob types"""

    @pytest.mark.integration
    def test_text_and_blob_types(self):
        """Test text and blob type handling"""
        # Create table with text and blob types
        self.mysql.execute(f"""
        CREATE TABLE `{TEST_TABLE_NAME}` (
            id int NOT NULL AUTO_INCREMENT,
            name varchar(255) NOT NULL,
            description text,
            content longtext,
            data_blob blob,
            large_data longblob,
            binary_data binary(16),
            variable_binary varbinary(255),
            PRIMARY KEY (id)
        );
        """)

        # Insert text and blob test data
        text_blob_data = [
            {
                "name": "Short Text",
                "description": "This is a short description",
                "content": "Short content for testing",
                "data_blob": b"Binary data test",
                "large_data": b"Large binary data for testing longblob",
                "binary_data": b"1234567890123456",  # Exactly 16 bytes
                "variable_binary": b"Variable length binary data"
            },
            {
                "name": "Long Text",
                "description": "This is a much longer description that tests the text data type capacity. " * 10,
                "content": "This is very long content that tests longtext capacity. " * 100,
                "data_blob": b"Larger binary data for blob testing" * 50,
                "large_data": b"Very large binary data for longblob testing" * 200,
                "binary_data": b"ABCDEFGHIJKLMNOP",  # Exactly 16 bytes
                "variable_binary": b"Different variable binary content"
            },
            {
                "name": "Empty/NULL Values",
                "description": "",  # Empty string
                "content": None,   # NULL value
                "data_blob": b"",  # Empty blob
                "large_data": None,  # NULL blob
                "binary_data": b"0000000000000000",  # Zero-filled 16 bytes
                "variable_binary": b""  # Empty varbinary
            }
        ]

        self.insert_multiple_records(TEST_TABLE_NAME, text_blob_data)

        # Start replication
        self.start_replication()
        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=3)

        # Verify text data
        self.verify_record_exists(
            TEST_TABLE_NAME, "name='Short Text'", 
            {"description": "This is a short description"}
        )

        # Verify blob data handling (check if record exists)
        self.verify_record_exists(
            TEST_TABLE_NAME, "name='Short Text' AND data_blob IS NOT NULL"
        )

        # Verify empty/NULL handling
        self.verify_record_exists(
            TEST_TABLE_NAME, "name='Empty/NULL Values' AND content IS NULL"
        )

        # Verify empty string vs NULL distinction
        self.verify_record_exists(
            TEST_TABLE_NAME, "name='Empty/NULL Values' AND description = ''"
        )