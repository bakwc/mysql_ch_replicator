"""Tests for log file rotation and logging management"""

import os
import time

import pytest

from tests.base import BaseReplicationTest, DataTestMixin, SchemaTestMixin
from tests.conftest import (
    TEST_DB_NAME,
    TEST_TABLE_NAME,
    RunAllRunner,
    read_logs,
)
from tests.fixtures import TableSchemas


class TestLogRotationManagement(BaseReplicationTest, SchemaTestMixin, DataTestMixin):
    """Test log file rotation and logging management scenarios"""

    @pytest.mark.integration
    def test_log_file_rotation(self):
        """Test that log file rotation doesn't break replication"""
        # Setup
        schema = TableSchemas.basic_user_table(TEST_TABLE_NAME)
        self.mysql.execute(schema.sql)

        self.insert_basic_record(TEST_TABLE_NAME, "LogTestUser", 30)

        # Start replication using the standard BaseReplicationTest method
        # This ensures proper configuration isolation is used
        self.start_replication()

        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=1)

        # Generate log activity by adding/updating data
        for i in range(10):
            self.insert_basic_record(TEST_TABLE_NAME, f"LogUser_{i}", 30 + i)
            if i % 3 == 0:
                self.update_record(
                    TEST_TABLE_NAME, f"name='LogUser_{i}'", {"age": 40 + i}
                )

        # Check logs exist and contain expected entries
        logs = read_logs(TEST_DB_NAME)
        assert len(logs) > 0, "No logs found"
        assert "replication" in logs.lower(), "No replication logs found"

        # Verify all data is still correctly replicated
        self.wait_for_table_sync(
            TEST_TABLE_NAME, expected_count=11
        )  # 1 initial + 10 new

        # Stop replication using the standard BaseReplicationTest method
        self.stop_replication()