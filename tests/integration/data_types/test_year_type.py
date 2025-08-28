"""Integration test for MySQL YEAR type mapping to ClickHouse UInt16"""

import pytest

from tests.base import BaseReplicationTest, DataTestMixin, SchemaTestMixin
from tests.conftest import TEST_DB_NAME, TEST_TABLE_NAME


class TestYearType(BaseReplicationTest, SchemaTestMixin, DataTestMixin):
    """Verify YEAR columns replicate correctly."""

    @pytest.mark.integration
    def test_year_type_mapping(self):
        # Create table with YEAR columns
        self.mysql.execute(
            f"""
            CREATE TABLE `{TEST_TABLE_NAME}` (
                id INT NOT NULL AUTO_INCREMENT,
                year_field YEAR NOT NULL,
                nullable_year YEAR,
                PRIMARY KEY (id)
            );
            """
        )

        # Seed rows covering min/max and NULL
        self.insert_multiple_records(
            TEST_TABLE_NAME,
            [
                {"year_field": 2024, "nullable_year": 2024},
                {"year_field": 1901, "nullable_year": None},
                {"year_field": 2155, "nullable_year": 2000},
                {"year_field": 2000, "nullable_year": 1999},
            ],
        )

        # Start replication
        self.start_replication(db_name=TEST_DB_NAME)

        # Verify initial YEAR type replication using helper methods
        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=4)
        
        # Verify specific YEAR values with expected data types
        self.verify_record_exists(TEST_TABLE_NAME, "id=1", {
            "year_field": 2024,
            "nullable_year": 2024
        })
        
        self.verify_record_exists(TEST_TABLE_NAME, "id=2", {
            "year_field": 1901  # MIN YEAR value
        })
        self.verify_record_exists(TEST_TABLE_NAME, "id=2 AND nullable_year IS NULL")
        
        self.verify_record_exists(TEST_TABLE_NAME, "id=3", {
            "year_field": 2155,  # MAX YEAR value
            "nullable_year": 2000
        })
        
        self.verify_record_exists(TEST_TABLE_NAME, "id=4", {
            "year_field": 2000,
            "nullable_year": 1999
        })

        # Realtime inserts
        self.insert_multiple_records(
            TEST_TABLE_NAME,
            [
                {"year_field": 2025, "nullable_year": 2025},
                {"year_field": 1999, "nullable_year": None},
                {"year_field": 2100, "nullable_year": 2100},
            ],
        )

        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=7)

        # Verify realtime YEAR insertions using helper methods
        self.verify_record_exists(TEST_TABLE_NAME, "year_field=2025", {
            "year_field": 2025,
            "nullable_year": 2025
        })
        
        self.verify_record_exists(TEST_TABLE_NAME, "year_field=1999", {
            "year_field": 1999
        })
        self.verify_record_exists(TEST_TABLE_NAME, "year_field=1999 AND nullable_year IS NULL")
        
        self.verify_record_exists(TEST_TABLE_NAME, "year_field=2100", {
            "year_field": 2100,
            "nullable_year": 2100
        })
        
        # Verify total count includes all YEAR boundary values (1901-2155)
        self.verify_record_exists(TEST_TABLE_NAME, "year_field=2155")
        self.verify_record_exists(TEST_TABLE_NAME, "year_field=1901")
