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

        # Verify initial rows
        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=4)
        rows = self.ch.select(TEST_TABLE_NAME)
        assert rows[0]["year_field"] == 2024
        assert rows[0]["nullable_year"] == 2024
        assert rows[1]["year_field"] == 1901
        assert rows[1]["nullable_year"] is None
        assert rows[2]["year_field"] == 2155
        assert rows[2]["nullable_year"] == 2000
        assert rows[3]["year_field"] == 2000
        assert rows[3]["nullable_year"] == 1999

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

        # Verify subset using ClickHouse filter
        newer = self.ch.select(
            TEST_TABLE_NAME, where="year_field >= 2025 ORDER BY year_field ASC"
        )
        assert len(newer) == 3
        assert newer[0]["year_field"] == 2025 and newer[0]["nullable_year"] == 2025
        assert newer[1]["year_field"] == 2100 and newer[1]["nullable_year"] == 2100
        assert newer[2]["year_field"] == 2155 and newer[2]["nullable_year"] == 2000
