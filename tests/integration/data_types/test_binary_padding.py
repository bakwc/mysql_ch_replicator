"""Integration test for BINARY(N) fixed-length padding semantics"""

import pytest

from tests.base import BaseReplicationTest, DataTestMixin, SchemaTestMixin
from tests.conftest import TEST_DB_NAME, TEST_TABLE_NAME


class TestBinaryPadding(BaseReplicationTest, SchemaTestMixin, DataTestMixin):
    """Verify MySQL BINARY(N) pads with NULs and replicates consistently."""

    @pytest.mark.integration
    def test_binary_16_padding(self):
        # Table with BINARY(16) plus a boolean/key to filter
        self.mysql.execute(
            f"""
            CREATE TABLE `{TEST_TABLE_NAME}` (
                id INT NOT NULL AUTO_INCREMENT,
                flag TINYINT(1) NOT NULL,
                bin16 BINARY(16),
                PRIMARY KEY (id)
            );
            """
        )

        # Insert shorter payload that should be NUL-padded to 16 bytes
        # and another row with NULL to verify nullability
        self.insert_multiple_records(
            TEST_TABLE_NAME,
            [
                {"flag": 0, "bin16": "azaza"},
                {"flag": 1, "bin16": None},
            ],
        )

        # Start replication
        self.start_replication(db_name=TEST_DB_NAME)
        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=2)

        # Validate padded representation and NULL handling
        row0 = self.ch.select(TEST_TABLE_NAME, "flag=False")[0]
        row1 = self.ch.select(TEST_TABLE_NAME, "flag=True")[0]

        # Expect original content with trailing NULs to 16 bytes
        assert row0["bin16"] == "azaza\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"
        assert row1["bin16"] is None
