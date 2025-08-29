"""Integration test for ENUM normalization and zero-value semantics"""

import pytest

from tests.base import BaseReplicationTest, DataTestMixin, SchemaTestMixin
from tests.conftest import TEST_DB_NAME, TEST_TABLE_NAME


class TestEnumNormalization(BaseReplicationTest, SchemaTestMixin, DataTestMixin):
    """Verify ENUM values normalize to lowercase and handle NULL/zero values properly."""

    @pytest.mark.integration
    def test_enum_lowercase_and_zero(self):
        # Create table with two ENUM columns
        self.mysql.execute(
            f"""
            CREATE TABLE `{TEST_TABLE_NAME}` (
                id INT NOT NULL AUTO_INCREMENT,
                status_mixed_case ENUM('Purchase','Sell','Transfer') NOT NULL,
                status_empty ENUM('Yes','No','Maybe'),
                PRIMARY KEY (id)
            );
            """
        )

        # Seed records with mixed case and NULLs
        self.insert_multiple_records(
            TEST_TABLE_NAME,
            [
                {"status_mixed_case": "Purchase", "status_empty": "Yes"},
                {"status_mixed_case": "Sell", "status_empty": None},
                {"status_mixed_case": "Transfer", "status_empty": None},
            ],
        )

        # Start replication
        self.start_replication()

        # Verify ENUM normalization and NULL handling using helper methods
        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=3)
        
        # Verify ENUM values are normalized to lowercase during replication
        self.verify_record_exists(TEST_TABLE_NAME, "id=1", {
            "status_mixed_case": "purchase",  # 'Purchase' → 'purchase'
            "status_empty": "yes"           # 'Yes' → 'yes'
        })
        
        self.verify_record_exists(TEST_TABLE_NAME, "id=2", {
            "status_mixed_case": "sell"      # 'Sell' → 'sell'
        })
        self.verify_record_exists(TEST_TABLE_NAME, "id=2 AND status_empty IS NULL")
        
        self.verify_record_exists(TEST_TABLE_NAME, "id=3", {
            "status_mixed_case": "transfer"  # 'Transfer' → 'transfer'
        })
        self.verify_record_exists(TEST_TABLE_NAME, "id=3 AND status_empty IS NULL")
