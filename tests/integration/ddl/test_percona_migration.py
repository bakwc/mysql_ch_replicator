"""Integration test for Percona pt-online-schema-change style migration"""

import pytest

from tests.base import BaseReplicationTest, DataTestMixin, SchemaTestMixin
from tests.conftest import TEST_DB_NAME, TEST_TABLE_NAME


class TestPerconaMigration(BaseReplicationTest, SchemaTestMixin, DataTestMixin):
    """Validate rename/copy flow used by pt-online-schema-change."""

    @pytest.mark.integration
    def test_pt_online_schema_change_flow(self):
        # Create base table and seed
        self.mysql.execute(
            f"""
            CREATE TABLE `{TEST_TABLE_NAME}` (
              `id` int NOT NULL,
              PRIMARY KEY (`id`)
            );
            """
        )
        self.insert_multiple_records(TEST_TABLE_NAME, [{"id": 42}])

        # Start replication
        self.start_replication(db_name=TEST_DB_NAME)
        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=1)

        # Create _new, alter it, backfill from old
        self.mysql.execute(
            f"""
            CREATE TABLE `{TEST_DB_NAME}`.`_{TEST_TABLE_NAME}_new` (
              `id` int NOT NULL,
              PRIMARY KEY (`id`)
            );
            """
        )
        self.mysql.execute(
            f"ALTER TABLE `{TEST_DB_NAME}`.`_{TEST_TABLE_NAME}_new` ADD COLUMN c1 INT;"
        )
        self.mysql.execute(
            f"""
            INSERT LOW_PRIORITY IGNORE INTO `{TEST_DB_NAME}`.`_{TEST_TABLE_NAME}_new` (`id`)
            SELECT `id` FROM `{TEST_DB_NAME}`.`{TEST_TABLE_NAME}` LOCK IN SHARE MODE;
            """,
            commit=True,
        )

        # Atomically rename
        self.mysql.execute(
            f"""
            RENAME TABLE `{TEST_DB_NAME}`.`{TEST_TABLE_NAME}` TO `{TEST_DB_NAME}`.`_{TEST_TABLE_NAME}_old`,
                         `{TEST_DB_NAME}`.`_{TEST_TABLE_NAME}_new` TO `{TEST_DB_NAME}`.`{TEST_TABLE_NAME}`;
            """
        )

        # Drop old
        self.mysql.execute(
            f"DROP TABLE IF EXISTS `{TEST_DB_NAME}`.`_{TEST_TABLE_NAME}_old`;"
        )

        # Verify table is usable after migration
        self.wait_for_table_sync(TEST_TABLE_NAME)  # structure change settles
        self.insert_multiple_records(TEST_TABLE_NAME, [{"id": 43, "c1": 1}])
        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=2)
        self.verify_record_exists(TEST_TABLE_NAME, "id=43", {"c1": 1})
