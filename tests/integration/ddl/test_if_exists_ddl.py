"""Integration test for IF [NOT] EXISTS DDL behavior"""

import pytest

from tests.base import BaseReplicationTest, DataTestMixin, SchemaTestMixin
from tests.conftest import TEST_DB_NAME


class TestIfExistsDdl(BaseReplicationTest, SchemaTestMixin, DataTestMixin):
    """Verify IF EXISTS / IF NOT EXISTS DDL statements replicate correctly."""

    @pytest.mark.integration
    def test_if_exists_if_not_exists(self):
        # Start replication first (schema operations will be observed live)
        self.start_replication(db_name=TEST_DB_NAME)

        # Create and drop using IF NOT EXISTS / IF EXISTS with qualified and unqualified names
        self.mysql.execute(
            """
            CREATE TABLE IF NOT EXISTS `test_table` (id int NOT NULL, PRIMARY KEY(id));
            """
        )
        self.mysql.execute(
            f"""
            CREATE TABLE IF NOT EXISTS `{TEST_DB_NAME}`.`test_table_2` (id int NOT NULL, PRIMARY KEY(id));
            """
        )

        self.mysql.execute(f"DROP TABLE IF EXISTS `{TEST_DB_NAME}`.`test_table`")
        self.mysql.execute("DROP TABLE IF EXISTS test_table")

        # Verify side effects in ClickHouse
        self.wait_for_table_sync("test_table_2", expected_count=0)
        assert "test_table" not in self.ch.get_tables()
