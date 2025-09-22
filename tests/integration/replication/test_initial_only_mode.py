"""Integration test for initial_only mode (non-performance)"""

import pytest

from tests.base import BaseReplicationTest, DataTestMixin, SchemaTestMixin
from tests.conftest import TEST_DB_NAME, TEST_TABLE_NAME, DbReplicatorRunner


class TestInitialOnlyMode(BaseReplicationTest, SchemaTestMixin, DataTestMixin):
    """Verify initial_only runs create schema and copy data, then exit cleanly."""

    @pytest.mark.integration
    def test_initial_only_replication(self):
        # Setup table and seed rows
        self.create_basic_table(TEST_TABLE_NAME)
        self.insert_multiple_records(
            TEST_TABLE_NAME,
            [
                {"name": "Ivan", "age": 42},
                {"name": "Peter", "age": 33},
            ],
        )

        # Run db replicator with initial_only flag
        db_replicator_runner = DbReplicatorRunner(
            TEST_DB_NAME, additional_arguments="--initial_only=True"
        )
        db_replicator_runner.run()
        db_replicator_runner.wait_complete()

        # Verify database and table copied
        assert TEST_DB_NAME in self.ch.get_databases()
        self.ch.execute_command(f"USE `{TEST_DB_NAME}`")
        assert TEST_TABLE_NAME in self.ch.get_tables()
        assert len(self.ch.select(TEST_TABLE_NAME)) == 2

        # Drop DB and rerun to ensure idempotency
        self.ch.execute_command(f"DROP DATABASE `{TEST_DB_NAME}`")

        db_replicator_runner = DbReplicatorRunner(
            TEST_DB_NAME, additional_arguments="--initial_only=True"
        )
        db_replicator_runner.run()
        db_replicator_runner.wait_complete()

        assert TEST_DB_NAME in self.ch.get_databases()

        db_replicator_runner.stop()
