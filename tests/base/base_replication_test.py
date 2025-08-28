"""Base test class for replication tests"""

import pytest

from tests.conftest import (
    CONFIG_FILE,
    TEST_DB_NAME,
    BinlogReplicatorRunner,
    DbReplicatorRunner,
    assert_wait,
)


class BaseReplicationTest:
    """Base class for all replication tests with common setup/teardown"""

    @pytest.fixture(autouse=True)
    def setup_replication_test(self, clean_environment):
        """Setup common to all replication tests"""
        self.cfg, self.mysql, self.ch = clean_environment
        self.config_file = getattr(self.cfg, "config_file", CONFIG_FILE)

        # Initialize runners as None - tests can create them as needed
        self.binlog_runner = None
        self.db_runner = None

        yield

        # Cleanup
        if self.db_runner:
            self.db_runner.stop()
        if self.binlog_runner:
            self.binlog_runner.stop()

    def start_replication(self, db_name=TEST_DB_NAME, config_file=None):
        """Start binlog and db replication with common setup"""
        config_file = config_file or self.config_file

        self.binlog_runner = BinlogReplicatorRunner(cfg_file=config_file)
        self.binlog_runner.run()

        self.db_runner = DbReplicatorRunner(db_name, cfg_file=config_file)
        self.db_runner.run()

        # Wait for replication to start
        assert_wait(lambda: db_name in self.ch.get_databases())
        self.ch.execute_command(f"USE `{db_name}`")

    def wait_for_table_sync(self, table_name, expected_count=None):
        """Wait for table to be synced to ClickHouse"""
        assert_wait(lambda: table_name in self.ch.get_tables())
        if expected_count is not None:
            assert_wait(lambda: len(self.ch.select(table_name)) == expected_count)

    def wait_for_data_sync(
        self, table_name, where_clause, expected_value=None, field="*"
    ):
        """Wait for specific data to be synced"""
        if expected_value is not None:
            if field == "*":
                assert_wait(
                    lambda: len(self.ch.select(table_name, where=where_clause)) > 0
                )
            else:
                assert_wait(
                    lambda: self.ch.select(table_name, where=where_clause)[0][field]
                    == expected_value
                )
        else:
            assert_wait(lambda: len(self.ch.select(table_name, where=where_clause)) > 0)
