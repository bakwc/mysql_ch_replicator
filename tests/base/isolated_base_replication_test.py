"""Isolated base test class for replication tests with path isolation"""

import pytest

from tests.base.base_replication_test import BaseReplicationTest


class IsolatedBaseReplicationTest(BaseReplicationTest):
    """Base class for replication tests with worker and test isolation"""

    @pytest.fixture(autouse=True)
    def setup_replication_test(self, isolated_clean_environment):
        """Setup common to all replication tests with isolation"""
        self.cfg, self.mysql, self.ch = isolated_clean_environment
        self.config_file = self.cfg.config_file

        # Initialize runners as None - tests can create them as needed
        self.binlog_runner = None
        self.db_runner = None

        yield

        # Cleanup
        if self.db_runner:
            self.db_runner.stop()
        if self.binlog_runner:
            self.binlog_runner.stop()