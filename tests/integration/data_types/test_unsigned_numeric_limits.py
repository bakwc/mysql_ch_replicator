"""Integration test for unsigned numeric limits and edge values"""

import pytest

from tests.base import BaseReplicationTest, DataTestMixin, SchemaTestMixin
from tests.conftest import TEST_DB_NAME, TEST_TABLE_NAME


class TestUnsignedNumericLimits(BaseReplicationTest, SchemaTestMixin, DataTestMixin):
    """Validate replication of extreme unsigned numeric values across types."""

    @pytest.mark.integration
    def test_unsigned_extremes(self):
        # Create table with a spread of numeric types
        self.mysql.execute(
            f"""
            CREATE TABLE `{TEST_TABLE_NAME}` (
                `id` int unsigned NOT NULL AUTO_INCREMENT,
                name varchar(255),
                test1 smallint,
                test2 smallint unsigned,
                test3 TINYINT,
                test4 TINYINT UNSIGNED,
                test5 MEDIUMINT UNSIGNED,
                test6 INT UNSIGNED,
                test7 BIGINT UNSIGNED,
                test8 MEDIUMINT UNSIGNED NULL,
                PRIMARY KEY (id)
            );
            """
        )

        # Insert edge-ish values
        self.mysql.execute(
            f"""
            INSERT INTO `{TEST_TABLE_NAME}` (name, test1, test2, test3, test4, test5, test6, test7, test8) VALUES
            ('Ivan', -20000, 50000, -30, 100, 16777200, 4294967290, 18446744073709551586, NULL);
            """,
            commit=True,
        )

        # Start replication
        self.start_replication(db_name=TEST_DB_NAME)
        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=1)

        # Second row
        self.mysql.execute(
            f"""
            INSERT INTO `{TEST_TABLE_NAME}` (name, test1, test2, test3, test4, test5, test6, test7, test8) VALUES
            ('Peter', -10000, 60000, -120, 250, 16777200, 4294967280, 18446744073709551586, NULL);
            """,
            commit=True,
        )

        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=2)

        # Validate selected points
        assert len(self.ch.select(TEST_TABLE_NAME, "test2=60000")) == 1
        assert len(self.ch.select(TEST_TABLE_NAME, "test4=250")) == 1
        assert len(self.ch.select(TEST_TABLE_NAME, "test5=16777200")) == 2
        assert len(self.ch.select(TEST_TABLE_NAME, "test6=4294967290")) == 1
        assert len(self.ch.select(TEST_TABLE_NAME, "test6=4294967280")) == 1
        assert len(self.ch.select(TEST_TABLE_NAME, "test7=18446744073709551586")) == 2
