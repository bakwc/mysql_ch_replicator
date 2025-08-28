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

        # Insert edge-case unsigned numeric values using helper method
        test_data = [
            {
                "name": "Ivan",
                "test1": -20000,  # smallint signed
                "test2": 50000,   # smallint unsigned
                "test3": -30,     # tinyint signed
                "test4": 100,     # tinyint unsigned
                "test5": 16777200, # mediumint unsigned
                "test6": 4294967290, # int unsigned
                "test7": 18446744073709551586, # bigint unsigned
                "test8": None,    # mediumint unsigned NULL
            }
        ]
        self.insert_multiple_records(TEST_TABLE_NAME, test_data)

        # Start replication
        self.start_replication(db_name=TEST_DB_NAME)
        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=1)

        # Insert second row with different edge values
        additional_data = [
            {
                "name": "Peter",
                "test1": -10000,  # smallint signed
                "test2": 60000,   # smallint unsigned
                "test3": -120,    # tinyint signed
                "test4": 250,     # tinyint unsigned
                "test5": 16777200, # mediumint unsigned (same as first)
                "test6": 4294967280, # int unsigned
                "test7": 18446744073709551586, # bigint unsigned (same as first)
                "test8": None,    # mediumint unsigned NULL
            }
        ]
        self.insert_multiple_records(TEST_TABLE_NAME, additional_data)

        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=2)

        # Validate unsigned numeric limits using helper methods
        self.verify_record_exists(TEST_TABLE_NAME, "name='Ivan'", {
            "test1": -20000,
            "test2": 50000,
            "test3": -30,
            "test4": 100,
            "test5": 16777200,
            "test6": 4294967290,
            "test7": 18446744073709551586
        })
        
        self.verify_record_exists(TEST_TABLE_NAME, "name='Peter'", {
            "test1": -10000,
            "test2": 60000,
            "test3": -120,
            "test4": 250,
            "test5": 16777200,
            "test6": 4294967280,
            "test7": 18446744073709551586
        })
        
        # Verify NULL handling for unsigned types
        self.verify_record_exists(TEST_TABLE_NAME, "name='Ivan' AND test8 IS NULL")
        self.verify_record_exists(TEST_TABLE_NAME, "name='Peter' AND test8 IS NULL")
