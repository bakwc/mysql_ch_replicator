"""Integration test for TRUNCATE operation bug (Issue #155)"""

import time

import pytest

from tests.conftest import (
    TEST_DB_NAME,
    TEST_TABLE_NAME,
    BinlogReplicatorRunner,
    DbReplicatorRunner,
    assert_wait,
)


@pytest.mark.integration
# @pytest.mark.skip(reason="Known bug - TRUNCATE operation not implemented")  # TRUNCATE is implemented - testing if it works
def test_truncate_operation_bug_issue_155(clean_environment):
    """
    Test to reproduce the bug from issue #155.

    Bug Description: TRUNCATE operation is not replicated - data is not cleared on ClickHouse side

    This test should FAIL until the bug is fixed.
    When the bug is present: TRUNCATE will not clear ClickHouse data and the test will FAIL
    When the bug is fixed: TRUNCATE will clear ClickHouse data and the test will PASS
    """
    cfg, mysql, ch = clean_environment

    # Create a test table
    mysql.execute(f"""
CREATE TABLE `{TEST_TABLE_NAME}` (
    id int NOT NULL AUTO_INCREMENT,
    name varchar(255),
    age int,
    PRIMARY KEY (id)
); 
    """)

    # Insert test data
    mysql.execute(
        f"INSERT INTO `{TEST_TABLE_NAME}` (name, age) VALUES ('Alice', 25);",
        commit=True,
    )
    mysql.execute(
        f"INSERT INTO `{TEST_TABLE_NAME}` (name, age) VALUES ('Bob', 30);", commit=True
    )
    mysql.execute(
        f"INSERT INTO `{TEST_TABLE_NAME}` (name, age) VALUES ('Charlie', 35);",
        commit=True,
    )

    # Start replication
    binlog_replicator_runner = BinlogReplicatorRunner()
    binlog_replicator_runner.run()
    db_replicator_runner = DbReplicatorRunner(TEST_DB_NAME)
    db_replicator_runner.run()

    # Wait for initial replication
    assert_wait(lambda: TEST_DB_NAME in ch.get_databases())
    ch.execute_command(f"USE `{TEST_DB_NAME}`")
    assert_wait(lambda: TEST_TABLE_NAME in ch.get_tables())
    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 3)

    # Verify data is replicated correctly
    mysql_count = len(mysql.fetch_all(f"SELECT * FROM `{TEST_TABLE_NAME}`"))
    assert mysql_count == 3

    ch_count = len(ch.select(TEST_TABLE_NAME))
    assert ch_count == 3

    # Execute TRUNCATE TABLE in MySQL
    mysql.execute(f"TRUNCATE TABLE `{TEST_TABLE_NAME}`;", commit=True)

    # Verify MySQL table is now empty
    mysql_count_after_truncate = len(mysql.fetch_all(f"SELECT * FROM `{TEST_TABLE_NAME}`"))
    assert mysql_count_after_truncate == 0, "MySQL table should be empty after TRUNCATE"

    # Wait for replication to process the TRUNCATE operation
    time.sleep(5)  # Give some time for the operation to be processed

    # This is where the bug manifests: ClickHouse table should be empty but it's not
    # When the bug is present, this assertion will FAIL because data is not cleared in ClickHouse
    ch_count_after_truncate = len(ch.select(TEST_TABLE_NAME))
    assert ch_count_after_truncate == 0, (
        f"ClickHouse table should be empty after TRUNCATE, but contains {ch_count_after_truncate} records"
    )

    # Insert new data to verify replication still works after TRUNCATE
    mysql.execute(
        f"INSERT INTO `{TEST_TABLE_NAME}` (name, age) VALUES ('Dave', 40);", commit=True
    )
    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 1)

    # Verify the new record
    new_record = ch.select(TEST_TABLE_NAME, where="name='Dave'")
    assert len(new_record) == 1
    assert new_record[0]["age"] == 40

    # Clean up
    db_replicator_runner.stop()
    binlog_replicator_runner.stop()