"""Integration tests for replication edge cases and bug reproductions"""

import os
import tempfile
import time

import pytest
import yaml

from mysql_ch_replicator import clickhouse_api, mysql_api
from mysql_ch_replicator.db_replicator import State as DbReplicatorState
from tests.conftest import (
    CONFIG_FILE,
    TEST_DB_NAME,
    TEST_TABLE_NAME,
    BinlogReplicatorRunner,
    DbReplicatorRunner,
    assert_wait,
    get_binlog_replicator_pid,
    get_db_replicator_pid,
    kill_process,
    mysql_create_database,
    mysql_drop_database,
    prepare_env,
)


@pytest.mark.integration
def test_schema_evolution_with_db_mapping(clean_environment):
    """Test case to reproduce issue where schema evolution doesn't work with database mapping."""
    # Use the predefined config file with database mapping
    config_file = "tests/configs/replicator/tests_config_db_mapping.yaml"

    cfg, mysql, ch = clean_environment
    cfg.load(config_file)

    # Note: Not setting a specific database in MySQL API
    mysql = mysql_api.MySQLApi(
        database=None,
        mysql_settings=cfg.mysql,
    )

    ch = clickhouse_api.ClickhouseApi(
        database="mapped_target_db",
        clickhouse_settings=cfg.clickhouse,
    )

    ch.drop_database("mapped_target_db")
    assert_wait(lambda: "mapped_target_db" not in ch.get_databases())

    prepare_env(cfg, mysql, ch, db_name=TEST_DB_NAME)

    # Create a test table with some columns using fully qualified name
    mysql.execute(f"""
CREATE TABLE `{TEST_DB_NAME}`.`{TEST_TABLE_NAME}` (
  `id` int NOT NULL,
  `name` varchar(255) NOT NULL,
  PRIMARY KEY (`id`)); 
    """)

    mysql.execute(
        f"INSERT INTO `{TEST_DB_NAME}`.`{TEST_TABLE_NAME}` (id, name) VALUES (1, 'Original')",
        commit=True,
    )

    # Start the replication
    binlog_replicator_runner = BinlogReplicatorRunner(cfg_file=config_file)
    binlog_replicator_runner.run()
    db_replicator_runner = DbReplicatorRunner(TEST_DB_NAME, cfg_file=config_file)
    db_replicator_runner.run()

    # Make sure initial replication works with the database mapping
    assert_wait(lambda: "mapped_target_db" in ch.get_databases())
    ch.execute_command("USE `mapped_target_db`")
    assert_wait(lambda: TEST_TABLE_NAME in ch.get_tables())
    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 1)

    # Now follow user's sequence of operations with fully qualified names (excluding RENAME operation)
    # 1. Add new column
    mysql.execute(
        f"ALTER TABLE `{TEST_DB_NAME}`.`{TEST_TABLE_NAME}` ADD COLUMN added_new_column char(1)",
        commit=True,
    )

    # 2. Rename the column
    mysql.execute(
        f"ALTER TABLE `{TEST_DB_NAME}`.`{TEST_TABLE_NAME}` RENAME COLUMN added_new_column TO rename_column_name",
        commit=True,
    )

    # 3. Modify column type
    mysql.execute(
        f"ALTER TABLE `{TEST_DB_NAME}`.`{TEST_TABLE_NAME}` MODIFY rename_column_name varchar(5)",
        commit=True,
    )

    # 4. Insert data using the modified schema
    mysql.execute(
        f"INSERT INTO `{TEST_DB_NAME}`.`{TEST_TABLE_NAME}` (id, name, rename_column_name) VALUES (2, 'Second', 'ABCDE')",
        commit=True,
    )

    # 5. Drop the column - this is where the error was reported
    mysql.execute(
        f"ALTER TABLE `{TEST_DB_NAME}`.`{TEST_TABLE_NAME}` DROP COLUMN rename_column_name",
        commit=True,
    )

    # 6. Add more inserts after schema changes to verify ongoing replication
    mysql.execute(
        f"INSERT INTO `{TEST_DB_NAME}`.`{TEST_TABLE_NAME}` (id, name) VALUES (3, 'Third record after drop column')",
        commit=True,
    )

    # Check if all changes were replicated correctly
    time.sleep(5)  # Allow time for processing the changes
    result = ch.select(TEST_TABLE_NAME)
    print(f"ClickHouse table contents: {result}")

    # Verify all records are present
    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 3)

    # Verify specific records exist
    records = ch.select(TEST_TABLE_NAME)
    print(f"Record type: {type(records[0])}")  # Debug the record type

    # Access by field name 'id' instead of by position
    record_ids = [record["id"] for record in records]
    assert 1 in record_ids, "Original record (id=1) not found"
    assert 3 in record_ids, "New record (id=3) after schema changes not found"

    # Note: This test confirms our fix for schema evolution with database mapping

    # Clean up
    db_replicator_runner.stop()
    binlog_replicator_runner.stop()


@pytest.mark.integration
def test_dynamic_column_addition_user_config(clean_environment):
    """Test to verify handling of dynamically added columns using user's exact configuration.

    This test reproduces the issue where columns are added on-the-fly via UPDATE
    rather than through ALTER TABLE statements, leading to an index error in the converter.
    """
    config_path = "tests/configs/replicator/tests_config_dynamic_column.yaml"

    cfg, mysql, ch = clean_environment
    cfg.load(config_path)

    mysql = mysql_api.MySQLApi(
        database=None,
        mysql_settings=cfg.mysql,
    )

    ch = clickhouse_api.ClickhouseApi(
        database=None,
        clickhouse_settings=cfg.clickhouse,
    )

    prepare_env(cfg, mysql, ch, db_name="test_replication")

    # Prepare environment - drop and recreate databases
    mysql_drop_database(mysql, "test_replication")
    mysql_create_database(mysql, "test_replication")
    mysql.set_database("test_replication")
    ch.drop_database("test_replication_ch")
    assert_wait(lambda: "test_replication_ch" not in ch.get_databases())

    # Create the exact table structure from the user's example
    mysql.execute("""
    CREATE TABLE test_replication.replication_data (
        code VARCHAR(255) NOT NULL PRIMARY KEY,
        val_1 VARCHAR(255) NOT NULL
    );
    """)

    # Insert initial data
    mysql.execute(
        "INSERT INTO test_replication.replication_data(code, val_1) VALUE ('test-1', '1');",
        commit=True,
    )

    # Start the replication processes
    binlog_replicator_runner = BinlogReplicatorRunner(cfg_file=config_path)
    binlog_replicator_runner.run()
    db_replicator_runner = DbReplicatorRunner("test_replication", cfg_file=config_path)
    db_replicator_runner.run()

    # Wait for initial replication to complete
    assert_wait(lambda: "test_replication_ch" in ch.get_databases())

    # Set the database before checking tables
    ch.execute_command("USE test_replication_ch")
    assert_wait(lambda: "replication_data" in ch.get_tables())
    assert_wait(lambda: len(ch.select("replication_data")) == 1)

    # Verify initial data was replicated correctly
    assert_wait(
        lambda: ch.select("replication_data", where="code='test-1'")[0]["val_1"] == "1"
    )

    # Update an existing field - this should work fine
    mysql.execute(
        "UPDATE test_replication.replication_data SET val_1 = '1200' WHERE code = 'test-1';",
        commit=True,
    )
    assert_wait(
        lambda: ch.select("replication_data", where="code='test-1'")[0]["val_1"]
        == "1200"
    )

    mysql.execute("USE test_replication")

    # Add val_2 column
    mysql.execute(
        "ALTER TABLE replication_data ADD COLUMN val_2 VARCHAR(255);", commit=True
    )

    # Now try to update with a field that doesn't exist
    # This would have caused an error before our fix
    mysql.execute(
        "UPDATE test_replication.replication_data SET val_2 = '100' WHERE code = 'test-1';",
        commit=True,
    )

    # Verify replication processes are still running
    binlog_pid = get_binlog_replicator_pid(cfg)
    db_pid = get_db_replicator_pid(cfg, "test_replication")

    assert binlog_pid is not None, "Binlog replicator process died"
    assert db_pid is not None, "DB replicator process died"

    # Verify the replication is still working after the dynamic column update
    mysql.execute(
        "UPDATE test_replication.replication_data SET val_1 = '1500' WHERE code = 'test-1';",
        commit=True,
    )
    assert_wait(
        lambda: ch.select("replication_data", where="code='test-1'")[0]["val_1"]
        == "1500"
    )

    print("Test passed - dynamic column was skipped without breaking replication")

    # Cleanup
    binlog_pid = get_binlog_replicator_pid(cfg)
    if binlog_pid:
        kill_process(binlog_pid)

    db_pid = get_db_replicator_pid(cfg, "test_replication")
    if db_pid:
        kill_process(db_pid)


@pytest.mark.integration
def test_resume_initial_replication_with_ignore_deletes(clean_environment):
    """
    Test that resuming initial replication works correctly with ignore_deletes=True.

    This reproduces the bug from https://github.com/bakwc/mysql_ch_replicator/issues/172
    where resuming initial replication would fail with "Database sirocco_tmp does not exist"
    when ignore_deletes=True because the code would try to use the _tmp database instead
    of the target database directly.
    """
    # Create a temporary config file with ignore_deletes=True
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".yaml", delete=False
    ) as temp_config_file:
        config_file = temp_config_file.name

        # Read the original config
        with open(CONFIG_FILE, "r") as original_config:
            config_data = yaml.safe_load(original_config)

        # Add ignore_deletes=True
        config_data["ignore_deletes"] = True

        # Set initial_replication_batch_size to 1 for testing
        config_data["initial_replication_batch_size"] = 1

        # Write to the temp file
        yaml.dump(config_data, temp_config_file)

    try:
        cfg, mysql, ch = clean_environment
        cfg.load(config_file)

        # Verify the ignore_deletes option was set
        assert cfg.ignore_deletes is True

        # Create a table with many records to ensure initial replication takes time
        mysql.execute(f"""
        CREATE TABLE `{TEST_TABLE_NAME}` (
            id int NOT NULL AUTO_INCREMENT,
            name varchar(255),
            data varchar(1000),
            PRIMARY KEY (id)
        )
        """)

        # Insert many records to make initial replication take longer
        for i in range(100):
            mysql.execute(
                f"INSERT INTO `{TEST_TABLE_NAME}` (name, data) VALUES ('test_{i}', 'data_{i}');",
                commit=True,
            )

        # Start binlog replicator
        binlog_replicator_runner = BinlogReplicatorRunner(cfg_file=config_file)
        binlog_replicator_runner.run()

        # Start db replicator for initial replication with test flag to exit early
        db_replicator_runner = DbReplicatorRunner(
            TEST_DB_NAME,
            cfg_file=config_file,
            additional_arguments="--initial-replication-test-fail-records 30",
        )
        db_replicator_runner.run()

        # Wait for initial replication to start
        assert_wait(lambda: TEST_DB_NAME in ch.get_databases())
        ch.execute_command(f"USE `{TEST_DB_NAME}`")
        assert_wait(lambda: TEST_TABLE_NAME in ch.get_tables())

        # Wait for some records to be replicated but not all (should hit the 30 record limit)
        assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) > 0)

        # The db replicator should have stopped automatically due to the test flag
        # But we still call stop() to ensure proper cleanup
        db_replicator_runner.stop()

        # Verify the state is still PERFORMING_INITIAL_REPLICATION
        state_path = os.path.join(
            cfg.binlog_replicator.data_dir, TEST_DB_NAME, "state.pckl"
        )
        state = DbReplicatorState(state_path)
        assert state.status.value == 2  # PERFORMING_INITIAL_REPLICATION

        # Add more records while replication is stopped
        for i in range(100, 150):
            mysql.execute(
                f"INSERT INTO `{TEST_TABLE_NAME}` (name, data) VALUES ('test_{i}', 'data_{i}');",
                commit=True,
            )

        # Verify that sirocco_tmp database does NOT exist (it should use sirocco directly)
        assert f"{TEST_DB_NAME}_tmp" not in ch.get_databases(), (
            "Temporary database should not exist with ignore_deletes=True"
        )

        # Resume initial replication - this should NOT fail with "Database sirocco_tmp does not exist"
        db_replicator_runner_2 = DbReplicatorRunner(TEST_DB_NAME, cfg_file=config_file)
        db_replicator_runner_2.run()

        # Wait for all records to be replicated (100 original + 50 extra = 150)
        assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 150, max_wait_time=30)

        # Verify the replication completed successfully
        records = ch.select(TEST_TABLE_NAME)
        assert len(records) == 150, f"Expected 150 records, got {len(records)}"

        # Verify we can continue with realtime replication
        mysql.execute(
            f"INSERT INTO `{TEST_TABLE_NAME}` (name, data) VALUES ('realtime_test', 'realtime_data');",
            commit=True,
        )
        assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 151)

        # Clean up
        db_replicator_runner_2.stop()
        binlog_replicator_runner.stop()

    finally:
        # Clean up temp config file
        os.unlink(config_file)


@pytest.mark.integration
@pytest.mark.skip(reason="Known bug - TRUNCATE operation not implemented")
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
    mysql.execute(f"SELECT COUNT(*) FROM `{TEST_TABLE_NAME}`")
    mysql_count = mysql.cursor.fetchall()[0][0]
    assert mysql_count == 3

    ch_count = len(ch.select(TEST_TABLE_NAME))
    assert ch_count == 3

    # Execute TRUNCATE TABLE in MySQL
    mysql.execute(f"TRUNCATE TABLE `{TEST_TABLE_NAME}`;", commit=True)

    # Verify MySQL table is now empty
    mysql.execute(f"SELECT COUNT(*) FROM `{TEST_TABLE_NAME}`")
    mysql_count_after_truncate = mysql.cursor.fetchall()[0][0]
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
