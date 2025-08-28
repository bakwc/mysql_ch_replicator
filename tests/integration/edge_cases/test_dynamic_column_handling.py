"""Integration test for dynamic column addition edge cases"""

import pytest
import yaml

from mysql_ch_replicator import clickhouse_api, mysql_api
from tests.conftest import (
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

    prepare_env(cfg, mysql, ch, db_name="test_replication", set_mysql_db=False)

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