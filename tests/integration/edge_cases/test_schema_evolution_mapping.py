"""Integration test for schema evolution with database mapping edge cases"""

import time

import pytest
import yaml

from mysql_ch_replicator import clickhouse_api, mysql_api
from tests.conftest import (
    TEST_DB_NAME,
    TEST_TABLE_NAME,
    BinlogReplicatorRunner,
    DbReplicatorRunner,
    assert_wait,
    prepare_env,
)


@pytest.mark.integration
def test_schema_evolution_with_db_mapping(clean_environment):
    """Test case to reproduce issue where schema evolution doesn't work with database mapping."""
    import tempfile
    import yaml
    import os
    
    cfg, mysql, ch = clean_environment
    
    # Load base config
    base_config_file = "tests/configs/replicator/tests_config_db_mapping.yaml"
    cfg.load(base_config_file)

    # Use the new dynamic configuration system for database isolation
    from tests.utils.dynamic_config import create_dynamic_config, get_config_manager
    
    # Create isolated target database name
    config_manager = get_config_manager()
    target_db_name = config_manager.get_isolated_target_database_name(TEST_DB_NAME, "mapped_target_db")
    
    # Create dynamic configuration with target database mapping
    config_file = create_dynamic_config(
        base_config_path=base_config_file,
        target_mappings={TEST_DB_NAME: target_db_name}
    )
    
    try:
        # Reload config from the temporary file
        cfg.load(config_file)

        # Note: Not setting a specific database in MySQL API
        mysql = mysql_api.MySQLApi(
            database=None,
            mysql_settings=cfg.mysql,
        )

        ch = clickhouse_api.ClickhouseApi(
            database=target_db_name,
            clickhouse_settings=cfg.clickhouse,
        )

        ch.drop_database(target_db_name)
        assert_wait(lambda: target_db_name not in ch.get_databases())

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
        assert_wait(lambda: target_db_name in ch.get_databases())
        ch.execute_command(f"USE `{target_db_name}`")
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
    
    finally:
        # Cleanup handled automatically by dynamic config system
        from tests.utils.dynamic_config import cleanup_config_files
        cleanup_config_files()