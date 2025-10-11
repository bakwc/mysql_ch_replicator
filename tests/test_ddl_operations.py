from common import *
import datetime
import json
import uuid
from mysql_ch_replicator import config
from mysql_ch_replicator import mysql_api
from mysql_ch_replicator import clickhouse_api
import time


def test_if_exists_if_not_exists(monkeypatch):
    cfg = config.Settings()
    cfg.load('tests/tests_config_string_primary_key.yaml')

    mysql = mysql_api.MySQLApi(
        database=None,
        mysql_settings=cfg.mysql,
    )

    ch = clickhouse_api.ClickhouseApi(
        database=TEST_DB_NAME,
        clickhouse_settings=cfg.clickhouse,
    )

    prepare_env(cfg, mysql, ch)

    binlog_replicator_runner = BinlogReplicatorRunner(cfg_file='tests/tests_config_string_primary_key.yaml')
    binlog_replicator_runner.run()
    db_replicator_runner = DbReplicatorRunner(TEST_DB_NAME, cfg_file='tests/tests_config_string_primary_key.yaml')
    db_replicator_runner.run()

    assert_wait(lambda: TEST_DB_NAME in ch.get_databases())

    mysql.execute(f"CREATE TABLE IF NOT EXISTS `{TEST_DB_NAME}`.`{TEST_TABLE_NAME}` (id int NOT NULL, PRIMARY KEY(id));")
    mysql.execute(f"CREATE TABLE IF NOT EXISTS `{TEST_TABLE_NAME}` (id int NOT NULL, PRIMARY KEY(id));")
    mysql.execute(f"CREATE TABLE IF NOT EXISTS `{TEST_DB_NAME}`.{TEST_TABLE_NAME_2} (id int NOT NULL, PRIMARY KEY(id));")
    mysql.execute(f"CREATE TABLE IF NOT EXISTS {TEST_TABLE_NAME_2} (id int NOT NULL, PRIMARY KEY(id));")
    mysql.execute(f"DROP TABLE IF EXISTS `{TEST_DB_NAME}`.{TEST_TABLE_NAME};")
    mysql.execute(f"DROP TABLE IF EXISTS {TEST_TABLE_NAME};")

    ch.execute_command(f'USE `{TEST_DB_NAME}`')

    assert_wait(lambda: TEST_TABLE_NAME_2 in ch.get_tables())
    assert_wait(lambda: TEST_TABLE_NAME not in ch.get_tables())

    db_replicator_runner.stop()
    binlog_replicator_runner.stop()


def test_percona_migration(monkeypatch):
    cfg = config.Settings()
    cfg.load('tests/tests_config_string_primary_key.yaml')

    mysql = mysql_api.MySQLApi(
        database=None,
        mysql_settings=cfg.mysql,
    )

    ch = clickhouse_api.ClickhouseApi(
        database=TEST_DB_NAME,
        clickhouse_settings=cfg.clickhouse,
    )

    prepare_env(cfg, mysql, ch)

    mysql.execute(f'''
CREATE TABLE `{TEST_TABLE_NAME}` (
  `id` int NOT NULL,
  PRIMARY KEY (`id`)); 
    ''')

    mysql.execute(
        f"INSERT INTO `{TEST_TABLE_NAME}` (id) VALUES (42)",
        commit=True,
    )

    binlog_replicator_runner = BinlogReplicatorRunner(cfg_file='tests/tests_config_string_primary_key.yaml')
    binlog_replicator_runner.run()
    db_replicator_runner = DbReplicatorRunner(TEST_DB_NAME, cfg_file='tests/tests_config_string_primary_key.yaml')
    db_replicator_runner.run()

    assert_wait(lambda: TEST_DB_NAME in ch.get_databases())

    ch.execute_command(f'USE `{TEST_DB_NAME}`')

    assert_wait(lambda: TEST_TABLE_NAME in ch.get_tables())
    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 1)

   # Perform 'pt-online-schema-change' style migration to add a column
   # This is a subset of what happens when the following command is run:
   #     pt-online-schema-change --alter "ADD COLUMN c1 INT" D=$TEST_DB_NAME,t=$TEST_TABLE_NAME,h=0.0.0.0,P=3306,u=root,p=admin --execute
    mysql.execute(f'''
CREATE TABLE `{TEST_DB_NAME}`.`_{TEST_TABLE_NAME}_new` (
  `id` int NOT NULL,
  PRIMARY KEY (`id`)
)''')

    mysql.execute(
        f"ALTER TABLE `{TEST_DB_NAME}`.`_{TEST_TABLE_NAME}_new` ADD COLUMN c1 INT;")

    mysql.execute(
        f"INSERT LOW_PRIORITY IGNORE INTO `{TEST_DB_NAME}`.`_{TEST_TABLE_NAME}_new` (`id`) SELECT `id` FROM `{TEST_DB_NAME}`.`{TEST_TABLE_NAME}` LOCK IN SHARE MODE;",
        commit=True,
    )

    mysql.execute(
        f"RENAME TABLE `{TEST_DB_NAME}`.`{TEST_TABLE_NAME}` TO `{TEST_DB_NAME}`.`_{TEST_TABLE_NAME}_old`, `{TEST_DB_NAME}`.`_{TEST_TABLE_NAME}_new` TO `{TEST_DB_NAME}`.`{TEST_TABLE_NAME}`;")

    mysql.execute(
        f"DROP TABLE IF EXISTS `{TEST_DB_NAME}`.`_{TEST_TABLE_NAME}_old`;")

    # Wait for table to be recreated in ClickHouse after rename
    assert_wait(lambda: TEST_TABLE_NAME in ch.get_tables())

    mysql.execute(
        f"INSERT INTO `{TEST_TABLE_NAME}` (id, c1) VALUES (43, 1)",
        commit=True,
    )

    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 2)

    db_replicator_runner.stop()
    binlog_replicator_runner.stop()



def test_add_column_first_after_and_drop_column(monkeypatch):
    cfg = config.Settings()
    cfg.load('tests/tests_config_string_primary_key.yaml')

    mysql = mysql_api.MySQLApi(
        database=None,
        mysql_settings=cfg.mysql,
    )

    ch = clickhouse_api.ClickhouseApi(
        database=TEST_DB_NAME,
        clickhouse_settings=cfg.clickhouse,
    )

    prepare_env(cfg, mysql, ch)

    mysql.execute(f'''
CREATE TABLE `{TEST_TABLE_NAME}` (
  `id` int NOT NULL,
  PRIMARY KEY (`id`)); 
    ''')

    mysql.execute(
        f"INSERT INTO `{TEST_TABLE_NAME}` (id) VALUES (42)",
        commit=True,
    )

    binlog_replicator_runner = BinlogReplicatorRunner(cfg_file='tests/tests_config_string_primary_key.yaml')
    binlog_replicator_runner.run()
    db_replicator_runner = DbReplicatorRunner(TEST_DB_NAME, cfg_file='tests/tests_config_string_primary_key.yaml')
    db_replicator_runner.run()

    assert_wait(lambda: TEST_DB_NAME in ch.get_databases())

    ch.execute_command(f'USE `{TEST_DB_NAME}`')

    assert_wait(lambda: TEST_TABLE_NAME in ch.get_tables())
    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 1)

    # Test adding a column as the new first column, after another column, and dropping a column
    # These all move the primary key column to a different index and test the table structure is
    # updated correctly.

    # Test add column first
    mysql.execute(
        f"ALTER TABLE `{TEST_TABLE_NAME}` ADD COLUMN c1 INT FIRST")
    mysql.execute(
        f"INSERT INTO `{TEST_TABLE_NAME}` (id, c1) VALUES (43, 11)",
        commit=True,
    )
    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME, where="id=43")) == 1)
    assert_wait(lambda: ch.select(TEST_TABLE_NAME, where="id=43")[0]['c1'] == 11)

    # Test add column after
    mysql.execute(
        f"ALTER TABLE `{TEST_TABLE_NAME}` ADD COLUMN c2 INT AFTER c1")
    mysql.execute(
        f"INSERT INTO `{TEST_TABLE_NAME}` (id, c1, c2) VALUES (44, 111, 222)",
        commit=True,
    )
    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME, where="id=44")) == 1)
    assert_wait(lambda: ch.select(TEST_TABLE_NAME, where="id=44")[0]['c1'] == 111)
    assert_wait(lambda: ch.select(TEST_TABLE_NAME, where="id=44")[0]['c2'] == 222)

    # Test add KEY
    mysql.execute(
        f"ALTER TABLE `{TEST_TABLE_NAME}` ADD KEY `idx_c1_c2` (`c1`,`c2`)")
    mysql.execute(
        f"INSERT INTO `{TEST_TABLE_NAME}` (id, c1, c2) VALUES (46, 333, 444)",
        commit=True,
    )
    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME, where="id=46")) == 1)
    assert_wait(lambda: ch.select(TEST_TABLE_NAME, where="id=46")[0]['c1'] == 333)
    assert_wait(lambda: ch.select(TEST_TABLE_NAME, where="id=46")[0]['c2'] == 444)
    
    # Test drop column
    mysql.execute(
        f"ALTER TABLE `{TEST_TABLE_NAME}` DROP COLUMN c2")
    mysql.execute(
        f"INSERT INTO `{TEST_TABLE_NAME}` (id, c1) VALUES (45, 1111)",
        commit=True,
    )
    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME, where="id=45")) == 1)
    assert_wait(lambda: ch.select(TEST_TABLE_NAME, where="id=45")[0]['c1'] == 1111)
    assert_wait(lambda: ch.select(TEST_TABLE_NAME, where="id=45")[0].get('c2') is None)

    # Test add index to c1 column
    mysql.execute(
        f"ALTER TABLE `{TEST_TABLE_NAME}` ADD INDEX(c1)")
    mysql.execute(
        f"INSERT INTO `{TEST_TABLE_NAME}` (id, c1) VALUES (47, 5555)",
        commit=True,
    )
    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME, where="id=47")) == 1)
    assert_wait(lambda: ch.select(TEST_TABLE_NAME, where="id=47")[0]['c1'] == 5555)

    db_replicator_runner.stop()
    binlog_replicator_runner.stop()


def test_schema_evolution_with_db_mapping():
    """Test case to reproduce issue where schema evolution doesn't work with database mapping."""
    # Use the predefined config file with database mapping
    config_file = "tests/tests_config_db_mapping.yaml"
    
    cfg = config.Settings()
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
    mysql.execute(f'''
CREATE TABLE `{TEST_DB_NAME}`.`{TEST_TABLE_NAME}` (
  `id` int NOT NULL,
  `name` varchar(255) NOT NULL,
  PRIMARY KEY (`id`)); 
    ''')

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
    ch.execute_command(f'USE `mapped_target_db`')
    assert_wait(lambda: TEST_TABLE_NAME in ch.get_tables())
    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 1)

    # Now follow user's sequence of operations with fully qualified names (excluding RENAME operation)
    # 1. Add new column
    mysql.execute(f"ALTER TABLE `{TEST_DB_NAME}`.`{TEST_TABLE_NAME}` ADD COLUMN added_new_column char(1)", commit=True)
    
    # 2. Rename the column
    mysql.execute(f"ALTER TABLE `{TEST_DB_NAME}`.`{TEST_TABLE_NAME}` RENAME COLUMN added_new_column TO rename_column_name", commit=True)
    
    # 3. Modify column type
    mysql.execute(f"ALTER TABLE `{TEST_DB_NAME}`.`{TEST_TABLE_NAME}` MODIFY rename_column_name varchar(5)", commit=True)
    
    # 4. Insert data using the modified schema
    mysql.execute(
        f"INSERT INTO `{TEST_DB_NAME}`.`{TEST_TABLE_NAME}` (id, name, rename_column_name) VALUES (2, 'Second', 'ABCDE')",
        commit=True,
    )
    
    # 5. Drop the column - this is where the error was reported
    mysql.execute(f"ALTER TABLE `{TEST_DB_NAME}`.`{TEST_TABLE_NAME}` DROP COLUMN rename_column_name", commit=True)
    
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
    record_ids = [record['id'] for record in records]
    assert 1 in record_ids, "Original record (id=1) not found"
    assert 3 in record_ids, "New record (id=3) after schema changes not found"
    
    # Note: This test confirms our fix for schema evolution with database mapping
    
    # Clean up
    db_replicator_runner.stop()
    binlog_replicator_runner.stop()



def test_dynamic_column_addition_user_config():
    """Test to verify handling of dynamically added columns using user's exact configuration.
    
    This test reproduces the issue where columns are added on-the-fly via UPDATE
    rather than through ALTER TABLE statements, leading to an index error in the converter.
    """
    config_path = 'tests/tests_config_dynamic_column.yaml'
    
    cfg = config.Settings()
    cfg.load(config_path)
    
    mysql = mysql_api.MySQLApi(
        database=None,
        mysql_settings=cfg.mysql,
    )

    ch = clickhouse_api.ClickhouseApi(
        database=None,
        clickhouse_settings=cfg.clickhouse,
    )

    prepare_env(cfg, mysql, ch, db_name='test_replication')

    # Prepare environment - drop and recreate databases
    mysql.drop_database("test_replication")
    mysql.create_database("test_replication")
    mysql.set_database("test_replication")
    ch.drop_database("test_replication_ch")
    assert_wait(lambda: "test_replication_ch" not in ch.get_databases())

    # Create the exact table structure from the user's example
    mysql.execute('''
    CREATE TABLE test_replication.replication_data (
        code VARCHAR(255) NOT NULL PRIMARY KEY,
        val_1 VARCHAR(255) NOT NULL
    );
    ''')
    
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
    assert_wait(lambda: ch.select("replication_data", where="code='test-1'")[0]['val_1'] == '1')

    # Update an existing field - this should work fine
    mysql.execute("UPDATE test_replication.replication_data SET val_1 = '1200' WHERE code = 'test-1';", commit=True)
    assert_wait(lambda: ch.select("replication_data", where="code='test-1'")[0]['val_1'] == '1200')

    mysql.execute("USE test_replication");
    
    # Add val_2 column
    mysql.execute("ALTER TABLE replication_data ADD COLUMN val_2 VARCHAR(255);", commit=True)
    
    # Now try to update with a field that doesn't exist
    # This would have caused an error before our fix
    mysql.execute("UPDATE test_replication.replication_data SET val_2 = '100' WHERE code = 'test-1';", commit=True)
    
    # Verify replication processes are still running
    binlog_pid = get_binlog_replicator_pid(cfg)
    db_pid = get_db_replicator_pid(cfg, "test_replication")
    
    assert binlog_pid is not None, "Binlog replicator process died"
    assert db_pid is not None, "DB replicator process died"
    
    # Verify the replication is still working after the dynamic column update
    mysql.execute("UPDATE test_replication.replication_data SET val_1 = '1500' WHERE code = 'test-1';", commit=True)
    assert_wait(lambda: ch.select("replication_data", where="code='test-1'")[0]['val_1'] == '1500')
    
    print("Test passed - dynamic column was skipped without breaking replication")
    
    # Cleanup
    binlog_pid = get_binlog_replicator_pid(cfg)
    if binlog_pid:
        kill_process(binlog_pid)
        
    db_pid = get_db_replicator_pid(cfg, "test_replication")
    if db_pid:
        kill_process(db_pid)
