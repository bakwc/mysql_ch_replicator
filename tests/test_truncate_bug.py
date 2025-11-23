from common import *
from mysql_ch_replicator import config
from mysql_ch_replicator import mysql_api
from mysql_ch_replicator import clickhouse_api


def test_truncate_without_table_keyword_non_replicated_table():
    """Test TRUNCATE statement without TABLE keyword on non-replicated table.
    
    Reproduces bug #216 where TRUNCATE `table_name` (without TABLE keyword)
    breaks replication even when the table is not being replicated.
    """
    cfg = config.Settings()
    cfg.load('tests/tests_config_truncate_bug.yaml')

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
  `name` varchar(255),
  PRIMARY KEY (`id`)
); 
    ''')

    mysql.execute(f'''
CREATE TABLE `telescope_entries` (
  `id` int NOT NULL,
  `data` text,
  PRIMARY KEY (`id`)
); 
    ''')

    mysql.execute(
        f"INSERT INTO `{TEST_TABLE_NAME}` (id, name) VALUES (1, 'test')",
        commit=True,
    )

    mysql.execute(
        f"INSERT INTO `telescope_entries` (id, data) VALUES (1, 'entry1')",
        commit=True,
    )

    binlog_replicator_runner = BinlogReplicatorRunner(cfg_file='tests/tests_config_truncate_bug.yaml')
    binlog_replicator_runner.run()
    db_replicator_runner = DbReplicatorRunner(TEST_DB_NAME, cfg_file='tests/tests_config_truncate_bug.yaml')
    db_replicator_runner.run()

    assert_wait(lambda: TEST_DB_NAME in ch.get_databases())
    ch.execute_command(f'USE `{TEST_DB_NAME}`')
    assert_wait(lambda: TEST_TABLE_NAME in ch.get_tables())
    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 1)

    mysql.execute(f"TRUNCATE `telescope_entries`", commit=True)

    binlog_pid = get_binlog_replicator_pid(cfg)
    db_pid = get_db_replicator_pid(cfg, TEST_DB_NAME)
    
    assert binlog_pid is not None, "Binlog replicator process died"
    assert db_pid is not None, "DB replicator process died after TRUNCATE"

    mysql.execute(
        f"INSERT INTO `{TEST_TABLE_NAME}` (id, name) VALUES (2, 'test2')",
        commit=True,
    )
    
    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 2)

    db_replicator_runner.stop()
    binlog_replicator_runner.stop()


def test_truncate_with_table_keyword_non_replicated_table():
    """Test TRUNCATE TABLE statement on non-replicated table.
    
    This should work correctly as the TABLE keyword is properly handled.
    """
    cfg = config.Settings()
    cfg.load('tests/tests_config_truncate_bug.yaml')

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
  `name` varchar(255),
  PRIMARY KEY (`id`)
); 
    ''')

    mysql.execute(f'''
CREATE TABLE `telescope_entries` (
  `id` int NOT NULL,
  `data` text,
  PRIMARY KEY (`id`)
); 
    ''')

    mysql.execute(
        f"INSERT INTO `{TEST_TABLE_NAME}` (id, name) VALUES (1, 'test')",
        commit=True,
    )

    mysql.execute(
        f"INSERT INTO `telescope_entries` (id, data) VALUES (1, 'entry1')",
        commit=True,
    )

    binlog_replicator_runner = BinlogReplicatorRunner(cfg_file='tests/tests_config_truncate_bug.yaml')
    binlog_replicator_runner.run()
    db_replicator_runner = DbReplicatorRunner(TEST_DB_NAME, cfg_file='tests/tests_config_truncate_bug.yaml')
    db_replicator_runner.run()

    assert_wait(lambda: TEST_DB_NAME in ch.get_databases())
    ch.execute_command(f'USE `{TEST_DB_NAME}`')
    assert_wait(lambda: TEST_TABLE_NAME in ch.get_tables())
    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 1)

    mysql.execute(f"TRUNCATE TABLE `telescope_entries`", commit=True)

    binlog_pid = get_binlog_replicator_pid(cfg)
    db_pid = get_db_replicator_pid(cfg, TEST_DB_NAME)
    
    assert binlog_pid is not None, "Binlog replicator process died"
    assert db_pid is not None, "DB replicator process died after TRUNCATE TABLE"

    mysql.execute(
        f"INSERT INTO `{TEST_TABLE_NAME}` (id, name) VALUES (2, 'test2')",
        commit=True,
    )
    
    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 2)

    db_replicator_runner.stop()
    binlog_replicator_runner.stop()
