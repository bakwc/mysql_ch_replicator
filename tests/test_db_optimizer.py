import os
import shutil

from mysql_ch_replicator import config
from mysql_ch_replicator import mysql_api
from mysql_ch_replicator import clickhouse_api
from mysql_ch_replicator.clickhouse_api import is_optimizable_table_engine
from mysql_ch_replicator.db_optimizer import DbOptimizer

from common import *


CONFIG_FILE_DB_OPTIMIZER = 'tests/tests_config_db_optimizer.yaml'
CH_TARGET_DB = 'mapped_target_db'
OPTIMIZER_VIEW_NAME = 'optimizer_test_view'


def test_is_optimizable_table_engine():
    assert is_optimizable_table_engine('ReplacingMergeTree') is True
    assert is_optimizable_table_engine('MergeTree') is True
    assert is_optimizable_table_engine('View') is False
    assert is_optimizable_table_engine('MaterializedView') is False
    assert is_optimizable_table_engine('LiveView') is False
    assert is_optimizable_table_engine('WindowView') is False


def test_db_optimizer_skips_view_and_uses_target_database():
    cfg = config.Settings()
    cfg.load(CONFIG_FILE_DB_OPTIMIZER)

    mysql = mysql_api.MySQLApi(
        database=None,
        mysql_settings=cfg.mysql,
    )

    ch = clickhouse_api.ClickhouseApi(
        database=CH_TARGET_DB,
        clickhouse_settings=cfg.clickhouse,
    )

    if os.path.exists(cfg.binlog_replicator.data_dir):
        shutil.rmtree(cfg.binlog_replicator.data_dir)
    os.makedirs(cfg.binlog_replicator.data_dir, exist_ok=True)

    mysql.drop_database(TEST_DB_NAME)
    mysql.create_database(TEST_DB_NAME)
    ch.drop_database(CH_TARGET_DB)
    assert_wait(lambda: CH_TARGET_DB not in ch.get_databases())

    mysql.set_database(TEST_DB_NAME)
    mysql.execute(f'''
        CREATE TABLE `{TEST_TABLE_NAME}` (
            id INT PRIMARY KEY,
            name VARCHAR(100) NOT NULL
        )
    ''')
    mysql.execute(f"INSERT INTO `{TEST_TABLE_NAME}` (id, name) VALUES (1, 'Alice');", commit=True)
    mysql.execute(f"INSERT INTO `{TEST_TABLE_NAME}` (id, name) VALUES (2, 'Bob');", commit=True)

    run_all_runner = RunAllRunner(cfg_file=CONFIG_FILE_DB_OPTIMIZER)
    run_all_runner.run()

    assert_wait(lambda: CH_TARGET_DB in ch.get_databases())
    ch.execute_command(f'USE `{CH_TARGET_DB}`')
    assert_wait(lambda: TEST_TABLE_NAME in ch.get_tables())
    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 2)

    ch.execute_command(
        f'CREATE VIEW `{OPTIMIZER_VIEW_NAME}` AS SELECT id, name FROM `{TEST_TABLE_NAME}`',
    )
    assert_wait(lambda: OPTIMIZER_VIEW_NAME in ch.get_tables())

    optimizable_tables = ch.get_optimizable_tables(CH_TARGET_DB)
    assert TEST_TABLE_NAME in optimizable_tables
    assert OPTIMIZER_VIEW_NAME not in optimizable_tables

    optimizer = DbOptimizer(cfg)
    assert optimizer.get_clickhouse_database(TEST_DB_NAME) == CH_TARGET_DB

    run_all_runner.stop()
    assert_wait(lambda: 'stopping db_replicator' in read_logs(TEST_DB_NAME))
    assert 'Traceback' not in read_logs(TEST_DB_NAME)

    db_optimizer_runner = DbOptimizerRunner(cfg_file=CONFIG_FILE_DB_OPTIMIZER)
    db_optimizer_runner.run()

    assert_wait(
        lambda: f'Optimizing table {CH_TARGET_DB}.{TEST_TABLE_NAME}' in read_db_optimizer_logs(cfg),
        max_wait_time=30,
    )
    optimizer_logs = read_db_optimizer_logs(cfg)
    assert 'Traceback' not in optimizer_logs
    assert f'Optimizing table {CH_TARGET_DB}.{OPTIMIZER_VIEW_NAME}' not in optimizer_logs

    db_optimizer_runner.stop()
