import os
import time

from mysql_ch_replicator import config
from mysql_ch_replicator import mysql_api
from mysql_ch_replicator import clickhouse_api

from common import *


CONFIG_FILE_TARGET_TABLES = 'tests/tests_config_target_tables.yaml'
TEST_DB_NAME = 'test_db'


def test_target_tables_initial_replication():
    cfg = config.Settings()
    cfg.load(CONFIG_FILE_TARGET_TABLES)
    
    mysql = mysql_api.MySQLApi(
        database=None,
        mysql_settings=cfg.mysql,
    )
    
    ch = clickhouse_api.ClickhouseApi(
        database=TEST_DB_NAME,
        clickhouse_settings=cfg.clickhouse,
    )
    
    if os.path.exists(cfg.binlog_replicator.data_dir):
        import shutil
        shutil.rmtree(cfg.binlog_replicator.data_dir)
    os.makedirs(cfg.binlog_replicator.data_dir, exist_ok=True)
    
    mysql.drop_database(TEST_DB_NAME)
    mysql.create_database(TEST_DB_NAME)
    mysql.set_database(TEST_DB_NAME)
    
    ch.drop_database(TEST_DB_NAME)
    assert_wait(lambda: TEST_DB_NAME not in ch.get_databases())
    
    mysql.execute('''
        CREATE TABLE users (
            id INT PRIMARY KEY,
            name VARCHAR(100) NOT NULL
        )
    ''')
    mysql.execute("INSERT INTO users (id, name) VALUES (1, 'Alice');", commit=True)
    mysql.execute("INSERT INTO users (id, name) VALUES (2, 'Bob');", commit=True)
    
    mysql.execute('''
        CREATE TABLE products (
            id INT PRIMARY KEY,
            name VARCHAR(100) NOT NULL
        )
    ''')
    mysql.execute("INSERT INTO products (id, name) VALUES (1, 'Widget');", commit=True)
    mysql.execute("INSERT INTO products (id, name) VALUES (2, 'Gadget');", commit=True)
    
    run_all_runner = RunAllRunner(cfg_file=CONFIG_FILE_TARGET_TABLES)
    run_all_runner.run()
    
    assert_wait(lambda: TEST_DB_NAME in ch.get_databases())
    ch.execute_command(f'USE `{TEST_DB_NAME}`')
    
    assert_wait(lambda: 'users_renamed' in ch.get_tables())
    assert_wait(lambda: 'products_renamed' in ch.get_tables())
    
    assert 'users' not in ch.get_tables()
    assert 'products' not in ch.get_tables()
    
    assert_wait(lambda: len(ch.select('users_renamed')) == 2)
    assert_wait(lambda: len(ch.select('products_renamed')) == 2)
    
    users_results = ch.select('users_renamed')
    users_ids = sorted([row['id'] for row in users_results])
    assert users_ids == [1, 2]
    
    products_results = ch.select('products_renamed')
    products_ids = sorted([row['id'] for row in products_results])
    assert products_ids == [1, 2]
    
    run_all_runner.stop()
    assert_wait(lambda: 'stopping db_replicator' in read_logs(TEST_DB_NAME))
    assert 'Traceback' not in read_logs(TEST_DB_NAME)


def test_target_tables_realtime_replication():
    cfg = config.Settings()
    cfg.load(CONFIG_FILE_TARGET_TABLES)
    
    mysql = mysql_api.MySQLApi(
        database=None,
        mysql_settings=cfg.mysql,
    )
    
    ch = clickhouse_api.ClickhouseApi(
        database=TEST_DB_NAME,
        clickhouse_settings=cfg.clickhouse,
    )
    
    if os.path.exists(cfg.binlog_replicator.data_dir):
        import shutil
        shutil.rmtree(cfg.binlog_replicator.data_dir)
    os.makedirs(cfg.binlog_replicator.data_dir, exist_ok=True)
    
    mysql.drop_database(TEST_DB_NAME)
    mysql.create_database(TEST_DB_NAME)
    mysql.set_database(TEST_DB_NAME)
    
    ch.drop_database(TEST_DB_NAME)
    assert_wait(lambda: TEST_DB_NAME not in ch.get_databases())
    
    mysql.execute('''
        CREATE TABLE users (
            id INT PRIMARY KEY,
            name VARCHAR(100) NOT NULL
        )
    ''')
    mysql.execute("INSERT INTO users (id, name) VALUES (1, 'Alice');", commit=True)
    
    run_all_runner = RunAllRunner(cfg_file=CONFIG_FILE_TARGET_TABLES)
    run_all_runner.run()
    
    assert_wait(lambda: TEST_DB_NAME in ch.get_databases())
    ch.execute_command(f'USE `{TEST_DB_NAME}`')
    assert_wait(lambda: 'users_renamed' in ch.get_tables())
    assert_wait(lambda: len(ch.select('users_renamed')) == 1)
    
    mysql.execute("INSERT INTO users (id, name) VALUES (2, 'Bob');", commit=True)
    assert_wait(lambda: len(ch.select('users_renamed')) == 2)
    
    mysql.execute("UPDATE users SET name = 'Bob Updated' WHERE id = 2;", commit=True)
    assert_wait(lambda: ch.select('users_renamed WHERE id = 2')[0]['name'] == 'Bob Updated')
    
    mysql.execute("DELETE FROM users WHERE id = 1;", commit=True)
    assert_wait(lambda: len(ch.select('users_renamed')) == 1)
    
    run_all_runner.stop()
    assert_wait(lambda: 'stopping db_replicator' in read_logs(TEST_DB_NAME))
    assert 'Traceback' not in read_logs(TEST_DB_NAME)


def test_target_tables_alter_operations():
    cfg = config.Settings()
    cfg.load(CONFIG_FILE_TARGET_TABLES)
    
    mysql = mysql_api.MySQLApi(
        database=None,
        mysql_settings=cfg.mysql,
    )
    
    ch = clickhouse_api.ClickhouseApi(
        database=TEST_DB_NAME,
        clickhouse_settings=cfg.clickhouse,
    )
    
    if os.path.exists(cfg.binlog_replicator.data_dir):
        import shutil
        shutil.rmtree(cfg.binlog_replicator.data_dir)
    os.makedirs(cfg.binlog_replicator.data_dir, exist_ok=True)
    
    mysql.drop_database(TEST_DB_NAME)
    mysql.create_database(TEST_DB_NAME)
    mysql.set_database(TEST_DB_NAME)
    
    ch.drop_database(TEST_DB_NAME)
    assert_wait(lambda: TEST_DB_NAME not in ch.get_databases())
    
    mysql.execute('''
        CREATE TABLE users (
            id INT PRIMARY KEY,
            name VARCHAR(100) NOT NULL
        )
    ''')
    mysql.execute("INSERT INTO users (id, name) VALUES (1, 'Alice');", commit=True)
    
    run_all_runner = RunAllRunner(cfg_file=CONFIG_FILE_TARGET_TABLES)
    run_all_runner.run()
    
    assert_wait(lambda: TEST_DB_NAME in ch.get_databases())
    ch.execute_command(f'USE `{TEST_DB_NAME}`')
    assert_wait(lambda: 'users_renamed' in ch.get_tables())
    assert_wait(lambda: len(ch.select('users_renamed')) == 1)
    
    mysql.execute("ALTER TABLE users ADD COLUMN age INT;", commit=True)
    time.sleep(2)
    
    mysql.execute("INSERT INTO users (id, name, age) VALUES (2, 'Bob', 30);", commit=True)
    assert_wait(lambda: len(ch.select('users_renamed')) == 2)
    
    users_result = ch.select('users_renamed WHERE id = 2')
    assert len(users_result) == 1
    assert users_result[0]['age'] == 30
    
    mysql.execute("ALTER TABLE users MODIFY COLUMN age BIGINT;", commit=True)
    time.sleep(2)
    
    mysql.execute("INSERT INTO users (id, name, age) VALUES (3, 'Charlie', 40);", commit=True)
    assert_wait(lambda: len(ch.select('users_renamed')) == 3)
    
    mysql.execute("ALTER TABLE users DROP COLUMN age;", commit=True)
    time.sleep(2)
    
    mysql.execute("INSERT INTO users (id, name) VALUES (4, 'David');", commit=True)
    assert_wait(lambda: len(ch.select('users_renamed')) == 4)
    
    run_all_runner.stop()
    assert_wait(lambda: 'stopping db_replicator' in read_logs(TEST_DB_NAME))
    assert 'Traceback' not in read_logs(TEST_DB_NAME)

