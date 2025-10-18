import os

from mysql_ch_replicator import config
from mysql_ch_replicator import mysql_api
from mysql_ch_replicator import clickhouse_api

from common import *


CONFIG_FILE_MULTI_MYSQL = 'tests/tests_config_multi_mysql_single_ch.yaml'
TEST_DB_NAME_1 = 'replication-test_db'
TEST_DB_NAME_2 = 'replication-test_db2'
SHARED_CH_DB_NAME = 'shared_target_db'


def test_multi_mysql_single_ch_detection():
    """Test that the system correctly detects multiple MySQL databases mapping to single ClickHouse database"""
    cfg = config.Settings()
    cfg.load(CONFIG_FILE_MULTI_MYSQL)
    
    # Verify detection logic
    assert cfg.is_multiple_mysql_dbs_to_single_ch_db(TEST_DB_NAME_1, SHARED_CH_DB_NAME) == True
    assert cfg.is_multiple_mysql_dbs_to_single_ch_db(TEST_DB_NAME_2, SHARED_CH_DB_NAME) == True
    
    # Test with single mapping - should return False
    assert cfg.is_multiple_mysql_dbs_to_single_ch_db('other_db', 'other_target') == False


def test_multi_mysql_single_ch_replication():
    """Test replication of multiple MySQL databases to single ClickHouse database"""
    cfg = config.Settings()
    cfg.load(CONFIG_FILE_MULTI_MYSQL)
    
    mysql = mysql_api.MySQLApi(
        database=None,
        mysql_settings=cfg.mysql,
    )
    
    ch = clickhouse_api.ClickhouseApi(
        database=SHARED_CH_DB_NAME,
        clickhouse_settings=cfg.clickhouse,
    )
    
    # Clean up
    if os.path.exists(cfg.binlog_replicator.data_dir):
        import shutil
        shutil.rmtree(cfg.binlog_replicator.data_dir)
    os.makedirs(cfg.binlog_replicator.data_dir, exist_ok=True)
    
    # Drop and recreate MySQL databases
    mysql.drop_database(TEST_DB_NAME_1)
    mysql.create_database(TEST_DB_NAME_1)
    mysql.drop_database(TEST_DB_NAME_2)
    mysql.create_database(TEST_DB_NAME_2)
    
    # Drop ClickHouse database
    ch.drop_database(SHARED_CH_DB_NAME)
    assert_wait(lambda: SHARED_CH_DB_NAME not in ch.get_databases())
    
    # Create tables in first MySQL database
    mysql.set_database(TEST_DB_NAME_1)
    mysql.execute(f'''
        CREATE TABLE users (
            id INT PRIMARY KEY,
            name VARCHAR(100) NOT NULL
        )
    ''')
    mysql.execute(f"INSERT INTO users (id, name) VALUES (1, 'Alice');", commit=True)
    mysql.execute(f"INSERT INTO users (id, name) VALUES (2, 'Bob');", commit=True)
    
    # Create tables in second MySQL database (different table name)
    mysql.set_database(TEST_DB_NAME_2)
    mysql.execute(f'''
        CREATE TABLE products (
            id INT PRIMARY KEY,
            name VARCHAR(100) NOT NULL
        )
    ''')
    mysql.execute(f"INSERT INTO products (id, name) VALUES (1, 'Widget');", commit=True)
    mysql.execute(f"INSERT INTO products (id, name) VALUES (2, 'Gadget');", commit=True)
    
    # Run replication using run_all
    run_all_runner = RunAllRunner(cfg_file=CONFIG_FILE_MULTI_MYSQL)
    run_all_runner.run()
    
    # Wait for both databases to be replicated (with renamed tables)
    assert_wait(lambda: SHARED_CH_DB_NAME in ch.get_databases())
    ch.execute_command(f'USE `{SHARED_CH_DB_NAME}`')
    assert_wait(lambda: 'db1_users' in ch.get_tables())
    assert_wait(lambda: 'db2_products' in ch.get_tables())
    assert_wait(lambda: len(ch.select('db1_users')) == 2)
    assert_wait(lambda: len(ch.select('db2_products')) == 2)
    
    # Test realtime replication - add more data
    mysql.set_database(TEST_DB_NAME_1)
    mysql.execute(f"INSERT INTO users (id, name) VALUES (3, 'Charlie');", commit=True)
    
    mysql.set_database(TEST_DB_NAME_2)
    mysql.execute(f"INSERT INTO products (id, name) VALUES (3, 'Doohickey');", commit=True)
    
    # Wait for realtime changes to replicate (using renamed tables)
    assert_wait(lambda: len(ch.select('db1_users')) == 3)
    assert_wait(lambda: len(ch.select('db2_products')) == 3)
    
    # Verify all data (using renamed tables)
    users_results = ch.select('db1_users')
    users_ids = sorted([row['id'] for row in users_results])
    assert users_ids == [1, 2, 3], f"Expected [1, 2, 3], got {users_ids}"
    
    products_results = ch.select('db2_products')
    products_ids = sorted([row['id'] for row in products_results])
    assert products_ids == [1, 2, 3], f"Expected [1, 2, 3], got {products_ids}"
    
    run_all_runner.stop()
    
    assert_wait(lambda: 'stopping db_replicator' in read_logs(TEST_DB_NAME_1))
    assert_wait(lambda: 'stopping db_replicator' in read_logs(TEST_DB_NAME_2))
    assert('Traceback' not in read_logs(TEST_DB_NAME_1))
    assert('Traceback' not in read_logs(TEST_DB_NAME_2))

