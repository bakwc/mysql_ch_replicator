import datetime
import os
import shutil
import time
import subprocess
import json
import uuid
import decimal
import tempfile
import yaml

import pytest
import requests

from mysql_ch_replicator import config
from mysql_ch_replicator import mysql_api
from mysql_ch_replicator import clickhouse_api
from mysql_ch_replicator.binlog_replicator import State as BinlogState, FileReader, EventType, BinlogReplicator
from mysql_ch_replicator.db_replicator import State as DbReplicatorState, DbReplicator, DbReplicatorInitial
from mysql_ch_replicator.converter import MysqlToClickhouseConverter

from mysql_ch_replicator.runner import ProcessRunner


CONFIG_FILE = 'tests_config.yaml'
CONFIG_FILE_MARIADB = 'tests_config_mariadb.yaml'
TEST_DB_NAME = 'replication-test_db'
TEST_DB_NAME_2 = 'replication-test_db_2'
TEST_DB_NAME_2_DESTINATION = 'replication-destination'

TEST_TABLE_NAME = 'test_table'
TEST_TABLE_NAME_2 = 'test_table_2'
TEST_TABLE_NAME_3 = 'test_table_3'


class BinlogReplicatorRunner(ProcessRunner):
    def __init__(self, cfg_file=CONFIG_FILE):
        super().__init__(f'./main.py --config {cfg_file} binlog_replicator')


class DbReplicatorRunner(ProcessRunner):
    def __init__(self, db_name, additional_arguments=None, cfg_file=CONFIG_FILE):
        additional_arguments = additional_arguments or ''
        if not additional_arguments.startswith(' '):
            additional_arguments = ' ' + additional_arguments
        super().__init__(f'./main.py --config {cfg_file} --db {db_name} db_replicator{additional_arguments}')


class RunAllRunner(ProcessRunner):
    def __init__(self, cfg_file=CONFIG_FILE):
        super().__init__(f'./main.py --config {cfg_file} run_all')


def kill_process(pid, force=False):
    command = f'kill {pid}'
    if force:
        command = f'kill -9 {pid}'
    subprocess.run(command, shell=True)


def assert_wait(condition, max_wait_time=20.0, retry_interval=0.05):
    max_time = time.time() + max_wait_time
    while time.time() < max_time:
        if condition():
            return
        time.sleep(retry_interval)
    assert condition()


def prepare_env(
        cfg: config.Settings,
        mysql: mysql_api.MySQLApi,
        ch: clickhouse_api.ClickhouseApi,
        db_name: str = TEST_DB_NAME,
        set_mysql_db: bool = True
):
    if os.path.exists(cfg.binlog_replicator.data_dir):
        shutil.rmtree(cfg.binlog_replicator.data_dir)
    os.mkdir(cfg.binlog_replicator.data_dir)
    mysql.drop_database(db_name)
    mysql.create_database(db_name)
    if set_mysql_db:
        mysql.set_database(db_name)
    ch.drop_database(db_name)
    assert_wait(lambda: db_name not in ch.get_databases())


@pytest.mark.parametrize('config_file', [
    CONFIG_FILE,
    CONFIG_FILE_MARIADB,
])
def test_e2e_regular(config_file):
    cfg = config.Settings()
    cfg.load(config_file)

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
    id int NOT NULL AUTO_INCREMENT,
    name varchar(255) COMMENT 'Dân tộc, ví dụ: Kinh',
    age int COMMENT 'CMND Cũ',
    field1 text,
    field2 blob,
    PRIMARY KEY (id)
); 
    ''')

    mysql.execute(
        f"INSERT INTO `{TEST_TABLE_NAME}` (name, age, field1, field2) VALUES ('Ivan', 42, 'test1', 'test2');",
        commit=True,
    )
    mysql.execute(f"INSERT INTO `{TEST_TABLE_NAME}` (name, age) VALUES ('Peter', 33);", commit=True)

    binlog_replicator_runner = BinlogReplicatorRunner(cfg_file=config_file)
    binlog_replicator_runner.run()
    db_replicator_runner = DbReplicatorRunner(TEST_DB_NAME, cfg_file=config_file)
    db_replicator_runner.run()

    assert_wait(lambda: TEST_DB_NAME in ch.get_databases())

    ch.execute_command(f'USE `{TEST_DB_NAME}`')

    assert_wait(lambda: TEST_TABLE_NAME in ch.get_tables())
    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 2)

    # Check for custom partition_by configuration when using CONFIG_FILE (tests_config.yaml)
    if config_file == CONFIG_FILE_MARIADB:
        create_query = ch.show_create_table(TEST_TABLE_NAME)
        assert 'PARTITION BY intDiv(id, 1000000)' in create_query, f"Custom partition_by not found in CREATE TABLE query: {create_query}"

    mysql.execute(f"INSERT INTO `{TEST_TABLE_NAME}` (name, age) VALUES ('Filipp', 50);", commit=True)
    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 3)
    assert_wait(lambda: ch.select(TEST_TABLE_NAME, where="name='Filipp'")[0]['age'] == 50)


    mysql.execute(f"ALTER TABLE `{TEST_TABLE_NAME}` ADD `last_name` varchar(255); ")
    mysql.execute(f"ALTER TABLE `{TEST_TABLE_NAME}` ADD `price` decimal(10,2) DEFAULT NULL; ")

    mysql.execute(f"ALTER TABLE `{TEST_TABLE_NAME}` ADD UNIQUE INDEX prise_idx (price)")
    mysql.execute(f"ALTER TABLE `{TEST_TABLE_NAME}` DROP INDEX prise_idx, ADD UNIQUE INDEX age_idx (age)")

    mysql.execute(f"INSERT INTO `{TEST_TABLE_NAME}` (name, age, last_name, price) VALUES ('Mary', 24, 'Smith', 3.2);", commit=True)

    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 4)
    assert_wait(lambda: ch.select(TEST_TABLE_NAME, where="name='Mary'")[0]['last_name'] == 'Smith')

    assert_wait(lambda: ch.select(TEST_TABLE_NAME, where="field1='test1'")[0]['name'] == 'Ivan')
    assert_wait(lambda: ch.select(TEST_TABLE_NAME, where="field2='test2'")[0]['name'] == 'Ivan')


    mysql.execute(
        f"ALTER TABLE `{TEST_DB_NAME}`.`{TEST_TABLE_NAME}` "
        f"ADD COLUMN country VARCHAR(25) DEFAULT '' NOT NULL AFTER name;"
    )

    mysql.execute(
        f"INSERT INTO `{TEST_TABLE_NAME}` (name, age, last_name, country) "
        f"VALUES ('John', 12, 'Doe', 'USA');", commit=True,
    )

    mysql.execute(
        f"ALTER TABLE `{TEST_DB_NAME}`.`{TEST_TABLE_NAME}` "
        f"CHANGE COLUMN country origin VARCHAR(24) DEFAULT '' NOT NULL",
    )

    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 5)
    assert_wait(lambda: ch.select(TEST_TABLE_NAME, where="name='John'")[0].get('origin') == 'USA')

    mysql.execute(
        f"ALTER TABLE `{TEST_DB_NAME}`.`{TEST_TABLE_NAME}` "
        f"CHANGE COLUMN origin country VARCHAR(24) DEFAULT '' NOT NULL",
    )
    assert_wait(lambda: ch.select(TEST_TABLE_NAME, where="name='John'")[0].get('origin') is None)
    assert_wait(lambda: ch.select(TEST_TABLE_NAME, where="name='John'")[0].get('country') == 'USA')

    mysql.execute(f"ALTER TABLE `{TEST_DB_NAME}`.`{TEST_TABLE_NAME}` DROP COLUMN country")
    assert_wait(lambda: ch.select(TEST_TABLE_NAME, where="name='John'")[0].get('country') is None)

    assert_wait(lambda: ch.select(TEST_TABLE_NAME, where="name='Filipp'")[0].get('last_name') is None)

    mysql.execute(f"UPDATE `{TEST_TABLE_NAME}` SET last_name = '' WHERE last_name IS NULL;")
    mysql.execute(f"ALTER TABLE `{TEST_TABLE_NAME}` MODIFY `last_name` varchar(1024) NOT NULL")

    assert_wait(lambda: ch.select(TEST_TABLE_NAME, where="name='Filipp'")[0].get('last_name') == '')


    mysql.execute(f'''
    CREATE TABLE {TEST_TABLE_NAME_2} (
        id int NOT NULL AUTO_INCREMENT,
        name varchar(255),
        age int,
        PRIMARY KEY (id)
    ); 
        ''')

    assert_wait(lambda: TEST_TABLE_NAME_2 in ch.get_tables())

    mysql.execute(f"INSERT INTO `{TEST_TABLE_NAME_2}` (name, age) VALUES ('Ivan', 42);", commit=True)
    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME_2)) == 1)


    mysql.execute(f'''
    CREATE TABLE `{TEST_TABLE_NAME_3}` (
        id int NOT NULL AUTO_INCREMENT,
        `name` varchar(255),
        age int,
        PRIMARY KEY (`id`)
    ); 
        ''')

    assert_wait(lambda: TEST_TABLE_NAME_3 in ch.get_tables())

    mysql.execute(f"INSERT INTO `{TEST_TABLE_NAME_3}` (name, `age`) VALUES ('Ivan', 42);", commit=True)
    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME_3)) == 1)

    mysql.execute(f'DROP TABLE `{TEST_TABLE_NAME_3}`')
    assert_wait(lambda: TEST_TABLE_NAME_3 not in ch.get_tables())

    db_replicator_runner.stop()


def test_e2e_multistatement():
    cfg = config.Settings()
    cfg.load(CONFIG_FILE)

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
    id int NOT NULL AUTO_INCREMENT,
    name varchar(255),
    age int,
    PRIMARY KEY (id, `name`)
); 
    ''')

    mysql.execute(f"INSERT INTO `{TEST_TABLE_NAME}` (name, age) VALUES ('Ivan', 42);", commit=True)

    binlog_replicator_runner = BinlogReplicatorRunner()
    binlog_replicator_runner.run()
    db_replicator_runner = DbReplicatorRunner(TEST_DB_NAME)
    db_replicator_runner.run()

    assert_wait(lambda: TEST_DB_NAME in ch.get_databases())

    ch.execute_command(f'USE `{TEST_DB_NAME}`')

    assert_wait(lambda: TEST_TABLE_NAME in ch.get_tables())
    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 1)

    mysql.execute(f"ALTER TABLE `{TEST_TABLE_NAME}` ADD `last_name` varchar(255), ADD COLUMN city varchar(255); ")
    mysql.execute(
        f"INSERT INTO `{TEST_TABLE_NAME}` (name, age, last_name, city) "
        f"VALUES ('Mary', 24, 'Smith', 'London');", commit=True,
    )

    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 2)
    assert_wait(lambda: ch.select(TEST_TABLE_NAME, where="name='Mary'")[0].get('last_name') == 'Smith')
    assert_wait(lambda: ch.select(TEST_TABLE_NAME, where="name='Mary'")[0].get('city') == 'London')

    mysql.execute(f"ALTER TABLE `{TEST_TABLE_NAME}` DROP COLUMN last_name, DROP COLUMN city")
    assert_wait(lambda: ch.select(TEST_TABLE_NAME, where="name='Mary'")[0].get('last_name') is None)
    assert_wait(lambda: ch.select(TEST_TABLE_NAME, where="name='Mary'")[0].get('city') is None)

    mysql.execute(f"DELETE FROM `{TEST_TABLE_NAME}` WHERE name='Ivan';", commit=True)
    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 1)

    mysql.execute(f"ALTER TABLE `{TEST_TABLE_NAME}` ADD factor NUMERIC(5, 2) DEFAULT NULL;")
    mysql.execute(f"INSERT INTO `{TEST_TABLE_NAME}` (name, age, factor) VALUES ('Snow', 31, 13.29);", commit=True)

    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 2)
    assert_wait(lambda: ch.select(TEST_TABLE_NAME, where="name='Snow'")[0].get('factor') == decimal.Decimal('13.29'))

    mysql.execute(
        f"CREATE TABLE {TEST_TABLE_NAME_2} "
        f"(id int NOT NULL AUTO_INCREMENT, name varchar(255), age int, "
        f"PRIMARY KEY (id));"
    )

    assert_wait(lambda: TEST_TABLE_NAME_2 in ch.get_tables())

    db_replicator_runner.stop()
    binlog_replicator_runner.stop()


def get_binlog_replicator_pid(cfg: config.Settings):
    path = os.path.join(
        cfg.binlog_replicator.data_dir,
        'state.json',
    )
    state = BinlogState(path)
    return state.pid


def get_db_replicator_pid(cfg: config.Settings, db_name: str):
    path = os.path.join(
        cfg.binlog_replicator.data_dir,
        db_name,
        'state.pckl',
    )
    state = DbReplicatorState(path)
    return state.pid


@pytest.mark.parametrize('cfg_file', [CONFIG_FILE, 'tests_config_parallel.yaml'])
def test_runner(cfg_file):
    cfg = config.Settings()
    cfg.load(cfg_file)

    mysql = mysql_api.MySQLApi(
        database=None,
        mysql_settings=cfg.mysql,
    )

    ch = clickhouse_api.ClickhouseApi(
        database=TEST_DB_NAME,
        clickhouse_settings=cfg.clickhouse,
    )

    mysql.drop_database(TEST_DB_NAME_2)
    ch.drop_database(TEST_DB_NAME_2)
    ch.drop_database(TEST_DB_NAME_2_DESTINATION)

    prepare_env(cfg, mysql, ch)

    mysql.execute(f'''
CREATE TABLE `{TEST_TABLE_NAME}` (
    id int NOT NULL AUTO_INCREMENT,
    name varchar(255),
    age int,
    rate decimal(10,4),
    coordinate point NOT NULL,
    KEY `IDX_age` (`age`),
    FULLTEXT KEY `IDX_name` (`name`),
    PRIMARY KEY (id),
    SPATIAL KEY `coordinate` (`coordinate`)
) ENGINE=InnoDB AUTO_INCREMENT=2478808 DEFAULT CHARSET=latin1; 
    ''', commit=True)


    mysql.execute(f'''
    CREATE TABLE `group` (
        id int NOT NULL AUTO_INCREMENT,
        name varchar(255) NOT NULL,
        age int,
        rate decimal(10,4),
        PRIMARY KEY (id)
    ); 
        ''', commit=True)


    mysql.execute(f"INSERT INTO `{TEST_TABLE_NAME}` (name, age, coordinate) VALUES ('Ivan', 42, POINT(10.0, 20.0));", commit=True)
    mysql.execute(f"INSERT INTO `{TEST_TABLE_NAME}` (name, age, coordinate) VALUES ('Peter', 33, POINT(10.0, 20.0));", commit=True)

    mysql.execute(f"INSERT INTO `group` (name, age, rate) VALUES ('Peter', 33, 10.2);", commit=True)

    run_all_runner = RunAllRunner(cfg_file=cfg_file)
    run_all_runner.run()

    assert_wait(lambda: TEST_DB_NAME in ch.get_databases())

    ch.execute_command(f'USE `{TEST_DB_NAME}`;')

    assert_wait(lambda: 'group' in ch.get_tables())

    mysql.drop_table('group')

    assert_wait(lambda: 'group' not in ch.get_databases())

    assert_wait(lambda: TEST_TABLE_NAME in ch.get_tables())
    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 2)

    mysql.execute(f"INSERT INTO `{TEST_TABLE_NAME}` (name, age, coordinate) VALUES ('Xeishfru32', 50, POINT(10.0, 20.0));", commit=True)
    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 3)
    assert_wait(lambda: ch.select(TEST_TABLE_NAME, where="name='Xeishfru32'")[0]['age'] == 50)

    # Test for restarting dead processes
    binlog_repl_pid = get_binlog_replicator_pid(cfg)
    db_repl_pid = get_db_replicator_pid(cfg, TEST_DB_NAME)

    kill_process(binlog_repl_pid)
    kill_process(db_repl_pid, force=True)

    mysql.execute(f"INSERT INTO `{TEST_TABLE_NAME}` (name, rate, coordinate) VALUES ('John', 12.5, POINT(10.0, 20.0));", commit=True)
    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 4)
    assert_wait(lambda: ch.select(TEST_TABLE_NAME, where="name='John'")[0]['rate'] == 12.5)

    mysql.execute(f"DELETE FROM `{TEST_TABLE_NAME}` WHERE name='John';", commit=True)
    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 3)

    mysql.execute(f"UPDATE `{TEST_TABLE_NAME}` SET age=66 WHERE name='Ivan'", commit=True)
    assert_wait(lambda: ch.select(TEST_TABLE_NAME, "name='Ivan'")[0]['age'] == 66)

    mysql.execute(f"UPDATE `{TEST_TABLE_NAME}` SET age=77 WHERE name='Ivan'", commit=True)
    assert_wait(lambda: ch.select(TEST_TABLE_NAME, "name='Ivan'")[0]['age'] == 77)

    mysql.execute(f"UPDATE `{TEST_TABLE_NAME}` SET age=88 WHERE name='Ivan'", commit=True)
    assert_wait(lambda: ch.select(TEST_TABLE_NAME, "name='Ivan'")[0]['age'] == 88)

    mysql.execute(f"INSERT INTO `{TEST_TABLE_NAME}` (name, age, coordinate) VALUES ('Vlad', 99, POINT(10.0, 20.0));", commit=True)

    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 4)

    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME, final=False)) == 4)

    mysql.execute(
        command=f"INSERT INTO `{TEST_TABLE_NAME}` (name, age, coordinate) VALUES (%s, %s, POINT(10.0, 20.0));",
        args=(b'H\xe4llo'.decode('latin-1'), 1912),
        commit=True,
    )

    assert_wait(lambda: TEST_DB_NAME in ch.get_databases())

    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 5)
    assert_wait(lambda: ch.select(TEST_TABLE_NAME, "age=1912")[0]['name'] == 'Hällo')

    ch.drop_database(TEST_DB_NAME)
    ch.drop_database(TEST_DB_NAME_2)

    requests.get('http://localhost:9128/restart_replication')
    time.sleep(1.0)

    assert_wait(lambda: TEST_DB_NAME in ch.get_databases())

    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 5)
    assert_wait(lambda: ch.select(TEST_TABLE_NAME, "age=1912")[0]['name'] == 'Hällo')

    mysql.create_database(TEST_DB_NAME_2)
    assert_wait(lambda: TEST_DB_NAME_2_DESTINATION in ch.get_databases())

    mysql.execute(f'''
    CREATE TABLE `group` (
        id int NOT NULL AUTO_INCREMENT,
        name varchar(255) NOT NULL,
        age int,
        rate decimal(10,4),
        PRIMARY KEY (id)
    ); 
        ''')

    assert_wait(lambda: 'group' in ch.get_tables())

    create_query = ch.show_create_table('group')
    assert 'INDEX name_idx name TYPE ngrambf_v1' in create_query

    run_all_runner.stop()


def read_logs(db_name):
    return open(os.path.join('binlog', db_name, 'db_replicator.log')).read()


def test_multi_column_erase():
    config_file = CONFIG_FILE

    cfg = config.Settings()
    cfg.load(config_file)

    mysql = mysql_api.MySQLApi(
        database=None,
        mysql_settings=cfg.mysql,
    )

    ch = clickhouse_api.ClickhouseApi(
        database=TEST_DB_NAME,
        clickhouse_settings=cfg.clickhouse,
    )

    mysql.drop_database(TEST_DB_NAME_2)
    ch.drop_database(TEST_DB_NAME_2_DESTINATION)

    prepare_env(cfg, mysql, ch)

    mysql.execute(f'''
CREATE TABLE `{TEST_TABLE_NAME}` (
    departments int(11) NOT NULL,
    termine int(11) NOT NULL,
    PRIMARY KEY (departments,termine)
)
''')


    mysql.execute(f"INSERT INTO `{TEST_TABLE_NAME}` (departments, termine) VALUES (10, 20);", commit=True)
    mysql.execute(f"INSERT INTO `{TEST_TABLE_NAME}` (departments, termine) VALUES (30, 40);", commit=True)
    mysql.execute(f"INSERT INTO `{TEST_TABLE_NAME}` (departments, termine) VALUES (50, 60);", commit=True)
    mysql.execute(f"INSERT INTO `{TEST_TABLE_NAME}` (departments, termine) VALUES (20, 10);", commit=True)
    mysql.execute(f"INSERT INTO `{TEST_TABLE_NAME}` (departments, termine) VALUES (40, 30);", commit=True)
    mysql.execute(f"INSERT INTO `{TEST_TABLE_NAME}` (departments, termine) VALUES (60, 50);", commit=True)

    run_all_runner = RunAllRunner(cfg_file=config_file)
    run_all_runner.run()

    assert_wait(lambda: TEST_DB_NAME in ch.get_databases())

    ch.execute_command(f'USE `{TEST_DB_NAME}`')

    assert_wait(lambda: TEST_TABLE_NAME in ch.get_tables())
    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 6)

    mysql.execute(f"DELETE FROM `{TEST_TABLE_NAME}` WHERE departments=10;", commit=True)
    mysql.execute(f"DELETE FROM `{TEST_TABLE_NAME}` WHERE departments=30;", commit=True)
    mysql.execute(f"DELETE FROM `{TEST_TABLE_NAME}` WHERE departments=50;", commit=True)

    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 3)

    run_all_runner.stop()

    assert_wait(lambda: 'stopping db_replicator' in read_logs(TEST_DB_NAME))
    assert('Traceback' not in read_logs(TEST_DB_NAME))


def test_initial_only():
    cfg = config.Settings()
    cfg.load(CONFIG_FILE)

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
    id int NOT NULL AUTO_INCREMENT,
    name varchar(255),
    age int,
    PRIMARY KEY (id)
); 
    ''')

    mysql.execute(f"INSERT INTO `{TEST_TABLE_NAME}` (name, age) VALUES ('Ivan', 42);", commit=True)
    mysql.execute(f"INSERT INTO `{TEST_TABLE_NAME}` (name, age) VALUES ('Peter', 33);", commit=True)

    db_replicator_runner = DbReplicatorRunner(TEST_DB_NAME, additional_arguments='--initial_only=True')
    db_replicator_runner.run()
    db_replicator_runner.wait_complete()

    assert TEST_DB_NAME in ch.get_databases()

    ch.execute_command(f'USE `{TEST_DB_NAME}`')

    assert TEST_TABLE_NAME in ch.get_tables()
    assert len(ch.select(TEST_TABLE_NAME)) == 2

    ch.execute_command(f'DROP DATABASE `{TEST_DB_NAME}`')

    db_replicator_runner.stop()

    db_replicator_runner = DbReplicatorRunner(TEST_DB_NAME, additional_arguments='--initial_only=True')
    db_replicator_runner.run()
    db_replicator_runner.wait_complete()
    assert TEST_DB_NAME in ch.get_databases()

    db_replicator_runner.stop()


def test_parallel_initial_replication_record_versions():
    """
    Test that record versions are properly consolidated from worker states
    after parallel initial replication.
    """
    # Only run this test with parallel configuration
    cfg_file = 'tests_config_parallel.yaml'
    cfg = config.Settings()
    cfg.load(cfg_file)
    
    # Ensure we have parallel replication configured
    assert cfg.initial_replication_threads > 1, "This test requires initial_replication_threads > 1"
    
    mysql = mysql_api.MySQLApi(
        database=None,
        mysql_settings=cfg.mysql,
    )

    ch = clickhouse_api.ClickhouseApi(
        database=TEST_DB_NAME,
        clickhouse_settings=cfg.clickhouse,
    )

    prepare_env(cfg, mysql, ch)

    # Create a table with sufficient records for parallel processing
    mysql.execute(f'''
CREATE TABLE `{TEST_TABLE_NAME}` (
    id int NOT NULL AUTO_INCREMENT,
    name varchar(255),
    age int,
    version int NOT NULL DEFAULT 1,
    PRIMARY KEY (id)
); 
    ''')

    # Insert a large number of records to ensure parallel processing
    for i in range(1, 1001):
        mysql.execute(
            f"INSERT INTO `{TEST_TABLE_NAME}` (name, age, version) VALUES ('User{i}', {20+i%50}, {i});", 
            commit=(i % 100 == 0)  # Commit every 100 records
        )
    
    # Run initial replication only with parallel workers
    db_replicator_runner = DbReplicatorRunner(TEST_DB_NAME, cfg_file=cfg_file)
    db_replicator_runner.run()

    assert_wait(lambda: TEST_DB_NAME in ch.get_databases(), max_wait_time=10.0)

    ch.execute_command(f'USE `{TEST_DB_NAME}`')

    assert_wait(lambda: TEST_TABLE_NAME in ch.get_tables(), max_wait_time=10.0)
    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 1000, max_wait_time=10.0)

    db_replicator_runner.stop()

    # Verify database and table were created
    assert TEST_DB_NAME in ch.get_databases()
    ch.execute_command(f'USE `{TEST_DB_NAME}`')
    assert TEST_TABLE_NAME in ch.get_tables()
    
    # Verify all records were replicated
    records = ch.select(TEST_TABLE_NAME)
    assert len(records) == 1000
    
    # Instead of reading the state file directly, verify the record versions are correctly handled
    # by checking the max _version in the ClickHouse table
    versions_query = ch.query(f"SELECT MAX(_version) FROM `{TEST_DB_NAME}`.`{TEST_TABLE_NAME}`")
    max_version_in_ch = versions_query.result_rows[0][0]
    assert max_version_in_ch >= 200, f"Expected max _version to be at least 200, got {max_version_in_ch}"
    

    # Now test realtime replication to verify versions continue correctly
    # Start binlog replication
    binlog_replicator_runner = BinlogReplicatorRunner(cfg_file=cfg_file)
    binlog_replicator_runner.run()

    time.sleep(3.0)
    
    # Start DB replicator in realtime mode
    realtime_db_replicator = DbReplicatorRunner(TEST_DB_NAME, cfg_file=cfg_file)
    realtime_db_replicator.run()
    
    # Insert a new record with version 1001
    mysql.execute(
        f"INSERT INTO `{TEST_TABLE_NAME}` (name, age, version) VALUES ('UserRealtime', 99, 1001);", 
        commit=True
    )
    
    # Wait for the record to be replicated
    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 1001)
    
    # Verify the new record was replicated correctly
    realtime_record = ch.select(TEST_TABLE_NAME, where="name='UserRealtime'")[0]
    assert realtime_record['age'] == 99
    assert realtime_record['version'] == 1001
    
    # Check that the _version column in CH is a reasonable value
    # With parallel workers, the _version won't be > 1000 because each worker
    # has its own independent version counter and they never intersect
    versions_query = ch.query(f"SELECT _version FROM `{TEST_DB_NAME}`.`{TEST_TABLE_NAME}` WHERE name='UserRealtime'")
    ch_version = versions_query.result_rows[0][0]


    # With parallel workers (default is 4), each worker would process ~250 records
    # So the version for the new record should be slightly higher than 250
    # but definitely lower than 1000
    assert ch_version > 0, f"ClickHouse _version should be > 0, but got {ch_version}"
    
    # We expect version to be roughly: (total_records / num_workers) + 1
    # For 1000 records and 4 workers, expect around 251
    expected_version_approx = 1000 // cfg.initial_replication_threads + 1
    # Allow some flexibility in the exact expected value
    assert abs(ch_version - expected_version_approx) < 50, (
        f"ClickHouse _version should be close to {expected_version_approx}, but got {ch_version}"
    )
    
    # Clean up
    binlog_replicator_runner.stop()
    realtime_db_replicator.stop()
    db_replicator_runner.stop()


def test_database_tables_filtering():
    cfg = config.Settings()
    cfg.load('tests_config_databases_tables.yaml')

    mysql = mysql_api.MySQLApi(
        database=None,
        mysql_settings=cfg.mysql,
    )

    ch = clickhouse_api.ClickhouseApi(
        database='test_db_2',
        clickhouse_settings=cfg.clickhouse,
    )

    mysql.drop_database('test_db_3')
    mysql.drop_database('test_db_12')

    mysql.create_database('test_db_3')
    mysql.create_database('test_db_12')

    ch.drop_database('test_db_3')
    ch.drop_database('test_db_12')

    prepare_env(cfg, mysql, ch, db_name='test_db_2')

    mysql.execute(f'''
    CREATE TABLE test_table_15 (
        id int NOT NULL AUTO_INCREMENT,
        name varchar(255),
        age int,
        PRIMARY KEY (id)
    ); 
        ''')

    mysql.execute(f'''
    CREATE TABLE test_table_142 (
        id int NOT NULL AUTO_INCREMENT,
        name varchar(255),
        age int,
        PRIMARY KEY (id)
    ); 
        ''')

    mysql.execute(f'''
    CREATE TABLE test_table_143 (
        id int NOT NULL AUTO_INCREMENT,
        name varchar(255),
        age int,
        PRIMARY KEY (id)
    ); 
        ''')

    mysql.execute(f'''
CREATE TABLE test_table_3 (
    id int NOT NULL AUTO_INCREMENT,
    name varchar(255),
    age int,
    PRIMARY KEY (id)
); 
    ''')

    mysql.execute(f'''
    CREATE TABLE test_table_2 (
        id int NOT NULL AUTO_INCREMENT,
        name varchar(255),
        age int,
        PRIMARY KEY (id)
    ); 
        ''')

    mysql.execute(f"INSERT INTO test_table_3 (name, age) VALUES ('Ivan', 42);", commit=True)
    mysql.execute(f"INSERT INTO test_table_2 (name, age) VALUES ('Ivan', 42);", commit=True)

    run_all_runner = RunAllRunner(cfg_file='tests_config_databases_tables.yaml')
    run_all_runner.run()

    assert_wait(lambda: 'test_db_2' in ch.get_databases())
    assert 'test_db_3' not in ch.get_databases()
    assert 'test_db_12' not in ch.get_databases()

    ch.execute_command('USE test_db_2')

    assert_wait(lambda: 'test_table_2' in ch.get_tables())
    assert_wait(lambda: len(ch.select('test_table_2')) == 1)

    assert_wait(lambda: 'test_table_143' in ch.get_tables())

    assert 'test_table_3' not in ch.get_tables()

    assert 'test_table_15' not in ch.get_tables()
    assert 'test_table_142' not in ch.get_tables()

    run_all_runner.stop()


def test_datetime_exception():
    cfg = config.Settings()
    cfg.load(CONFIG_FILE)

    mysql = mysql_api.MySQLApi(
        database=None,
        mysql_settings=cfg.mysql,
    )

    ch = clickhouse_api.ClickhouseApi(
        database=TEST_DB_NAME,
        clickhouse_settings=cfg.clickhouse,
    )

    prepare_env(cfg, mysql, ch)

    mysql.execute("SET sql_mode = 'ALLOW_INVALID_DATES';")

    mysql.execute(f'''
CREATE TABLE `{TEST_TABLE_NAME}` (
    id int NOT NULL AUTO_INCREMENT,
    name varchar(255),
    modified_date DateTime(3) NOT NULL,
    test_date date NOT NULL,
        PRIMARY KEY (id)
    );
    ''')

    mysql.execute(
        f"INSERT INTO `{TEST_TABLE_NAME}` (name, modified_date, test_date) "
        f"VALUES ('Ivan', '0000-00-00 00:00:00', '2015-05-28');",
        commit=True,
    )

    binlog_replicator_runner = BinlogReplicatorRunner()
    binlog_replicator_runner.run()
    db_replicator_runner = DbReplicatorRunner(TEST_DB_NAME)
    db_replicator_runner.run()

    assert_wait(lambda: TEST_DB_NAME in ch.get_databases())

    ch.execute_command(f'USE `{TEST_DB_NAME}`')

    assert_wait(lambda: TEST_TABLE_NAME in ch.get_tables())
    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 1)

    mysql.execute(
        f"INSERT INTO `{TEST_TABLE_NAME}` (name, modified_date, test_date) "
        f"VALUES ('Alex', '0000-00-00 00:00:00', '2015-06-02');",
        commit=True,
    )
    mysql.execute(
        f"INSERT INTO `{TEST_TABLE_NAME}` (name, modified_date, test_date) "
        f"VALUES ('Givi', '2023-01-08 03:11:09', '2015-06-02');",
        commit=True,
    )
    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 3)
    assert_wait(lambda: str(ch.select(TEST_TABLE_NAME, where="name='Alex'")[0]['test_date']) == '2015-06-02')
    assert_wait(lambda: str(ch.select(TEST_TABLE_NAME, where="name='Ivan'")[0]['test_date']) == '2015-05-28')

    db_replicator_runner.stop()
    binlog_replicator_runner.stop()



def test_different_types_1():
    cfg = config.Settings()
    cfg.load(CONFIG_FILE)

    mysql = mysql_api.MySQLApi(
        database=None,
        mysql_settings=cfg.mysql,
    )

    ch = clickhouse_api.ClickhouseApi(
        database=TEST_DB_NAME,
        clickhouse_settings=cfg.clickhouse,
    )

    prepare_env(cfg, mysql, ch, set_mysql_db=False)

    mysql.execute("SET sql_mode = 'ALLOW_INVALID_DATES';")

    mysql.execute(f'''
CREATE TABLE `{TEST_DB_NAME}`.`{TEST_TABLE_NAME}` (
    `id` int unsigned NOT NULL AUTO_INCREMENT,
    name varchar(255),
  `employee` int unsigned NOT NULL,
  `position` smallint unsigned NOT NULL,
  `job_title` smallint NOT NULL DEFAULT '0',
  `department` smallint unsigned NOT NULL DEFAULT '0',
  `job_level` smallint unsigned NOT NULL DEFAULT '0',
  `job_grade` smallint unsigned NOT NULL DEFAULT '0',
  `level` smallint unsigned NOT NULL DEFAULT '0',
  `team` smallint unsigned NOT NULL DEFAULT '0',
  `factory` smallint unsigned NOT NULL DEFAULT '0',
  `ship` smallint unsigned NOT NULL DEFAULT '0',
  `report_to` int unsigned NOT NULL DEFAULT '0',
  `line_manager` int unsigned NOT NULL DEFAULT '0',
  `location` smallint unsigned NOT NULL DEFAULT '0',
  `customer` int unsigned NOT NULL DEFAULT '0',
  `effective_date` date NOT NULL DEFAULT '0000-00-00',
  `status` tinyint unsigned NOT NULL DEFAULT '0',
  `promotion` tinyint unsigned NOT NULL DEFAULT '0',
  `promotion_id` int unsigned NOT NULL DEFAULT '0',
  `note` text CHARACTER SET utf8mb3 COLLATE utf8mb3_unicode_ci NOT NULL,
  `is_change_probation_time` tinyint unsigned NOT NULL DEFAULT '0',
  `deleted` tinyint unsigned NOT NULL DEFAULT '0',
  `created_by` int unsigned NOT NULL DEFAULT '0',
  `created_by_name` varchar(125) CHARACTER SET utf8mb3 COLLATE utf8mb3_unicode_ci NOT NULL DEFAULT '',
  `created_date` datetime NOT NULL DEFAULT '0000-00-00 00:00:00',
  `modified_by` int unsigned NOT NULL DEFAULT '0',
  `modified_by_name` varchar(125) CHARACTER SET utf8mb3 COLLATE utf8mb3_unicode_ci NOT NULL DEFAULT '',
  `modified_date` datetime NOT NULL DEFAULT '0000-00-00 00:00:00',
  `entity` int NOT NULL DEFAULT '0',
  `sent_2_tac` char(1) CHARACTER SET utf8mb3 COLLATE utf8mb3_unicode_ci NOT NULL DEFAULT '0',
    PRIMARY KEY (id),
    KEY `name, employee` (`name`,`employee`) USING BTREE
); 
    ''')

    mysql.execute(
        f"INSERT INTO `{TEST_DB_NAME}`.`{TEST_TABLE_NAME}` (name, modified_date) VALUES ('Ivan', '0000-00-00 00:00:00');",
        commit=True,
    )

    binlog_replicator_runner = BinlogReplicatorRunner()
    binlog_replicator_runner.run()
    db_replicator_runner = DbReplicatorRunner(TEST_DB_NAME)
    db_replicator_runner.run()

    assert_wait(lambda: TEST_DB_NAME in ch.get_databases())

    ch.execute_command(f'USE `{TEST_DB_NAME}`')

    assert_wait(lambda: TEST_TABLE_NAME in ch.get_tables())
    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 1)

    mysql.execute(
        f"INSERT INTO `{TEST_DB_NAME}`.`{TEST_TABLE_NAME}` (name, modified_date) VALUES ('Alex', '0000-00-00 00:00:00');",
        commit=True,
    )
    mysql.execute(
        f"INSERT INTO `{TEST_DB_NAME}`.`{TEST_TABLE_NAME}` (name, modified_date) VALUES ('Givi', '2023-01-08 03:11:09');",
        commit=True,
    )
    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 3)

    mysql.execute(f'''
    CREATE TABLE `{TEST_DB_NAME}`.`{TEST_TABLE_NAME_2}` (
        `id` int unsigned NOT NULL AUTO_INCREMENT,
        name varchar(255),
        PRIMARY KEY (id)
    ); 
        ''')

    mysql.execute(
        f"INSERT INTO `{TEST_DB_NAME}`.`{TEST_TABLE_NAME_2}` (name) VALUES ('Ivan');",
        commit=True,
    )

    assert_wait(lambda: TEST_TABLE_NAME_2 in ch.get_tables())

    db_replicator_runner.stop()
    binlog_replicator_runner.stop()

def test_numeric_types_and_limits():
    cfg = config.Settings()
    cfg.load(CONFIG_FILE)

    mysql = mysql_api.MySQLApi(
        database=None,
        mysql_settings=cfg.mysql,
    )

    ch = clickhouse_api.ClickhouseApi(
        database=TEST_DB_NAME,
        clickhouse_settings=cfg.clickhouse,
    )

    prepare_env(cfg, mysql, ch)

    mysql.execute("SET sql_mode = 'ALLOW_INVALID_DATES';")

    mysql.execute(f'''
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
    ''')

    mysql.execute(
        f"INSERT INTO `{TEST_TABLE_NAME}` (name, test1, test2, test3, test4, test5, test6, test7, test8) VALUES "
        f"('Ivan', -20000, 50000, -30, 100, 16777200, 4294967290, 18446744073709551586, NULL);",
        commit=True,
    )

    binlog_replicator_runner = BinlogReplicatorRunner()
    binlog_replicator_runner.run()
    db_replicator_runner = DbReplicatorRunner(TEST_DB_NAME)
    db_replicator_runner.run()

    assert_wait(lambda: TEST_DB_NAME in ch.get_databases())

    ch.execute_command(f'USE `{TEST_DB_NAME}`')

    assert_wait(lambda: TEST_TABLE_NAME in ch.get_tables())
    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 1)

    mysql.execute(
        f"INSERT INTO `{TEST_TABLE_NAME}` (name, test1, test2, test3, test4, test5, test6, test7, test8) VALUES "
        f"('Peter', -10000, 60000, -120, 250, 16777200, 4294967280, 18446744073709551586, NULL);",
        commit=True,
    )
    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 2)
    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME, 'test2=60000')) == 1)
    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME, 'test4=250')) == 1)
    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME, 'test5=16777200')) == 2)
    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME, 'test6=4294967290')) == 1)
    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME, 'test6=4294967280')) == 1)
    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME, 'test7=18446744073709551586')) == 2)

    db_replicator_runner.stop()
    binlog_replicator_runner.stop()


def test_different_types_2():
    cfg = config.Settings()
    cfg.load(CONFIG_FILE)

    mysql = mysql_api.MySQLApi(
        database=None,
        mysql_settings=cfg.mysql,
    )

    ch = clickhouse_api.ClickhouseApi(
        database=TEST_DB_NAME,
        clickhouse_settings=cfg.clickhouse,
    )

    prepare_env(cfg, mysql, ch)

    mysql.execute("SET sql_mode = 'ALLOW_INVALID_DATES';")

    mysql.execute(f'''
CREATE TABLE `{TEST_TABLE_NAME}` (
    `id` int unsigned NOT NULL AUTO_INCREMENT,
    test1 bit(1),
    test2 point,
    test3 binary(16),
    test4 set('1','2','3','4','5','6','7'),
    test5 timestamp(0),
    test6 char(36),
    test7 ENUM('point', 'qwe', 'def', 'azaza kokoko'),
    PRIMARY KEY (id)
); 
    ''')

    mysql.execute(
        f"INSERT INTO `{TEST_TABLE_NAME}` (test1, test2, test3, test4, test5, test6, test7) VALUES "
        f"(0, POINT(10.0, 20.0), 'azaza', '1,3,5', '2023-08-15 14:30:00', '550e8400-e29b-41d4-a716-446655440000', 'def');",
        commit=True,
    )

    binlog_replicator_runner = BinlogReplicatorRunner()
    binlog_replicator_runner.run()
    db_replicator_runner = DbReplicatorRunner(TEST_DB_NAME)
    db_replicator_runner.run()

    assert_wait(lambda: TEST_DB_NAME in ch.get_databases())

    ch.execute_command(f'USE `{TEST_DB_NAME}`')

    assert_wait(lambda: TEST_TABLE_NAME in ch.get_tables())
    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 1)

    mysql.execute(
        f"INSERT INTO `{TEST_TABLE_NAME}` (test1, test2, test4, test5, test6, test7) VALUES "
        f"(1, POINT(15.0, 14.0), '2,4,5', '2023-08-15 14:40:00', '110e6103-e39b-51d4-a716-826755413099', 'point');",
        commit=True,
    )

    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 2)
    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME, 'test1=True')) == 1)

    assert ch.select(TEST_TABLE_NAME, 'test1=True')[0]['test2']['x'] == 15.0
    assert ch.select(TEST_TABLE_NAME, 'test1=True')[0]['test7'] == 'point'
    assert ch.select(TEST_TABLE_NAME, 'test1=False')[0]['test2']['y'] == 20.0
    assert ch.select(TEST_TABLE_NAME, 'test1=False')[0]['test7'] == 'def'
    assert ch.select(TEST_TABLE_NAME, 'test1=False')[0]['test3'] == 'azaza\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'

    assert ch.select(TEST_TABLE_NAME, 'test1=True')[0]['test4'] == '2,4,5'
    assert ch.select(TEST_TABLE_NAME, 'test1=False')[0]['test4'] == '1,3,5'

    value = ch.select(TEST_TABLE_NAME, 'test1=True')[0]['test5']
    assert isinstance(value, datetime.datetime)
    assert str(value) == '2023-08-15 14:40:00+00:00'

    assert ch.select(TEST_TABLE_NAME, 'test1=True')[0]['test6'] == uuid.UUID('110e6103-e39b-51d4-a716-826755413099')

    mysql.execute(
        f"INSERT INTO `{TEST_TABLE_NAME}` (test1, test2) VALUES "
        f"(0, NULL);",
        commit=True,
    )
    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 3)

    db_replicator_runner.stop()
    binlog_replicator_runner.stop()


def test_json():
    cfg = config.Settings()
    cfg.load(CONFIG_FILE)

    mysql = mysql_api.MySQLApi(
        database=None,
        mysql_settings=cfg.mysql,
    )

    ch = clickhouse_api.ClickhouseApi(
        database=TEST_DB_NAME,
        clickhouse_settings=cfg.clickhouse,
    )

    prepare_env(cfg, mysql, ch)

    mysql.execute("SET sql_mode = 'ALLOW_INVALID_DATES';")

    mysql.execute(f'''
CREATE TABLE `{TEST_TABLE_NAME}` (
    `id` int unsigned NOT NULL AUTO_INCREMENT,
    name varchar(255),
    data json,
    PRIMARY KEY (id)
); 
    ''')

    mysql.execute(
        f"INSERT INTO `{TEST_TABLE_NAME}` (name, data) VALUES " +
        """('Ivan', '{"a": "b", "c": [1,2,3]}');""",
        commit=True,
    )

    binlog_replicator_runner = BinlogReplicatorRunner()
    binlog_replicator_runner.run()
    db_replicator_runner = DbReplicatorRunner(TEST_DB_NAME)
    db_replicator_runner.run()

    assert_wait(lambda: TEST_DB_NAME in ch.get_databases())

    ch.execute_command(f'USE `{TEST_DB_NAME}`')

    assert_wait(lambda: TEST_TABLE_NAME in ch.get_tables())
    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 1)

    mysql.execute(
        f"INSERT INTO `{TEST_TABLE_NAME}` (name, data) VALUES " +
        """('Peter', '{"b": "b", "c": [3,2,1]}');""",
        commit=True,
    )
    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 2)

    assert json.loads(ch.select(TEST_TABLE_NAME, "name='Ivan'")[0]['data'])['c'] == [1, 2, 3]
    assert json.loads(ch.select(TEST_TABLE_NAME, "name='Peter'")[0]['data'])['c'] == [3, 2, 1]

    db_replicator_runner.stop()
    binlog_replicator_runner.stop()


def test_string_primary_key(monkeypatch):
    cfg = config.Settings()
    cfg.load('tests_config_string_primary_key.yaml')

    mysql = mysql_api.MySQLApi(
        database=None,
        mysql_settings=cfg.mysql,
    )

    ch = clickhouse_api.ClickhouseApi(
        database=TEST_DB_NAME,
        clickhouse_settings=cfg.clickhouse,
    )

    prepare_env(cfg, mysql, ch)

    mysql.execute("SET sql_mode = 'ALLOW_INVALID_DATES';")

    mysql.execute(f'''
CREATE TABLE `{TEST_TABLE_NAME}` (
    `id` char(30) NOT NULL,
    name varchar(255),
    PRIMARY KEY (id)
); 
    ''')

    mysql.execute(
        f"INSERT INTO `{TEST_TABLE_NAME}` (id, name) VALUES " +
        """('01', 'Ivan');""",
        commit=True,
    )
    mysql.execute(
        f"INSERT INTO `{TEST_TABLE_NAME}` (id, name) VALUES " +
        """('02', 'Peter');""",
        commit=True,
    )

    binlog_replicator_runner = BinlogReplicatorRunner(cfg_file='tests_config_string_primary_key.yaml')
    binlog_replicator_runner.run()
    db_replicator_runner = DbReplicatorRunner(TEST_DB_NAME, cfg_file='tests_config_string_primary_key.yaml')
    db_replicator_runner.run()

    assert_wait(lambda: TEST_DB_NAME in ch.get_databases())

    ch.execute_command(f'USE `{TEST_DB_NAME}`')

    assert_wait(lambda: TEST_TABLE_NAME in ch.get_tables())
    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 2)

    mysql.execute(
        f"INSERT INTO `{TEST_TABLE_NAME}` (id, name) VALUES " +
        """('03', 'Filipp');""",
        commit=True,
    )
    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 3)

    db_replicator_runner.stop()
    binlog_replicator_runner.stop()


def test_if_exists_if_not_exists(monkeypatch):
    cfg = config.Settings()
    cfg.load('tests_config_string_primary_key.yaml')

    mysql = mysql_api.MySQLApi(
        database=None,
        mysql_settings=cfg.mysql,
    )

    ch = clickhouse_api.ClickhouseApi(
        database=TEST_DB_NAME,
        clickhouse_settings=cfg.clickhouse,
    )

    prepare_env(cfg, mysql, ch)

    binlog_replicator_runner = BinlogReplicatorRunner(cfg_file='tests_config_string_primary_key.yaml')
    binlog_replicator_runner.run()
    db_replicator_runner = DbReplicatorRunner(TEST_DB_NAME, cfg_file='tests_config_string_primary_key.yaml')
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
    cfg.load('tests_config_string_primary_key.yaml')

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

    binlog_replicator_runner = BinlogReplicatorRunner(cfg_file='tests_config_string_primary_key.yaml')
    binlog_replicator_runner.run()
    db_replicator_runner = DbReplicatorRunner(TEST_DB_NAME, cfg_file='tests_config_string_primary_key.yaml')
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
    cfg.load('tests_config_string_primary_key.yaml')

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

    binlog_replicator_runner = BinlogReplicatorRunner(cfg_file='tests_config_string_primary_key.yaml')
    binlog_replicator_runner.run()
    db_replicator_runner = DbReplicatorRunner(TEST_DB_NAME, cfg_file='tests_config_string_primary_key.yaml')
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

    db_replicator_runner.stop()
    binlog_replicator_runner.stop()


def test_parse_mysql_table_structure():
    query = "CREATE TABLE IF NOT EXISTS user_preferences_portal (\n\t\t\tid char(36) NOT NULL,\n\t\t\tcategory varchar(50) DEFAULT NULL,\n\t\t\tdeleted tinyint(1) DEFAULT 0,\n\t\t\tdate_entered datetime DEFAULT NULL,\n\t\t\tdate_modified datetime DEFAULT NULL,\n\t\t\tassigned_user_id char(36) DEFAULT NULL,\n\t\t\tcontents longtext DEFAULT NULL\n\t\t ) ENGINE=InnoDB DEFAULT CHARSET=utf8"

    converter = MysqlToClickhouseConverter()

    structure = converter.parse_mysql_table_structure(query)

    assert structure.table_name == 'user_preferences_portal'


def get_last_file(directory, extension='.bin'):
    max_num = -1
    last_file = None
    ext_len = len(extension)

    with os.scandir(directory) as it:
        for entry in it:
            if entry.is_file() and entry.name.endswith(extension):
                # Extract the numerical part by removing the extension
                num_part = entry.name[:-ext_len]
                try:
                    num = int(num_part)
                    if num > max_num:
                        max_num = num
                        last_file = entry.name
                except ValueError:
                    # Skip files where the name before extension is not an integer
                    continue
    return last_file


def get_last_insert_from_binlog(cfg: config.Settings, db_name: str):
    binlog_dir_path = os.path.join(cfg.binlog_replicator.data_dir, db_name)
    if not os.path.exists(binlog_dir_path):
        return None
    last_file = get_last_file(binlog_dir_path)
    if last_file is None:
        return None
    reader = FileReader(os.path.join(binlog_dir_path, last_file))
    last_insert = None
    while True:
        event = reader.read_next_event()
        if event is None:
            break
        if event.event_type != EventType.ADD_EVENT.value:
            continue
        for record in event.records:
            last_insert = record
    return last_insert


@pytest.mark.optional
def test_performance_realtime_replication():
    config_file = 'tests_config_perf.yaml'
    num_records = 100000

    cfg = config.Settings()
    cfg.load(config_file)

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
        id int NOT NULL AUTO_INCREMENT,
        name varchar(2048),
        age int,
        PRIMARY KEY (id)
    ); 
        ''')

    binlog_replicator_runner = BinlogReplicatorRunner(cfg_file=config_file)
    binlog_replicator_runner.run()
    db_replicator_runner = DbReplicatorRunner(TEST_DB_NAME, cfg_file=config_file)
    db_replicator_runner.run()

    time.sleep(1)

    mysql.execute(f"INSERT INTO `{TEST_TABLE_NAME}` (name, age) VALUES ('TEST_VALUE_1', 33);", commit=True)

    def _get_last_insert_name():
        record = get_last_insert_from_binlog(cfg=cfg, db_name=TEST_DB_NAME)
        if record is None:
            return None
        return record[1].decode('utf-8')

    assert_wait(lambda: _get_last_insert_name() == 'TEST_VALUE_1', retry_interval=0.5)
    
    # Wait for the database and table to be created in ClickHouse
    assert_wait(lambda: TEST_DB_NAME in ch.get_databases(), retry_interval=0.5)
    ch.execute_command(f'USE `{TEST_DB_NAME}`')
    assert_wait(lambda: TEST_TABLE_NAME in ch.get_tables(), retry_interval=0.5)
    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 1, retry_interval=0.5)

    binlog_replicator_runner.stop()
    db_replicator_runner.stop()

    time.sleep(1)

    print("populating mysql data")

    base_value = 'a' * 2000

    for i in range(num_records):
        if i % 2000 == 0:
            print(f'populated {i} elements')
        mysql.execute(
            f"INSERT INTO `{TEST_TABLE_NAME}` (name, age) "
            f"VALUES ('TEST_VALUE_{i}_{base_value}', {i});", commit=i % 20 == 0,
        )

    mysql.execute(f"INSERT INTO `{TEST_TABLE_NAME}` (name, age) VALUES ('TEST_VALUE_FINAL', 0);", commit=True)

    print("running binlog_replicator")
    t1 = time.time()
    binlog_replicator_runner = BinlogReplicatorRunner(cfg_file=config_file)
    binlog_replicator_runner.run()

    assert_wait(lambda: _get_last_insert_name() == 'TEST_VALUE_FINAL', retry_interval=0.5, max_wait_time=1000)
    t2 = time.time()

    binlog_replicator_runner.stop()

    time_delta = t2 - t1
    rps = num_records / time_delta

    print('\n\n')
    print("*****************************")
    print("Binlog Replicator Performance:")
    print("records per second:", int(rps))
    print("total time (seconds):", round(time_delta, 2))
    print("*****************************")
    print('\n\n')

    # Now test db_replicator performance
    print("running db_replicator")
    t1 = time.time()
    db_replicator_runner = DbReplicatorRunner(TEST_DB_NAME, cfg_file=config_file)
    db_replicator_runner.run()

    # Make sure the database and table exist before querying
    assert_wait(lambda: TEST_DB_NAME in ch.get_databases(), retry_interval=0.5)
    ch.execute_command(f'USE `{TEST_DB_NAME}`')
    assert_wait(lambda: TEST_TABLE_NAME in ch.get_tables(), retry_interval=0.5)
    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == num_records + 2, retry_interval=0.5, max_wait_time=1000)
    t2 = time.time()

    db_replicator_runner.stop()

    time_delta = t2 - t1
    rps = num_records / time_delta

    print('\n\n')
    print("*****************************")
    print("DB Replicator Performance:")
    print("records per second:", int(rps))
    print("total time (seconds):", round(time_delta, 2))
    print("*****************************")
    print('\n\n')


def test_alter_tokens_split():
    examples = [
        # basic examples from the prompt:
        ("test_name VARCHAR(254) NULL", ["test_name", "VARCHAR(254)", "NULL"]),
        ("factor NUMERIC(5, 2) DEFAULT NULL", ["factor", "NUMERIC(5, 2)", "DEFAULT", "NULL"]),
        # backquoted column name:
        ("`test_name` VARCHAR(254) NULL", ["`test_name`", "VARCHAR(254)", "NULL"]),
        ("`order` INT NOT NULL", ["`order`", "INT", "NOT", "NULL"]),
        # type that contains a parenthesized list with quoted values:
        ("status ENUM('active','inactive') DEFAULT 'active'",
         ["status", "ENUM('active','inactive')", "DEFAULT", "'active'"]),
        # multi‐word type definitions:
        ("col DOUBLE PRECISION DEFAULT 0", ["col", "DOUBLE PRECISION", "DEFAULT", "0"]),
        ("col INT UNSIGNED DEFAULT 0", ["col", "INT UNSIGNED", "DEFAULT", "0"]),
        # a case with a quoted string containing spaces and punctuation:
        ("message VARCHAR(100) DEFAULT 'Hello, world!'",
         ["message", "VARCHAR(100)", "DEFAULT", "'Hello, world!'"]),
        # longer definition with more options:
        ("col DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
         ["col", "DATETIME", "DEFAULT", "CURRENT_TIMESTAMP", "ON", "UPDATE", "CURRENT_TIMESTAMP"]),
        # type with a COMMENT clause (here the type is given, then a parameter keyword)
        ("col VARCHAR(100) COMMENT 'This is a test comment'",
         ["col", "VARCHAR(100)", "COMMENT", "'This is a test comment'"]),
        ("c1 INT FIRST", ["c1", "INT", "FIRST"]),
    ]

    for sql, expected in examples:
        result = MysqlToClickhouseConverter._tokenize_alter_query(sql)
        print("SQL Input:  ", sql)
        print("Expected:   ", expected)
        print("Tokenized:  ", result)
        print("Match?     ", result == expected)
        print("-" * 60)
        assert result == expected


def test_enum_conversion():
    """
    Test that enum values are properly converted to lowercase in ClickHouse
    and that zero values are preserved rather than converted to first enum value.
    """
    config_file = CONFIG_FILE
    cfg = config.Settings()
    cfg.load(config_file)
    mysql_config = cfg.mysql
    clickhouse_config = cfg.clickhouse
    mysql = mysql_api.MySQLApi(
        database=None,
        mysql_settings=mysql_config
    )
    ch = clickhouse_api.ClickhouseApi(
        database=TEST_DB_NAME,
        clickhouse_settings=clickhouse_config
    )

    prepare_env(cfg, mysql, ch)

    mysql.execute(f'''
    CREATE TABLE `{TEST_TABLE_NAME}` (
        id INT NOT NULL AUTO_INCREMENT, 
        status_mixed_case ENUM('Purchase','Sell','Transfer') NOT NULL,
        status_empty ENUM('Yes','No','Maybe'),
        PRIMARY KEY (id)
    )
    ''')

    # Insert values with mixed case and NULL values
    mysql.execute(f'''
    INSERT INTO `{TEST_TABLE_NAME}` (status_mixed_case, status_empty) VALUES 
    ('Purchase', 'Yes'),
    ('Sell', NULL),
    ('Transfer', NULL);
    ''', commit=True)

    run_all_runner = RunAllRunner(cfg_file=config_file)
    run_all_runner.run()

    assert_wait(lambda: TEST_DB_NAME in ch.get_databases())
    ch.execute_command(f'USE `{TEST_DB_NAME}`')
    assert_wait(lambda: TEST_TABLE_NAME in ch.get_tables())
    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 3)

    # Get the ClickHouse data
    results = ch.select(TEST_TABLE_NAME)
    
    # Verify all values are properly converted
    assert results[0]['status_mixed_case'] == 'purchase'
    assert results[1]['status_mixed_case'] == 'sell'
    assert results[2]['status_mixed_case'] == 'transfer'
    
    # Status_empty should handle NULL values correctly
    assert results[0]['status_empty'] == 'yes'
    assert results[1]['status_empty'] is None
    assert results[2]['status_empty'] is None

    run_all_runner.stop()
    assert_wait(lambda: 'stopping db_replicator' in read_logs(TEST_DB_NAME))
    assert('Traceback' not in read_logs(TEST_DB_NAME))
    

def test_polygon_type():
    """
    Test that polygon type is properly converted and handled between MySQL and ClickHouse.
    Tests both the type conversion and data handling for polygon values.
    """
    config_file = CONFIG_FILE
    cfg = config.Settings()
    cfg.load(config_file)
    mysql_config = cfg.mysql
    clickhouse_config = cfg.clickhouse
    mysql = mysql_api.MySQLApi(
        database=None,
        mysql_settings=mysql_config
    )
    ch = clickhouse_api.ClickhouseApi(
        database=TEST_DB_NAME,
        clickhouse_settings=clickhouse_config
    )

    prepare_env(cfg, mysql, ch)

    # Create a table with polygon type
    mysql.execute(f'''
    CREATE TABLE `{TEST_TABLE_NAME}` (
        id INT NOT NULL AUTO_INCREMENT,
        name VARCHAR(50) NOT NULL,
        area POLYGON NOT NULL,
        nullable_area POLYGON,
        PRIMARY KEY (id)
    )
    ''')

    # Insert test data with polygons
    # Using ST_GeomFromText to create polygons from WKT (Well-Known Text) format
    mysql.execute(f'''
    INSERT INTO `{TEST_TABLE_NAME}` (name, area, nullable_area) VALUES 
    ('Square', ST_GeomFromText('POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))'), ST_GeomFromText('POLYGON((0 0, 0 2, 2 2, 2 0, 0 0))')),
    ('Triangle', ST_GeomFromText('POLYGON((0 0, 1 0, 0.5 1, 0 0))'), NULL),
    ('Complex', ST_GeomFromText('POLYGON((0 0, 0 3, 3 3, 3 0, 0 0))'), ST_GeomFromText('POLYGON((1 1, 1 2, 2 2, 2 1, 1 1))'));
    ''', commit=True)

    run_all_runner = RunAllRunner(cfg_file=config_file)
    run_all_runner.run()

    assert_wait(lambda: TEST_DB_NAME in ch.get_databases())
    ch.execute_command(f'USE `{TEST_DB_NAME}`')
    assert_wait(lambda: TEST_TABLE_NAME in ch.get_tables())
    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 3)

    # Get the ClickHouse data
    results = ch.select(TEST_TABLE_NAME)
    
    # Verify the data
    assert len(results) == 3
    
    # Check first row (Square)
    assert results[0]['name'] == 'Square'
    assert len(results[0]['area']) == 5  # Square has 5 points (including closing point)
    assert len(results[0]['nullable_area']) == 5
    # Verify some specific points
    assert results[0]['area'][0] == {'x': 0.0, 'y': 0.0}
    assert results[0]['area'][1] == {'x': 0.0, 'y': 1.0}
    assert results[0]['area'][2] == {'x': 1.0, 'y': 1.0}
    assert results[0]['area'][3] == {'x': 1.0, 'y': 0.0}
    assert results[0]['area'][4] == {'x': 0.0, 'y': 0.0}  # Closing point
    
    # Check second row (Triangle)
    assert results[1]['name'] == 'Triangle'
    assert len(results[1]['area']) == 4  # Triangle has 4 points (including closing point)
    assert results[1]['nullable_area'] == []  # NULL values are returned as empty list
    # Verify some specific points
    assert results[1]['area'][0] == {'x': 0.0, 'y': 0.0}
    assert results[1]['area'][1] == {'x': 1.0, 'y': 0.0}
    assert results[1]['area'][2] == {'x': 0.5, 'y': 1.0}
    assert results[1]['area'][3] == {'x': 0.0, 'y': 0.0}  # Closing point
    
    # Check third row (Complex)
    assert results[2]['name'] == 'Complex'
    assert len(results[2]['area']) == 5  # Outer square
    assert len(results[2]['nullable_area']) == 5  # Inner square
    # Verify some specific points
    assert results[2]['area'][0] == {'x': 0.0, 'y': 0.0}
    assert results[2]['area'][2] == {'x': 3.0, 'y': 3.0}
    assert results[2]['nullable_area'][0] == {'x': 1.0, 'y': 1.0}
    assert results[2]['nullable_area'][2] == {'x': 2.0, 'y': 2.0}

    # Test realtime replication by adding more records
    mysql.execute(f'''
    INSERT INTO `{TEST_TABLE_NAME}` (name, area, nullable_area) VALUES 
    ('Pentagon', ST_GeomFromText('POLYGON((0 0, 1 0, 1.5 1, 0.5 1.5, 0 0))'), ST_GeomFromText('POLYGON((0.2 0.2, 0.8 0.2, 1 0.8, 0.5 1, 0.2 0.2))')),
    ('Hexagon', ST_GeomFromText('POLYGON((0 0, 1 0, 1.5 0.5, 1 1, 0.5 1, 0 0))'), NULL),
    ('Circle', ST_GeomFromText('POLYGON((0 0, 0 2, 2 2, 2 0, 0 0))'), ST_GeomFromText('POLYGON((0.5 0.5, 0.5 1.5, 1.5 1.5, 1.5 0.5, 0.5 0.5))'));
    ''', commit=True)

    # Wait for new records to be replicated
    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 6)

    # Verify the new records using WHERE clauses
    # Check Pentagon
    pentagon = ch.select(TEST_TABLE_NAME, where="name='Pentagon'")[0]
    assert pentagon['name'] == 'Pentagon'
    assert len(pentagon['area']) == 5  # Pentagon has 5 points
    assert len(pentagon['nullable_area']) == 5  # Inner pentagon
    assert abs(pentagon['area'][0]['x'] - 0.0) < 1e-6
    assert abs(pentagon['area'][0]['y'] - 0.0) < 1e-6
    assert abs(pentagon['area'][2]['x'] - 1.5) < 1e-6
    assert abs(pentagon['area'][2]['y'] - 1.0) < 1e-6
    assert abs(pentagon['nullable_area'][0]['x'] - 0.2) < 1e-6
    assert abs(pentagon['nullable_area'][0]['y'] - 0.2) < 1e-6
    assert abs(pentagon['nullable_area'][2]['x'] - 1.0) < 1e-6
    assert abs(pentagon['nullable_area'][2]['y'] - 0.8) < 1e-6
    
    # Check Hexagon
    hexagon = ch.select(TEST_TABLE_NAME, where="name='Hexagon'")[0]
    assert hexagon['name'] == 'Hexagon'
    assert len(hexagon['area']) == 6  # Hexagon has 6 points
    assert hexagon['nullable_area'] == []  # NULL values are returned as empty list
    assert abs(hexagon['area'][0]['x'] - 0.0) < 1e-6
    assert abs(hexagon['area'][0]['y'] - 0.0) < 1e-6
    assert abs(hexagon['area'][2]['x'] - 1.5) < 1e-6
    assert abs(hexagon['area'][2]['y'] - 0.5) < 1e-6
    assert abs(hexagon['area'][4]['x'] - 0.5) < 1e-6
    assert abs(hexagon['area'][4]['y'] - 1.0) < 1e-6
    
    # Check Circle
    circle = ch.select(TEST_TABLE_NAME, where="name='Circle'")[0]
    assert circle['name'] == 'Circle'
    assert len(circle['area']) == 5  # Outer square
    assert len(circle['nullable_area']) == 5  # Inner square
    assert abs(circle['area'][0]['x'] - 0.0) < 1e-6
    assert abs(circle['area'][0]['y'] - 0.0) < 1e-6
    assert abs(circle['area'][2]['x'] - 2.0) < 1e-6
    assert abs(circle['area'][2]['y'] - 2.0) < 1e-6
    assert abs(circle['nullable_area'][0]['x'] - 0.5) < 1e-6
    assert abs(circle['nullable_area'][0]['y'] - 0.5) < 1e-6
    assert abs(circle['nullable_area'][2]['x'] - 1.5) < 1e-6
    assert abs(circle['nullable_area'][2]['y'] - 1.5) < 1e-6

    run_all_runner.stop()
    assert_wait(lambda: 'stopping db_replicator' in read_logs(TEST_DB_NAME))
    assert('Traceback' not in read_logs(TEST_DB_NAME))

@pytest.mark.parametrize("query,expected", [
    ("CREATE TABLE `mydb`.`mytable` (id INT)", "mydb"),
    ("CREATE TABLE mydb.mytable (id INT)", "mydb"),
    ("ALTER TABLE `mydb`.mytable ADD COLUMN name VARCHAR(50)", "mydb"),
    ("CREATE TABLE IF NOT EXISTS mydb.mytable (id INT)", "mydb"),
    ("CREATE TABLE mytable (id INT)", ""),
    ("  CREATE   TABLE    `mydb`   .   `mytable` \n ( id INT )", "mydb"),
    ('ALTER TABLE "testdb"."tablename" ADD COLUMN flag BOOLEAN', "testdb"),
    ("create table mydb.mytable (id int)", "mydb"),
    ("DROP DATABASE mydb", ""),
    ("CREATE TABLE mydbmytable (id int)", ""),  # missing dot between DB and table
    ("""
        CREATE TABLE IF NOT EXISTS
        `multidb`
        .
        `multitable`
        (
          id INT,
          name VARCHAR(100)
        )
    """, "multidb"),
    ("""
        ALTER TABLE
        `justtable`
        ADD COLUMN age INT;
    """, ""),
    ("""
    CREATE TABLE `replication-test_db`.`test_table_2` (
        `id` int unsigned NOT NULL AUTO_INCREMENT,
        name varchar(255),
        PRIMARY KEY (id)
    )
    """, "replication-test_db"),
    ("BEGIN", ""),
])
def test_parse_db_name_from_query(query, expected):
    assert BinlogReplicator._try_parse_db_name_from_query(query) == expected


def test_create_table_like():
    """
    Test that CREATE TABLE ... LIKE statements are handled correctly.
    The test creates a source table, then creates another table using LIKE,
    and verifies that both tables have the same structure in ClickHouse.
    """
    config_file = CONFIG_FILE
    cfg = config.Settings()
    cfg.load(config_file)

    mysql = mysql_api.MySQLApi(
        database=None,
        mysql_settings=cfg.mysql,
    )

    ch = clickhouse_api.ClickhouseApi(
        database=TEST_DB_NAME,
        clickhouse_settings=cfg.clickhouse,
    )

    prepare_env(cfg, mysql, ch)
    mysql.set_database(TEST_DB_NAME)

    # Create the source table with a complex structure
    mysql.execute(f'''
    CREATE TABLE `source_table` (
        id INT NOT NULL AUTO_INCREMENT,
        name VARCHAR(255) NOT NULL,
        age INT UNSIGNED,
        email VARCHAR(100) UNIQUE,
        status ENUM('active','inactive','pending') DEFAULT 'active',
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        data JSON,
        PRIMARY KEY (id)
    );
    ''')
    
    # Get the CREATE statement for the source table
    source_create = mysql.get_table_create_statement('source_table')
    
    # Create a table using LIKE statement
    mysql.execute(f'''
    CREATE TABLE `derived_table` LIKE `source_table`;
    ''')

    # Set up replication
    binlog_replicator_runner = BinlogReplicatorRunner(cfg_file=config_file)
    binlog_replicator_runner.run()
    db_replicator_runner = DbReplicatorRunner(TEST_DB_NAME, cfg_file=config_file)
    db_replicator_runner.run()

    # Wait for database to be created and renamed from tmp to final
    assert_wait(lambda: TEST_DB_NAME in ch.get_databases(), max_wait_time=10.0)
    
    # Use the correct database explicitly
    ch.execute_command(f'USE `{TEST_DB_NAME}`')

    # Wait for tables to be created in ClickHouse with a longer timeout
    assert_wait(lambda: 'source_table' in ch.get_tables(), max_wait_time=10.0)
    assert_wait(lambda: 'derived_table' in ch.get_tables(), max_wait_time=10.0)

    # Insert data into both tables to verify they work
    mysql.execute("INSERT INTO `source_table` (name, age, email, status) VALUES ('Alice', 30, 'alice@example.com', 'active');", commit=True)
    mysql.execute("INSERT INTO `derived_table` (name, age, email, status) VALUES ('Bob', 25, 'bob@example.com', 'pending');", commit=True)

    # Wait for data to be replicated
    assert_wait(lambda: len(ch.select('source_table')) == 1, max_wait_time=10.0)
    assert_wait(lambda: len(ch.select('derived_table')) == 1, max_wait_time=10.0)

    # Compare structures by reading descriptions in ClickHouse
    source_desc = ch.execute_command("DESCRIBE TABLE source_table")
    derived_desc = ch.execute_command("DESCRIBE TABLE derived_table")

    # The structures should be identical
    assert source_desc == derived_desc
    
    # Verify the data in both tables
    source_data = ch.select('source_table')[0]
    derived_data = ch.select('derived_table')[0]
    
    assert source_data['name'] == 'Alice'
    assert derived_data['name'] == 'Bob'
    
    # Both tables should have same column types
    assert type(source_data['id']) == type(derived_data['id'])
    assert type(source_data['name']) == type(derived_data['name'])
    assert type(source_data['age']) == type(derived_data['age'])
    
    # Now test realtime replication by creating a new table after the initial replication
    mysql.execute(f'''
    CREATE TABLE `realtime_table` (
        id INT NOT NULL AUTO_INCREMENT,
        title VARCHAR(100) NOT NULL,
        description TEXT,
        price DECIMAL(10,2),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (id)
    );
    ''')
    
    # Wait for the new table to be created in ClickHouse
    assert_wait(lambda: 'realtime_table' in ch.get_tables(), max_wait_time=10.0)
    
    # Insert data into the new table
    mysql.execute("""
    INSERT INTO `realtime_table` (title, description, price) VALUES 
    ('Product 1', 'First product description', 19.99),
    ('Product 2', 'Second product description', 29.99),
    ('Product 3', 'Third product description', 39.99);
    """, commit=True)
    
    # Wait for data to be replicated
    assert_wait(lambda: len(ch.select('realtime_table')) == 3, max_wait_time=10.0)
    
    # Verify the data in the realtime table
    realtime_data = ch.select('realtime_table')
    assert len(realtime_data) == 3
    
    # Verify specific values
    products = sorted([record['title'] for record in realtime_data])
    assert products == ['Product 1', 'Product 2', 'Product 3']
    
    prices = sorted([float(record['price']) for record in realtime_data])
    assert prices == [19.99, 29.99, 39.99]
    
    # Now create another table using LIKE after initial replication
    mysql.execute(f'''
    CREATE TABLE `realtime_like_table` LIKE `realtime_table`;
    ''')
    
    # Wait for the new LIKE table to be created in ClickHouse
    assert_wait(lambda: 'realtime_like_table' in ch.get_tables(), max_wait_time=10.0)
    
    # Insert data into the new LIKE table
    mysql.execute("""
    INSERT INTO `realtime_like_table` (title, description, price) VALUES 
    ('Service A', 'Premium service', 99.99),
    ('Service B', 'Standard service', 49.99);
    """, commit=True)
    
    # Wait for data to be replicated
    assert_wait(lambda: len(ch.select('realtime_like_table')) == 2, max_wait_time=10.0)
    
    # Verify the data in the realtime LIKE table
    like_data = ch.select('realtime_like_table')
    assert len(like_data) == 2
    
    services = sorted([record['title'] for record in like_data])
    assert services == ['Service A', 'Service B']
    
    # Clean up
    db_replicator_runner.stop()
    binlog_replicator_runner.stop()


def test_year_type():
    """
    Test that MySQL YEAR type is properly converted to UInt16 in ClickHouse
    and that year values are correctly handled.
    """
    config_file = CONFIG_FILE
    cfg = config.Settings()
    cfg.load(config_file)
    mysql_config = cfg.mysql
    clickhouse_config = cfg.clickhouse
    mysql = mysql_api.MySQLApi(
        database=None,
        mysql_settings=mysql_config
    )
    ch = clickhouse_api.ClickhouseApi(
        database=TEST_DB_NAME,
        clickhouse_settings=clickhouse_config
    )

    prepare_env(cfg, mysql, ch)

    mysql.execute(f'''
    CREATE TABLE `{TEST_TABLE_NAME}` (
        id INT NOT NULL AUTO_INCREMENT,
        year_field YEAR NOT NULL,
        nullable_year YEAR,
        PRIMARY KEY (id)
    )
    ''')

    # Insert test data with various year values
    mysql.execute(f'''
    INSERT INTO `{TEST_TABLE_NAME}` (year_field, nullable_year) VALUES 
    (2024, 2024),
    (1901, NULL),
    (2155, 2000),
    (2000, 1999);
    ''', commit=True)

    run_all_runner = RunAllRunner(cfg_file=config_file)
    run_all_runner.run()

    assert_wait(lambda: TEST_DB_NAME in ch.get_databases())
    ch.execute_command(f'USE `{TEST_DB_NAME}`')
    assert_wait(lambda: TEST_TABLE_NAME in ch.get_tables())
    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 4)

    # Get the ClickHouse data
    results = ch.select(TEST_TABLE_NAME)
    
    # Verify the data
    assert results[0]['year_field'] == 2024
    assert results[0]['nullable_year'] == 2024
    assert results[1]['year_field'] == 1901
    assert results[1]['nullable_year'] is None
    assert results[2]['year_field'] == 2155
    assert results[2]['nullable_year'] == 2000
    assert results[3]['year_field'] == 2000
    assert results[3]['nullable_year'] == 1999

    # Test realtime replication by adding more records
    mysql.execute(f'''
    INSERT INTO `{TEST_TABLE_NAME}` (year_field, nullable_year) VALUES 
    (2025, 2025),
    (1999, NULL),
    (2100, 2100);
    ''', commit=True)

    # Wait for new records to be replicated
    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 7)

    # Verify the new records - include order by in the where clause
    new_results = ch.select(TEST_TABLE_NAME, where="year_field >= 2025 ORDER BY year_field ASC")
    assert len(new_results) == 3
    
    # Check specific values
    assert new_results[0]['year_field'] == 2025
    assert new_results[0]['nullable_year'] == 2025
    assert new_results[1]['year_field'] == 2100
    assert new_results[1]['nullable_year'] == 2100
    assert new_results[2]['year_field'] == 2155
    assert new_results[2]['nullable_year'] == 2000

    run_all_runner.stop()
    assert_wait(lambda: 'stopping db_replicator' in read_logs(TEST_DB_NAME))
    assert('Traceback' not in read_logs(TEST_DB_NAME))


@pytest.mark.optional
def test_performance_initial_only_replication():
    config_file = 'tests_config_perf.yaml'
    num_records = 300000

    cfg = config.Settings()
    cfg.load(config_file)

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
        id int NOT NULL AUTO_INCREMENT,
        name varchar(2048),
        age int,
        PRIMARY KEY (id)
    ); 
    ''')

    print("populating mysql data")

    base_value = 'a' * 2000

    for i in range(num_records):
        if i % 2000 == 0:
            print(f'populated {i} elements')
        mysql.execute(
            f"INSERT INTO `{TEST_TABLE_NAME}` (name, age) "
            f"VALUES ('TEST_VALUE_{i}_{base_value}', {i});", commit=i % 20 == 0,
        )

    mysql.execute(f"INSERT INTO `{TEST_TABLE_NAME}` (name, age) VALUES ('TEST_VALUE_FINAL', 0);", commit=True)
    print(f"finished populating {num_records} records")

    # Now test db_replicator performance in initial_only mode
    print("running db_replicator in initial_only mode")
    t1 = time.time()
    
    db_replicator_runner = DbReplicatorRunner(
        TEST_DB_NAME, 
        additional_arguments='--initial_only=True',
        cfg_file=config_file
    )
    db_replicator_runner.run()
    db_replicator_runner.wait_complete()  # Wait for the process to complete

    # Make sure the database and table exist
    assert_wait(lambda: TEST_DB_NAME in ch.get_databases(), retry_interval=0.5)
    ch.execute_command(f'USE `{TEST_DB_NAME}`')
    assert_wait(lambda: TEST_TABLE_NAME in ch.get_tables(), retry_interval=0.5)
    
    # Check that all records were replicated
    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == num_records + 1, retry_interval=0.5, max_wait_time=300)
    
    t2 = time.time()

    time_delta = t2 - t1
    rps = num_records / time_delta

    print('\n\n')
    print("*****************************")
    print("DB Replicator Initial Only Mode Performance:")
    print("records per second:", int(rps))
    print("total time (seconds):", round(time_delta, 2))
    print("*****************************")
    print('\n\n')
    
    # Clean up
    ch.drop_database(TEST_DB_NAME)
    
    # Now test with parallel replication
    # Set initial_replication_threads in the config
    print("running db_replicator with parallel initial replication")
    
    t1 = time.time()
    
    # Create a custom config file for testing with parallel replication
    parallel_config_file = 'tests_config_perf_parallel.yaml'
    if os.path.exists(parallel_config_file):
        os.remove(parallel_config_file)

    with open(config_file, 'r') as src_file:
        config_content = src_file.read()
    config_content += f"\ninitial_replication_threads: 8\n"
    with open(parallel_config_file, 'w') as dest_file:
        dest_file.write(config_content)
    
    # Use the DbReplicator directly to test the new parallel implementation
    db_replicator_runner = DbReplicatorRunner(
        TEST_DB_NAME, 
        cfg_file=parallel_config_file
    )
    db_replicator_runner.run()
    
    # Make sure the database and table exist
    assert_wait(lambda: TEST_DB_NAME in ch.get_databases(), retry_interval=0.5)
    ch.execute_command(f'USE `{TEST_DB_NAME}`')
    assert_wait(lambda: TEST_TABLE_NAME in ch.get_tables(), retry_interval=0.5)
    
    # Check that all records were replicated
    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == num_records + 1, retry_interval=0.5, max_wait_time=300)
    
    t2 = time.time()
    
    time_delta = t2 - t1
    rps = num_records / time_delta
    
    print('\n\n')
    print("*****************************")
    print("DB Replicator Parallel Mode Performance:")
    print("workers:", cfg.initial_replication_threads)
    print("records per second:", int(rps))
    print("total time (seconds):", round(time_delta, 2))
    print("*****************************")
    print('\n\n')
    
    db_replicator_runner.stop()
    
    # Clean up the temporary config file
    os.remove(parallel_config_file)


def test_schema_evolution_with_db_mapping():
    """Test case to reproduce issue where schema evolution doesn't work with database mapping."""
    # Use the predefined config file with database mapping
    config_file = "tests_config_db_mapping.yaml"
    
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
    config_path = 'tests_config_dynamic_column.yaml'
    
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


def test_ignore_deletes():
    # Create a temporary config file with ignore_deletes=True
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as temp_config_file:
        config_file = temp_config_file.name
        
        # Read the original config
        with open(CONFIG_FILE, 'r') as original_config:
            config_data = yaml.safe_load(original_config)
        
        # Add ignore_deletes=True
        config_data['ignore_deletes'] = True
        
        # Write to the temp file
        yaml.dump(config_data, temp_config_file)

    try:
        cfg = config.Settings()
        cfg.load(config_file)
        
        # Verify the ignore_deletes option was set
        assert cfg.ignore_deletes is True

        mysql = mysql_api.MySQLApi(
            database=None,
            mysql_settings=cfg.mysql,
        )

        ch = clickhouse_api.ClickhouseApi(
            database=TEST_DB_NAME,
            clickhouse_settings=cfg.clickhouse,
        )

        prepare_env(cfg, mysql, ch)

        # Create a table with a composite primary key
        mysql.execute(f'''
        CREATE TABLE `{TEST_TABLE_NAME}` (
            departments int(11) NOT NULL,
            termine int(11) NOT NULL,
            data varchar(255) NOT NULL,
            PRIMARY KEY (departments,termine)
        )
        ''')

        # Insert initial records
        mysql.execute(f"INSERT INTO `{TEST_TABLE_NAME}` (departments, termine, data) VALUES (10, 20, 'data1');", commit=True)
        mysql.execute(f"INSERT INTO `{TEST_TABLE_NAME}` (departments, termine, data) VALUES (30, 40, 'data2');", commit=True)
        mysql.execute(f"INSERT INTO `{TEST_TABLE_NAME}` (departments, termine, data) VALUES (50, 60, 'data3');", commit=True)

        # Run the replicator with ignore_deletes=True
        run_all_runner = RunAllRunner(cfg_file=config_file)
        run_all_runner.run()

        # Wait for replication to complete
        assert_wait(lambda: TEST_DB_NAME in ch.get_databases())
        ch.execute_command(f'USE `{TEST_DB_NAME}`')
        assert_wait(lambda: TEST_TABLE_NAME in ch.get_tables())
        assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 3)

        # Delete some records from MySQL
        mysql.execute(f"DELETE FROM `{TEST_TABLE_NAME}` WHERE departments=10;", commit=True)
        mysql.execute(f"DELETE FROM `{TEST_TABLE_NAME}` WHERE departments=30;", commit=True)
        
        # Wait a moment to ensure replication processes the events
        time.sleep(5)
        
        # Verify records are NOT deleted in ClickHouse (since ignore_deletes=True)
        # The count should still be 3
        assert len(ch.select(TEST_TABLE_NAME)) == 3, "Deletions were processed despite ignore_deletes=True"
        
        # Insert a new record and verify it's added
        mysql.execute(f"INSERT INTO `{TEST_TABLE_NAME}` (departments, termine, data) VALUES (70, 80, 'data4');", commit=True)
        assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 4)
        
        # Verify the new record is correctly added
        result = ch.select(TEST_TABLE_NAME, where="departments=70 AND termine=80")
        assert len(result) == 1
        assert result[0]['data'] == 'data4'
        
        # Clean up
        run_all_runner.stop()
        
        # Verify no errors occurred
        assert_wait(lambda: 'stopping db_replicator' in read_logs(TEST_DB_NAME))
        assert('Traceback' not in read_logs(TEST_DB_NAME))
        
        # Additional tests for persistence after restart
        
        # 1. Remove all entries from table in MySQL
        mysql.execute(f"DELETE FROM `{TEST_TABLE_NAME}` WHERE 1=1;", commit=True)

                # Add a new row in MySQL before starting the replicator
        mysql.execute(f"INSERT INTO `{TEST_TABLE_NAME}` (departments, termine, data) VALUES (110, 120, 'offline_data');", commit=True)
        
        # 2. Wait 5 seconds
        time.sleep(5)
        
        # 3. Remove binlog directory (similar to prepare_env, but without removing tables)
        if os.path.exists(cfg.binlog_replicator.data_dir):
            shutil.rmtree(cfg.binlog_replicator.data_dir)
        os.mkdir(cfg.binlog_replicator.data_dir)
        

        # 4. Create and run a new runner
        new_runner = RunAllRunner(cfg_file=config_file)
        new_runner.run()
        
        # 5. Ensure it has all the previous data (should still be 4 records from before + 1 new offline record)
        assert_wait(lambda: TEST_DB_NAME in ch.get_databases())
        ch.execute_command(f'USE `{TEST_DB_NAME}`')
        assert_wait(lambda: TEST_TABLE_NAME in ch.get_tables())
        assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 5)
        
        # Verify we still have all the old data
        assert len(ch.select(TEST_TABLE_NAME, where="departments=10 AND termine=20")) == 1
        assert len(ch.select(TEST_TABLE_NAME, where="departments=30 AND termine=40")) == 1
        assert len(ch.select(TEST_TABLE_NAME, where="departments=50 AND termine=60")) == 1
        assert len(ch.select(TEST_TABLE_NAME, where="departments=70 AND termine=80")) == 1
        
        # Verify the offline data was replicated
        assert len(ch.select(TEST_TABLE_NAME, where="departments=110 AND termine=120")) == 1
        offline_data = ch.select(TEST_TABLE_NAME, where="departments=110 AND termine=120")[0]
        assert offline_data['data'] == 'offline_data'
        
        # 6. Insert new data and verify it gets added to existing data
        mysql.execute(f"INSERT INTO `{TEST_TABLE_NAME}` (departments, termine, data) VALUES (90, 100, 'data5');", commit=True)
        assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 6)
        
        # Verify the combined old and new data
        result = ch.select(TEST_TABLE_NAME, where="departments=90 AND termine=100")
        assert len(result) == 1
        assert result[0]['data'] == 'data5'
        
        # Make sure we have all 6 records (4 original + 1 offline + 1 new one)
        assert len(ch.select(TEST_TABLE_NAME)) == 6
        
        new_runner.stop()
    finally:
        # Clean up the temporary config file
        os.unlink(config_file)

def test_issue_160_unknown_mysql_type_bug():
    """
    Test to reproduce the bug from issue #160.
    
    Bug Description: Replication fails when adding a new table during realtime replication
    with Exception: unknown mysql type ""
    
    This test should FAIL until the bug is fixed.
    When the bug is present: parsing will fail with unknown mysql type and the test will FAIL
    When the bug is fixed: parsing will succeed and the test will PASS
    """
    # The exact CREATE TABLE statement from the bug report
    create_table_query = """create table test_table
(
    id    bigint          not null,
    col_a datetime(6)     not null,
    col_b datetime(6)     null,
    col_c varchar(255)    not null,
    col_d varchar(255)    not null,
    col_e int             not null,
    col_f decimal(20, 10) not null,
    col_g decimal(20, 10) not null,
    col_h datetime(6)     not null,
    col_i date            not null,
    col_j varchar(255)    not null,
    col_k varchar(255)    not null,
    col_l bigint          not null,
    col_m varchar(50)     not null,
    col_n bigint          null,
    col_o decimal(20, 1)  null,
    col_p date            null,
    primary key (id, col_e)
);"""

    # Create a converter instance
    converter = MysqlToClickhouseConverter()
    
    # This should succeed when the bug is fixed
    # When the bug is present, this will raise "unknown mysql type """ and the test will FAIL
    mysql_structure, ch_structure = converter.parse_create_table_query(create_table_query)
    
    # Verify the parsing worked correctly
    assert mysql_structure.table_name == 'test_table'
    assert len(mysql_structure.fields) == 17  # All columns should be parsed
    assert mysql_structure.primary_keys == ['id', 'col_e']

def test_truncate_operation_bug_issue_155():
    """
    Test to reproduce the bug from issue #155.
    
    Bug Description: TRUNCATE operation is not replicated - data is not cleared on ClickHouse side
    
    This test should FAIL until the bug is fixed.
    When the bug is present: TRUNCATE will not clear ClickHouse data and the test will FAIL
    When the bug is fixed: TRUNCATE will clear ClickHouse data and the test will PASS
    """
    cfg = config.Settings()
    cfg.load(CONFIG_FILE)

    mysql = mysql_api.MySQLApi(
        database=None,
        mysql_settings=cfg.mysql,
    )

    ch = clickhouse_api.ClickhouseApi(
        database=TEST_DB_NAME,
        clickhouse_settings=cfg.clickhouse,
    )

    prepare_env(cfg, mysql, ch)

    # Create a test table
    mysql.execute(f'''
CREATE TABLE `{TEST_TABLE_NAME}` (
    id int NOT NULL AUTO_INCREMENT,
    name varchar(255),
    age int,
    PRIMARY KEY (id)
); 
    ''')

    # Insert test data
    mysql.execute(f"INSERT INTO `{TEST_TABLE_NAME}` (name, age) VALUES ('Alice', 25);", commit=True)
    mysql.execute(f"INSERT INTO `{TEST_TABLE_NAME}` (name, age) VALUES ('Bob', 30);", commit=True)
    mysql.execute(f"INSERT INTO `{TEST_TABLE_NAME}` (name, age) VALUES ('Charlie', 35);", commit=True)

    # Start replication
    binlog_replicator_runner = BinlogReplicatorRunner()
    binlog_replicator_runner.run()
    db_replicator_runner = DbReplicatorRunner(TEST_DB_NAME)
    db_replicator_runner.run()

    # Wait for initial replication
    assert_wait(lambda: TEST_DB_NAME in ch.get_databases())
    ch.execute_command(f'USE `{TEST_DB_NAME}`')
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
    assert ch_count_after_truncate == 0, f"ClickHouse table should be empty after TRUNCATE, but contains {ch_count_after_truncate} records"

    # Insert new data to verify replication still works after TRUNCATE
    mysql.execute(f"INSERT INTO `{TEST_TABLE_NAME}` (name, age) VALUES ('Dave', 40);", commit=True)
    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 1)
    
    # Verify the new record
    new_record = ch.select(TEST_TABLE_NAME, where="name='Dave'")
    assert len(new_record) == 1
    assert new_record[0]['age'] == 40

    # Clean up
    db_replicator_runner.stop()
    binlog_replicator_runner.stop()

def test_json2():
    cfg = config.Settings()
    cfg.load(CONFIG_FILE)

    mysql = mysql_api.MySQLApi(
        database=None,
        mysql_settings=cfg.mysql,
    )

    ch = clickhouse_api.ClickhouseApi(
        database=TEST_DB_NAME,
        clickhouse_settings=cfg.clickhouse,
    )

    prepare_env(cfg, mysql, ch)

    mysql.execute("SET sql_mode = 'ALLOW_INVALID_DATES';")

    mysql.execute(f'''
CREATE TABLE `{TEST_TABLE_NAME}` (
    `id` int unsigned NOT NULL AUTO_INCREMENT,
    name varchar(255),
    data json,
    PRIMARY KEY (id)
); 
    ''')

    mysql.execute(
        f"INSERT INTO `{TEST_TABLE_NAME}` (name, data) VALUES " +
        """('Ivan', '{"а": "б", "в": [1,2,3]}');""",
        commit=True,
    )

    binlog_replicator_runner = BinlogReplicatorRunner()
    binlog_replicator_runner.run()
    db_replicator_runner = DbReplicatorRunner(TEST_DB_NAME)
    db_replicator_runner.run()

    assert_wait(lambda: TEST_DB_NAME in ch.get_databases())

    ch.execute_command(f'USE `{TEST_DB_NAME}`')

    assert_wait(lambda: TEST_TABLE_NAME in ch.get_tables())
    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 1)

    mysql.execute(
        f"INSERT INTO `{TEST_TABLE_NAME}` (name, data) VALUES " +
        """('Peter', '{"в": "б", "а": [3,2,1]}');""",
        commit=True,
    )
    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 2)

    assert json.loads(ch.select(TEST_TABLE_NAME, "name='Ivan'")[0]['data'])['в'] == [1, 2, 3]
    assert json.loads(ch.select(TEST_TABLE_NAME, "name='Peter'")[0]['data'])['в'] == 'б'
    db_replicator_runner.stop()
    binlog_replicator_runner.stop()

def test_timezone_conversion():
    """
    Test that MySQL timestamp fields are converted to ClickHouse DateTime64 with custom timezone.
    This test reproduces the issue from GitHub issue #170.
    """
    # Create a temporary config file with custom timezone
    config_content = """
mysql:
  host: 'localhost'
  port: 9306
  user: 'root'
  password: 'admin'

clickhouse:
  host: 'localhost'
  port: 9123
  user: 'default'
  password: 'admin'

binlog_replicator:
  data_dir: '/app/binlog/'
  records_per_file: 100000

databases: '*test*'
log_level: 'debug'
mysql_timezone: 'America/New_York'
"""
    
    # Create temporary config file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        f.write(config_content)
        temp_config_file = f.name
    
    try:
        cfg = config.Settings()
        cfg.load(temp_config_file)
        
        # Verify timezone is loaded correctly
        assert cfg.mysql_timezone == 'America/New_York'
        
        mysql = mysql_api.MySQLApi(
            database=None,
            mysql_settings=cfg.mysql,
        )

        ch = clickhouse_api.ClickhouseApi(
            database=TEST_DB_NAME,
            clickhouse_settings=cfg.clickhouse,
        )

        prepare_env(cfg, mysql, ch)

        # Create table with timestamp fields
        mysql.execute(f'''
        CREATE TABLE `{TEST_TABLE_NAME}` (
            id int NOT NULL AUTO_INCREMENT,
            name varchar(255),
            created_at timestamp NULL,
            updated_at timestamp(3) NULL,
            PRIMARY KEY (id)
        );
        ''')

        # Insert test data with specific timestamp
        mysql.execute(
            f"INSERT INTO `{TEST_TABLE_NAME}` (name, created_at, updated_at) "
            f"VALUES ('test_timezone', '2023-08-15 14:30:00', '2023-08-15 14:30:00.123');",
            commit=True,
        )

        # Run replication
        run_all_runner = RunAllRunner(cfg_file=temp_config_file)
        run_all_runner.run()

        assert_wait(lambda: TEST_DB_NAME in ch.get_databases())
        ch.execute_command(f'USE `{TEST_DB_NAME}`')
        assert_wait(lambda: TEST_TABLE_NAME in ch.get_tables())
        assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 1)

        # Get the table structure from ClickHouse
        table_info = ch.query(f'DESCRIBE `{TEST_TABLE_NAME}`')
        
        # Check that timestamp fields are converted to DateTime64 with timezone
        created_at_type = None
        updated_at_type = None
        for row in table_info.result_rows:
            if row[0] == 'created_at':
                created_at_type = row[1]
            elif row[0] == 'updated_at':
                updated_at_type = row[1]
        
        # Verify the types include the timezone
        assert created_at_type is not None
        assert updated_at_type is not None
        assert 'America/New_York' in created_at_type
        assert 'America/New_York' in updated_at_type
        
        # Verify data was inserted correctly
        results = ch.select(TEST_TABLE_NAME)
        assert len(results) == 1
        assert results[0]['name'] == 'test_timezone'
        
        run_all_runner.stop()
        
    finally:
        # Clean up temporary config file
        os.unlink(temp_config_file)

def test_resume_initial_replication_with_ignore_deletes():
    """
    Test that resuming initial replication works correctly with ignore_deletes=True.
    
    This reproduces the bug from https://github.com/bakwc/mysql_ch_replicator/issues/172
    where resuming initial replication would fail with "Database sirocco_tmp does not exist"
    when ignore_deletes=True because the code would try to use the _tmp database instead
    of the target database directly.
    """
    # Create a temporary config file with ignore_deletes=True
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as temp_config_file:
        config_file = temp_config_file.name
        
        # Read the original config
        with open(CONFIG_FILE, 'r') as original_config:
            config_data = yaml.safe_load(original_config)
        
        # Add ignore_deletes=True
        config_data['ignore_deletes'] = True
        
        # Set initial_replication_batch_size to 1 for testing
        config_data['initial_replication_batch_size'] = 1
        
        # Write to the temp file
        yaml.dump(config_data, temp_config_file)

    try:
        cfg = config.Settings()
        cfg.load(config_file)
        
        # Verify the ignore_deletes option was set
        assert cfg.ignore_deletes is True

        mysql = mysql_api.MySQLApi(
            database=None,
            mysql_settings=cfg.mysql,
        )

        ch = clickhouse_api.ClickhouseApi(
            database=TEST_DB_NAME,
            clickhouse_settings=cfg.clickhouse,
        )

        prepare_env(cfg, mysql, ch)

        # Create a table with many records to ensure initial replication takes time
        mysql.execute(f'''
        CREATE TABLE `{TEST_TABLE_NAME}` (
            id int NOT NULL AUTO_INCREMENT,
            name varchar(255),
            data varchar(1000),
            PRIMARY KEY (id)
        )
        ''')

        # Insert many records to make initial replication take longer
        for i in range(100):
            mysql.execute(
                f"INSERT INTO `{TEST_TABLE_NAME}` (name, data) VALUES ('test_{i}', 'data_{i}');",
                commit=True
            )

        # Start binlog replicator
        binlog_replicator_runner = BinlogReplicatorRunner(cfg_file=config_file)
        binlog_replicator_runner.run()

        # Start db replicator for initial replication with test flag to exit early
        db_replicator_runner = DbReplicatorRunner(TEST_DB_NAME, cfg_file=config_file, 
                                                 additional_arguments='--initial-replication-test-fail-records 30')
        db_replicator_runner.run()
        
        # Wait for initial replication to start
        assert_wait(lambda: TEST_DB_NAME in ch.get_databases())
        ch.execute_command(f'USE `{TEST_DB_NAME}`')
        assert_wait(lambda: TEST_TABLE_NAME in ch.get_tables())
        
        # Wait for some records to be replicated but not all (should hit the 30 record limit)
        assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) > 0)
        
        # The db replicator should have stopped automatically due to the test flag
        # But we still call stop() to ensure proper cleanup
        db_replicator_runner.stop()
        
        # Verify the state is still PERFORMING_INITIAL_REPLICATION
        state_path = os.path.join(cfg.binlog_replicator.data_dir, TEST_DB_NAME, 'state.pckl')
        state = DbReplicatorState(state_path)
        assert state.status.value == 2  # PERFORMING_INITIAL_REPLICATION
        
        # Add more records while replication is stopped
        for i in range(100, 150):
            mysql.execute(
                f"INSERT INTO `{TEST_TABLE_NAME}` (name, data) VALUES ('test_{i}', 'data_{i}');",
                commit=True
            )

        # Verify that sirocco_tmp database does NOT exist (it should use sirocco directly)
        assert f"{TEST_DB_NAME}_tmp" not in ch.get_databases(), "Temporary database should not exist with ignore_deletes=True"
        
        # Resume initial replication - this should NOT fail with "Database sirocco_tmp does not exist"
        db_replicator_runner_2 = DbReplicatorRunner(TEST_DB_NAME, cfg_file=config_file)
        db_replicator_runner_2.run()
        
        # Wait for all records to be replicated (100 original + 50 extra = 150)
        assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 150, max_wait_time=30)
        
        # Verify the replication completed successfully
        records = ch.select(TEST_TABLE_NAME)
        assert len(records) == 150, f"Expected 150 records, got {len(records)}"
        
        # Verify we can continue with realtime replication
        mysql.execute(f"INSERT INTO `{TEST_TABLE_NAME}` (name, data) VALUES ('realtime_test', 'realtime_data');", commit=True)
        assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 151)
        
        # Clean up
        db_replicator_runner_2.stop()
        binlog_replicator_runner.stop()
        
    finally:
        # Clean up temp config file
        os.unlink(config_file)


def test_charset_configuration():
    """
    Test that charset configuration is properly loaded and used for MySQL connections.
    This test verifies that utf8mb4 charset can be configured to properly handle
    4-byte Unicode characters in JSON fields.
    """
    # Create a temporary config file with explicit charset configuration
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as temp_config_file:
        config_file = temp_config_file.name
        
        # Load base config and add charset setting
        with open(CONFIG_FILE, 'r') as f:
            base_config = yaml.safe_load(f)
        
        # Ensure charset is set to utf8mb4
        base_config['mysql']['charset'] = 'utf8mb4'
        
        yaml.dump(base_config, temp_config_file)
    
    try:
        cfg = config.Settings()
        cfg.load(config_file)
        
        # Verify charset is loaded correctly
        assert hasattr(cfg.mysql, 'charset'), "MysqlSettings should have charset attribute"
        assert cfg.mysql.charset == 'utf8mb4', f"Expected charset utf8mb4, got {cfg.mysql.charset}"
        
        mysql = mysql_api.MySQLApi(None, cfg.mysql)
        ch = clickhouse_api.ClickhouseApi(None, cfg.clickhouse)
        
        prepare_env(cfg, mysql, ch)
        
        mysql.database = TEST_DB_NAME
        ch.database = TEST_DB_NAME
        
        # Create table with JSON field
        mysql.execute(f"""
            CREATE TABLE IF NOT EXISTS {TEST_TABLE_NAME} (
                id INT AUTO_INCREMENT PRIMARY KEY,
                json_data JSON
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """, commit=True)
        
        # Insert data with 4-byte Unicode characters (emoji and Arabic text)
        test_data = {
            "ar": "مرحباً بالعالم",  # Arabic: Hello World
            "emoji": "🌍🎉✨",
            "cn": "你好世界",  # Chinese: Hello World
            "en": "Hello World"
        }
        
        mysql.execute(
            f"INSERT INTO {TEST_TABLE_NAME} (json_data) VALUES (%s)",
            args=(json.dumps(test_data, ensure_ascii=False),),
            commit=True
        )
        
        # Verify the data can be read back correctly
        mysql.cursor.execute(f"SELECT json_data FROM {TEST_TABLE_NAME}")
        result = mysql.cursor.fetchone()
        assert result is not None, "Should have retrieved a record"
        
        retrieved_data = json.loads(result[0]) if isinstance(result[0], str) else result[0]
        assert retrieved_data['ar'] == test_data['ar'], f"Arabic text mismatch: {retrieved_data['ar']} != {test_data['ar']}"
        assert retrieved_data['emoji'] == test_data['emoji'], f"Emoji mismatch: {retrieved_data['emoji']} != {test_data['emoji']}"
        assert retrieved_data['cn'] == test_data['cn'], f"Chinese text mismatch: {retrieved_data['cn']} != {test_data['cn']}"
        
        # Test binlog replication with charset
        binlog_replicator_runner = BinlogReplicatorRunner(cfg_file=config_file)
        binlog_replicator_runner.run()
        
        try:
            # Start db replicator
            db_replicator_runner = DbReplicatorRunner(TEST_DB_NAME, cfg_file=config_file)
            db_replicator_runner.run()
            
            # Wait for database and table to be created in ClickHouse
            assert_wait(lambda: TEST_DB_NAME in ch.get_databases(), max_wait_time=20)
            ch.execute_command(f'USE `{TEST_DB_NAME}`')
            assert_wait(lambda: TEST_TABLE_NAME in ch.get_tables(), max_wait_time=20)
            
            # Wait for replication
            assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 1, max_wait_time=20)
            
            # Verify data in ClickHouse
            ch_records = ch.select(TEST_TABLE_NAME)
            assert len(ch_records) == 1, f"Expected 1 record in ClickHouse, got {len(ch_records)}"
            
            # Access the json_data column using dictionary access
            ch_record = ch_records[0]
            ch_json_data = json.loads(ch_record['json_data']) if isinstance(ch_record['json_data'], str) else ch_record['json_data']
            
            # Verify Unicode characters are preserved correctly
            assert ch_json_data['ar'] == test_data['ar'], f"Arabic text not preserved in CH: {ch_json_data.get('ar')}"
            assert ch_json_data['emoji'] == test_data['emoji'], f"Emoji not preserved in CH: {ch_json_data.get('emoji')}"
            assert ch_json_data['cn'] == test_data['cn'], f"Chinese text not preserved in CH: {ch_json_data.get('cn')}"
            
            # Test realtime replication with more Unicode data
            more_data = {"test": "🔥 Real-time 测试 اختبار"}
            mysql.execute(
                f"INSERT INTO {TEST_TABLE_NAME} (json_data) VALUES (%s)",
                args=(json.dumps(more_data, ensure_ascii=False),),
                commit=True
            )
            
            assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 2, max_wait_time=20)
            
            # Verify the second record
            ch_records = ch.select(TEST_TABLE_NAME)
            assert len(ch_records) == 2, f"Expected 2 records in ClickHouse, got {len(ch_records)}"
            
            db_replicator_runner.stop()
        finally:
            binlog_replicator_runner.stop()
            
    finally:
        # Clean up temp config file
        os.unlink(config_file)
