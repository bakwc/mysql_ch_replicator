import datetime
import os
import shutil
import time
import subprocess
import json
import uuid
import decimal

import pytest
import requests

from mysql_ch_replicator import config
from mysql_ch_replicator import mysql_api
from mysql_ch_replicator import clickhouse_api
from mysql_ch_replicator.binlog_replicator import State as BinlogState, FileReader, EventType, BinlogReplicator
from mysql_ch_replicator.db_replicator import State as DbReplicatorState, DbReplicator
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


def test_runner():
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

    run_all_runner = RunAllRunner()
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

    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 5)
    assert_wait(lambda: ch.select(TEST_TABLE_NAME, "age=1912")[0]['name'] == 'Hällo')

    ch.drop_database(TEST_DB_NAME)
    ch.drop_database(TEST_DB_NAME_2)

    requests.get('http://localhost:9128/restart_replication')
    time.sleep(1.0)

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


def test_performance():
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

    time.sleep(1)

    mysql.execute(f"INSERT INTO `{TEST_TABLE_NAME}` (name, age) VALUES ('TEST_VALUE_1', 33);", commit=True)

    def _get_last_insert_name():
        record = get_last_insert_from_binlog(cfg=cfg, db_name=TEST_DB_NAME)
        if record is None:
            return None
        return record[1].decode('utf-8')

    assert_wait(lambda: _get_last_insert_name() == 'TEST_VALUE_1', retry_interval=0.5)

    binlog_replicator_runner.stop()

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
#`replication-test_db`
    mysql.execute(f"INSERT INTO `{TEST_TABLE_NAME}` (name, age) VALUES ('TEST_VALUE_FINAL', 0);", commit=True)

    print("running db_replicator")
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
    print("records per second:", int(rps))
    print("total time (seconds):", round(time_delta, 2))
    print("*****************************")
    print('\n\n')



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
    monkeypatch.setattr(DbReplicator, 'INITIAL_REPLICATION_BATCH_SIZE', 1)

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

    binlog_replicator_runner = BinlogReplicatorRunner()
    binlog_replicator_runner.run()
    db_replicator_runner = DbReplicatorRunner(TEST_DB_NAME)
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
    monkeypatch.setattr(DbReplicator, 'INITIAL_REPLICATION_BATCH_SIZE', 1)

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

    binlog_replicator_runner = BinlogReplicatorRunner()
    binlog_replicator_runner.run()
    db_replicator_runner = DbReplicatorRunner(TEST_DB_NAME)
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
    monkeypatch.setattr(DbReplicator, 'INITIAL_REPLICATION_BATCH_SIZE', 1)

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
  `id` int NOT NULL,
  PRIMARY KEY (`id`)); 
    ''')

    mysql.execute(
        f"INSERT INTO `{TEST_TABLE_NAME}` (id) VALUES (42)",
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
    monkeypatch.setattr(DbReplicator, 'INITIAL_REPLICATION_BATCH_SIZE', 1)

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
  `id` int NOT NULL,
  PRIMARY KEY (`id`)); 
    ''')

    mysql.execute(
        f"INSERT INTO `{TEST_TABLE_NAME}` (id) VALUES (42)",
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
def test_performance_dbreplicator():
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

    time.sleep(1)

    mysql.execute(f"INSERT INTO `{TEST_TABLE_NAME}` (name, age) VALUES ('TEST_VALUE_1', 33);", commit=True)

    def _get_last_insert_name():
        record = get_last_insert_from_binlog(cfg=cfg, db_name=TEST_DB_NAME)
        if record is None:
            return None
        return record[1].decode('utf-8')

    assert_wait(lambda: _get_last_insert_name() == 'TEST_VALUE_1', retry_interval=0.5)

    binlog_replicator_runner.stop()

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

    print("running db_replicator")
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
