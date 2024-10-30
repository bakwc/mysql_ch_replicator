import os
import shutil
import time
import subprocess

from mysql_ch_replicator import config
from mysql_ch_replicator import mysql_api
from mysql_ch_replicator import clickhouse_api
from mysql_ch_replicator.binlog_replicator import State as BinlogState
from mysql_ch_replicator.db_replicator import State as DbReplicatorState

from mysql_ch_replicator.runner import ProcessRunner


CONFIG_FILE = 'tests_config.yaml'
TEST_DB_NAME = 'replication_test_db'
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


def assert_wait(condition, max_wait_time=15.0, retry_interval=0.05):
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
        db_name: str = TEST_DB_NAME
):
    if os.path.exists(cfg.binlog_replicator.data_dir):
        shutil.rmtree(cfg.binlog_replicator.data_dir)
    os.mkdir(cfg.binlog_replicator.data_dir)
    mysql.drop_database(db_name)
    mysql.create_database(db_name)
    mysql.set_database(db_name)
    ch.drop_database(db_name)
    assert_wait(lambda: db_name not in ch.get_databases())


def test_e2e_regular():
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
CREATE TABLE {TEST_TABLE_NAME} (
    id int NOT NULL AUTO_INCREMENT,
    name varchar(255),
    age int,
    field1 text,
    field2 blob,
    PRIMARY KEY (id)
); 
    ''')

    mysql.execute(
        f"INSERT INTO {TEST_TABLE_NAME} (name, age, field1, field2) VALUES ('Ivan', 42, 'test1', 'test2');",
        commit=True,
    )
    mysql.execute(f"INSERT INTO {TEST_TABLE_NAME} (name, age) VALUES ('Peter', 33);", commit=True)

    binlog_replicator_runner = BinlogReplicatorRunner()
    binlog_replicator_runner.run()
    db_replicator_runner = DbReplicatorRunner(TEST_DB_NAME)
    db_replicator_runner.run()

    assert_wait(lambda: TEST_DB_NAME in ch.get_databases())

    ch.execute_command(f'USE {TEST_DB_NAME}')

    assert_wait(lambda: TEST_TABLE_NAME in ch.get_tables())
    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 2)

    mysql.execute(f"INSERT INTO {TEST_TABLE_NAME} (name, age) VALUES ('Filipp', 50);", commit=True)
    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 3)
    assert_wait(lambda: ch.select(TEST_TABLE_NAME, where="name='Filipp'")[0]['age'] == 50)


    mysql.execute(f"ALTER TABLE `{TEST_TABLE_NAME}` ADD `last_name` varchar(255); ")
    mysql.execute(f"INSERT INTO {TEST_TABLE_NAME} (name, age, last_name) VALUES ('Mary', 24, 'Smith');", commit=True)

    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 4)
    assert_wait(lambda: ch.select(TEST_TABLE_NAME, where="name='Mary'")[0]['last_name'] == 'Smith')

    assert_wait(lambda: ch.select(TEST_TABLE_NAME, where="field1='test1'")[0]['name'] == 'Ivan')
    assert_wait(lambda: ch.select(TEST_TABLE_NAME, where="field2='test2'")[0]['name'] == 'Ivan')


    mysql.execute(
        f"ALTER TABLE {TEST_DB_NAME}.{TEST_TABLE_NAME} "
        f"ADD COLUMN country VARCHAR(25) DEFAULT '' NOT NULL AFTER name;"
    )

    mysql.execute(
        f"INSERT INTO {TEST_TABLE_NAME} (name, age, last_name, country) "
        f"VALUES ('John', 12, 'Doe', 'USA');", commit=True,
    )

    mysql.execute(
        f"ALTER TABLE {TEST_DB_NAME}.{TEST_TABLE_NAME} "
        f"CHANGE COLUMN country origin VARCHAR(24) DEFAULT '' NOT NULL",
    )

    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 5)
    assert_wait(lambda: ch.select(TEST_TABLE_NAME, where="name='John'")[0].get('origin') == 'USA')

    mysql.execute(
        f"ALTER TABLE {TEST_DB_NAME}.{TEST_TABLE_NAME} "
        f"CHANGE COLUMN origin country VARCHAR(24) DEFAULT '' NOT NULL",
    )
    assert_wait(lambda: ch.select(TEST_TABLE_NAME, where="name='John'")[0].get('origin') is None)
    assert_wait(lambda: ch.select(TEST_TABLE_NAME, where="name='John'")[0].get('country') == 'USA')

    mysql.execute(f"ALTER TABLE {TEST_DB_NAME}.{TEST_TABLE_NAME} DROP COLUMN country")
    assert_wait(lambda: ch.select(TEST_TABLE_NAME, where="name='John'")[0].get('country') is None)

    assert_wait(lambda: ch.select(TEST_TABLE_NAME, where="name='Filipp'")[0].get('last_name') is None)

    mysql.execute(f"UPDATE {TEST_TABLE_NAME} SET last_name = '' WHERE last_name IS NULL;")
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

    mysql.execute(f"INSERT INTO {TEST_TABLE_NAME_2} (name, age) VALUES ('Ivan', 42);", commit=True)
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

    mysql.execute(f"INSERT INTO {TEST_TABLE_NAME_3} (name, `age`) VALUES ('Ivan', 42);", commit=True)
    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME_3)) == 1)

    mysql.execute(f'DROP TABLE {TEST_TABLE_NAME_3}')
    assert_wait(lambda: TEST_TABLE_NAME_3 not in ch.get_tables())


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
CREATE TABLE {TEST_TABLE_NAME} (
    id int NOT NULL AUTO_INCREMENT,
    name varchar(255),
    age int,
    PRIMARY KEY (id)
); 
    ''')

    mysql.execute(f"INSERT INTO {TEST_TABLE_NAME} (name, age) VALUES ('Ivan', 42);", commit=True)

    binlog_replicator_runner = BinlogReplicatorRunner()
    binlog_replicator_runner.run()
    db_replicator_runner = DbReplicatorRunner(TEST_DB_NAME)
    db_replicator_runner.run()

    assert_wait(lambda: TEST_DB_NAME in ch.get_databases())

    ch.execute_command(f'USE {TEST_DB_NAME}')

    assert_wait(lambda: TEST_TABLE_NAME in ch.get_tables())
    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 1)

    mysql.execute(f"ALTER TABLE `{TEST_TABLE_NAME}` ADD `last_name` varchar(255), ADD COLUMN city varchar(255); ")
    mysql.execute(
        f"INSERT INTO {TEST_TABLE_NAME} (name, age, last_name, city) "
        f"VALUES ('Mary', 24, 'Smith', 'London');", commit=True,
    )

    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 2)
    assert_wait(lambda: ch.select(TEST_TABLE_NAME, where="name='Mary'")[0].get('last_name') == 'Smith')
    assert_wait(lambda: ch.select(TEST_TABLE_NAME, where="name='Mary'")[0].get('city') == 'London')

    mysql.execute(f"ALTER TABLE {TEST_TABLE_NAME} DROP COLUMN last_name, DROP COLUMN city")
    assert_wait(lambda: ch.select(TEST_TABLE_NAME, where="name='Mary'")[0].get('last_name') is None)
    assert_wait(lambda: ch.select(TEST_TABLE_NAME, where="name='Mary'")[0].get('city') is None)

    mysql.execute(
        f"CREATE TABLE {TEST_TABLE_NAME_2} "
        f"(id int NOT NULL AUTO_INCREMENT, name varchar(255), age int, "
        f"PRIMARY KEY (id));"
    )

    assert_wait(lambda: TEST_TABLE_NAME_2 in ch.get_tables())


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

    prepare_env(cfg, mysql, ch)

    mysql.execute(f'''
CREATE TABLE {TEST_TABLE_NAME} (
    id int NOT NULL AUTO_INCREMENT,
    name varchar(255),
    age int,
    rate decimal(10,4),
    PRIMARY KEY (id)
); 
    ''')

    mysql.execute(f"INSERT INTO {TEST_TABLE_NAME} (name, age) VALUES ('Ivan', 42);", commit=True)
    mysql.execute(f"INSERT INTO {TEST_TABLE_NAME} (name, age) VALUES ('Peter', 33);", commit=True)

    run_all_runner = RunAllRunner()
    run_all_runner.run()

    assert_wait(lambda: TEST_DB_NAME in ch.get_databases())

    ch.execute_command(f'USE {TEST_DB_NAME}')

    assert_wait(lambda: TEST_TABLE_NAME in ch.get_tables())
    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 2)

    mysql.execute(f"INSERT INTO {TEST_TABLE_NAME} (name, age) VALUES ('Filipp', 50);", commit=True)
    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 3)
    assert_wait(lambda: ch.select(TEST_TABLE_NAME, where="name='Filipp'")[0]['age'] == 50)

    # Test for restarting dead processes
    binlog_repl_pid = get_binlog_replicator_pid(cfg)
    db_repl_pid = get_db_replicator_pid(cfg, TEST_DB_NAME)

    kill_process(binlog_repl_pid)
    kill_process(db_repl_pid, force=True)

    mysql.execute(f"INSERT INTO {TEST_TABLE_NAME} (name, rate) VALUES ('John', 12.5);", commit=True)
    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 4)
    assert_wait(lambda: ch.select(TEST_TABLE_NAME, where="name='John'")[0]['rate'] == 12.5)

    mysql.execute(f"DELETE FROM {TEST_TABLE_NAME} WHERE name='John';", commit=True)
    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 3)

    mysql.execute(f"UPDATE {TEST_TABLE_NAME} SET age=66 WHERE name='Ivan'", commit=True)
    time.sleep(4)
    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 3)

    run_all_runner.stop()


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
CREATE TABLE {TEST_TABLE_NAME} (
    id int NOT NULL AUTO_INCREMENT,
    name varchar(255),
    age int,
    PRIMARY KEY (id)
); 
    ''')

    mysql.execute(f"INSERT INTO {TEST_TABLE_NAME} (name, age) VALUES ('Ivan', 42);", commit=True)
    mysql.execute(f"INSERT INTO {TEST_TABLE_NAME} (name, age) VALUES ('Peter', 33);", commit=True)

    db_replicator_runner = DbReplicatorRunner(TEST_DB_NAME, additional_arguments='--initial_only=True')
    db_replicator_runner.run()
    db_replicator_runner.wait_complete()

    assert TEST_DB_NAME in ch.get_databases()

    ch.execute_command(f'USE {TEST_DB_NAME}')

    assert TEST_TABLE_NAME in ch.get_tables()
    assert len(ch.select(TEST_TABLE_NAME)) == 2


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
    mysql.create_database('test_db_3')
    ch.drop_database('test_db_3')

    prepare_env(cfg, mysql, ch, db_name='test_db_2')

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

    ch.execute_command('USE test_db_2')

    assert_wait(lambda: 'test_table_2' in ch.get_tables())
    assert_wait(lambda: len(ch.select('test_table_2')) == 1)

    assert 'test_table_3' not in ch.get_tables()


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
CREATE TABLE {TEST_TABLE_NAME} (
    id int NOT NULL AUTO_INCREMENT,
    name varchar(255),
    modified_date DateTime(3) NOT NULL,
    PRIMARY KEY (id)
); 
    ''')

    mysql.execute(
        f"INSERT INTO {TEST_TABLE_NAME} (name, modified_date) VALUES ('Ivan', '0000-00-00 00:00:00');",
        commit=True,
    )

    binlog_replicator_runner = BinlogReplicatorRunner()
    binlog_replicator_runner.run()
    db_replicator_runner = DbReplicatorRunner(TEST_DB_NAME)
    db_replicator_runner.run()

    assert_wait(lambda: TEST_DB_NAME in ch.get_databases())

    ch.execute_command(f'USE {TEST_DB_NAME}')

    assert_wait(lambda: TEST_TABLE_NAME in ch.get_tables())
    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 1)
