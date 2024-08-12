import os
import shutil
import subprocess
import time

import config
import mysql_api
import clickhouse_api


CONFIG_FILE = 'config.yaml'
TEST_DB_NAME = 'replication_test_db'
TEST_TABLE_NAME = 'test_table'


def assert_wait(condition, max_wait_time=15.0, retry_interval=0.05):
    max_time = time.time() + max_wait_time
    while time.time() < max_time:
        if condition():
            return
        time.sleep(retry_interval)
    assert condition()


class ProcessRunner:
    def __init__(self, cmd):
        self.cmd = cmd
        self.process = None

    def run(self):
        cmd = self.cmd.split()
        self.process = subprocess.Popen(cmd)

    def stop(self):
        if self.process is not None:
            self.process.kill()
            self.process.wait()
            self.process = None

    def __del__(self):
        self.stop()


class BinlogReplicatorRunner(ProcessRunner):
    def __init__(self):
        super().__init__('python3 main.py --config config.yaml binlog_replicator')


class DbReplicatorRunner(ProcessRunner):
    def __init__(self, db_name):
        super().__init__(f'python3 main.py --config config.yaml --db {db_name} db_replicator')


def prepare_env(
        cfg: config.Settings,
        mysql: mysql_api.MySQLApi,
        ch: clickhouse_api.ClickhouseApi,
):
    if os.path.exists(cfg.binlog_replicator.data_dir):
        shutil.rmtree(cfg.binlog_replicator.data_dir)
    os.mkdir(cfg.binlog_replicator.data_dir)
    mysql.drop_database(TEST_DB_NAME)
    mysql.create_database(TEST_DB_NAME)
    mysql.execute(f'USE {TEST_DB_NAME}')
    ch.drop_database(TEST_DB_NAME)


def test_e2e():
    cfg = config.Settings()
    cfg.load(CONFIG_FILE)

    mysql = mysql_api.MySQLApi(
        database=None,
        mysql_settings=cfg.mysql,
    )

    ch = clickhouse_api.ClickhouseApi(
        database=None,
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


    mysql.execute(f"ALTER TABLE {TEST_TABLE_NAME} ADD last_name varchar(255); ")
    mysql.execute(f"INSERT INTO {TEST_TABLE_NAME} (name, age, last_name) VALUES ('Mary', 24, 'Smith');", commit=True)

    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 4)
    assert_wait(lambda: ch.select(TEST_TABLE_NAME, where="name='Mary'")[0]['last_name'] == 'Smith')


    mysql.execute(
        f"ALTER TABLE {TEST_DB_NAME}.{TEST_TABLE_NAME} "
        f"ADD COLUMN country VARCHAR(25) DEFAULT '' NOT NULL AFTER name;"
    )

    mysql.execute(
        f"INSERT INTO {TEST_TABLE_NAME} (name, age, last_name, country) "
        f"VALUES ('John', 12, 'Doe', 'USA');", commit=True,
    )

    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 5)
    assert_wait(lambda: ch.select(TEST_TABLE_NAME, where="name='John'")[0].get('country') == 'USA')

    mysql.execute(f"ALTER TABLE {TEST_DB_NAME}.{TEST_TABLE_NAME} DROP COLUMN country")
    assert_wait(lambda: ch.select(TEST_TABLE_NAME, where="name='John'")[0].get('country') is None)
