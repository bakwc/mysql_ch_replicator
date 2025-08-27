"""Shared test fixtures and utilities for mysql-ch-replicator tests"""

import os
import shutil
import subprocess
import tempfile
import time

import pytest
import yaml

from mysql_ch_replicator import clickhouse_api, config, mysql_api
from mysql_ch_replicator.runner import ProcessRunner
from tests.utils.mysql_test_api import MySQLTestApi

# Constants
CONFIG_FILE = "tests/configs/replicator/tests_config.yaml"
CONFIG_FILE_MARIADB = "tests/configs/replicator/tests_config_mariadb.yaml"
TEST_DB_NAME = "replication-test_db"
TEST_DB_NAME_2 = "replication-test_db_2"
TEST_DB_NAME_2_DESTINATION = "replication-destination"
TEST_TABLE_NAME = "test_table"
TEST_TABLE_NAME_2 = "test_table_2"
TEST_TABLE_NAME_3 = "test_table_3"


# Test runners
class BinlogReplicatorRunner(ProcessRunner):
    def __init__(self, cfg_file=CONFIG_FILE):
        super().__init__(f"./main.py --config {cfg_file} binlog_replicator")


class DbReplicatorRunner(ProcessRunner):
    def __init__(self, db_name, additional_arguments=None, cfg_file=CONFIG_FILE):
        additional_arguments = additional_arguments or ""
        if not additional_arguments.startswith(" "):
            additional_arguments = " " + additional_arguments
        super().__init__(
            f"./main.py --config {cfg_file} --db {db_name} db_replicator{additional_arguments}"
        )


class RunAllRunner(ProcessRunner):
    def __init__(self, cfg_file=CONFIG_FILE):
        super().__init__(f"./main.py --config {cfg_file} run_all")


# Database operation helpers
def mysql_drop_database(mysql_test_api: MySQLTestApi, db_name: str):
    """Drop MySQL database (helper function)"""
    with mysql_test_api.get_connection() as (connection, cursor):
        cursor.execute(f"DROP DATABASE IF EXISTS `{db_name}`")


def mysql_create_database(mysql_test_api: MySQLTestApi, db_name: str):
    """Create MySQL database (helper function)"""
    with mysql_test_api.get_connection() as (connection, cursor):
        cursor.execute(f"CREATE DATABASE `{db_name}`")


def mysql_drop_table(mysql_test_api: MySQLTestApi, table_name: str):
    """Drop MySQL table (helper function)"""
    with mysql_test_api.get_connection() as (connection, cursor):
        cursor.execute(f"DROP TABLE IF EXISTS `{table_name}`")


# Utility functions
def kill_process(pid, force=False):
    """Kill a process by PID"""
    command = f"kill {pid}"
    if force:
        command = f"kill -9 {pid}"
    subprocess.run(command, shell=True)


def assert_wait(condition, max_wait_time=20.0, retry_interval=0.05):
    """Wait for a condition to be true with timeout"""
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
    set_mysql_db: bool = True,
):
    """Prepare clean test environment"""
    if os.path.exists(cfg.binlog_replicator.data_dir):
        shutil.rmtree(cfg.binlog_replicator.data_dir)
    os.mkdir(cfg.binlog_replicator.data_dir)
    mysql_drop_database(mysql, db_name)
    mysql_create_database(mysql, db_name)
    if set_mysql_db:
        mysql.set_database(db_name)
    ch.drop_database(db_name)
    assert_wait(lambda: db_name not in ch.get_databases())


def read_logs(db_name):
    """Read logs from db replicator for debugging"""
    return open(os.path.join("binlog", db_name, "db_replicator.log")).read()


def get_binlog_replicator_pid(cfg: config.Settings):
    """Get binlog replicator process ID"""
    from mysql_ch_replicator.binlog_replicator import State as BinlogState

    path = os.path.join(cfg.binlog_replicator.data_dir, "state.json")
    state = BinlogState(path)
    return state.pid


def get_db_replicator_pid(cfg: config.Settings, db_name: str):
    """Get database replicator process ID"""
    from mysql_ch_replicator.db_replicator import State as DbReplicatorState

    path = os.path.join(cfg.binlog_replicator.data_dir, db_name, "state.pckl")
    state = DbReplicatorState(path)
    return state.pid


def get_last_file(directory, extension=".bin"):
    """Get the last file in directory by number"""
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


def get_last_insert_from_binlog(cfg, db_name: str):
    """Get the last insert record from binlog files"""
    from mysql_ch_replicator.binlog_replicator import EventType, FileReader

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


# Pytest fixtures
@pytest.fixture
def test_config():
    """Load test configuration"""
    cfg = config.Settings()
    cfg.load(CONFIG_FILE)
    return cfg


@pytest.fixture
def dynamic_config(request):
    """Load configuration dynamically based on test parameter"""
    config_file = getattr(request, "param", CONFIG_FILE)
    cfg = config.Settings()
    cfg.load(config_file)
    # Store the config file path for reference
    cfg.config_file = config_file
    return cfg


@pytest.fixture
def mysql_api_instance(test_config):
    """Create MySQL Test API instance for testing scenarios"""
    return MySQLTestApi(
        database=None,
        mysql_settings=test_config.mysql,
    )


@pytest.fixture
def dynamic_mysql_api_instance(dynamic_config):
    """Create MySQL Test API instance with dynamic config"""
    return MySQLTestApi(
        database=None,
        mysql_settings=dynamic_config.mysql,
    )


@pytest.fixture
def clickhouse_api_instance(test_config):
    """Create ClickHouse API instance"""
    return clickhouse_api.ClickhouseApi(
        database=TEST_DB_NAME,
        clickhouse_settings=test_config.clickhouse,
    )


@pytest.fixture
def dynamic_clickhouse_api_instance(dynamic_config):
    """Create ClickHouse API instance with dynamic config"""
    return clickhouse_api.ClickhouseApi(
        database=TEST_DB_NAME,
        clickhouse_settings=dynamic_config.clickhouse,
    )


@pytest.fixture
def clean_environment(test_config, mysql_api_instance, clickhouse_api_instance):
    """Provide clean test environment with automatic cleanup"""
    prepare_env(test_config, mysql_api_instance, clickhouse_api_instance)
    yield test_config, mysql_api_instance, clickhouse_api_instance
    # Cleanup after test
    try:
        mysql_drop_database(mysql_api_instance, TEST_DB_NAME)
        mysql_drop_database(mysql_api_instance, TEST_DB_NAME_2)
        clickhouse_api_instance.drop_database(TEST_DB_NAME)
        clickhouse_api_instance.drop_database(TEST_DB_NAME_2)
        clickhouse_api_instance.drop_database(TEST_DB_NAME_2_DESTINATION)
    except Exception:
        pass  # Ignore cleanup errors


@pytest.fixture
def dynamic_clean_environment(
    dynamic_config, dynamic_mysql_api_instance, dynamic_clickhouse_api_instance
):
    """Provide clean test environment with dynamic config and automatic cleanup"""
    prepare_env(
        dynamic_config, dynamic_mysql_api_instance, dynamic_clickhouse_api_instance
    )
    yield dynamic_config, dynamic_mysql_api_instance, dynamic_clickhouse_api_instance
    # Cleanup after test
    try:
        mysql_drop_database(dynamic_mysql_api_instance, TEST_DB_NAME)
        mysql_drop_database(dynamic_mysql_api_instance, TEST_DB_NAME_2)
        dynamic_clickhouse_api_instance.drop_database(TEST_DB_NAME)
        dynamic_clickhouse_api_instance.drop_database(TEST_DB_NAME_2)
        dynamic_clickhouse_api_instance.drop_database(TEST_DB_NAME_2_DESTINATION)
    except Exception:
        pass  # Ignore cleanup errors


@pytest.fixture
def temp_config_file():
    """Create temporary config file for tests that need custom config"""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        yield f.name
    # Cleanup
    try:
        os.unlink(f.name)
    except FileNotFoundError:
        pass


@pytest.fixture
def ignore_deletes_config(temp_config_file):
    """Config with ignore_deletes=True"""
    # Read the original config
    with open(CONFIG_FILE, "r") as original_config:
        config_data = yaml.safe_load(original_config)

    # Add ignore_deletes=True
    config_data["ignore_deletes"] = True

    # Write to temp file
    with open(temp_config_file, "w") as f:
        yaml.dump(config_data, f)

    return temp_config_file


# Pytest markers
def pytest_configure(config):
    """Register custom markers"""
    config.addinivalue_line(
        "markers", "optional: mark test as optional (may be skipped in CI)"
    )
    config.addinivalue_line("markers", "performance: mark test as performance test")
    config.addinivalue_line("markers", "slow: mark test as slow running")
    config.addinivalue_line("markers", "integration: mark test as integration test")
    config.addinivalue_line("markers", "unit: mark test as unit test")
