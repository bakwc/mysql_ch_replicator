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

# Test isolation for parallel testing
import uuid
import threading

# Thread-local storage for test-specific names
_test_local = threading.local()

def get_worker_id():
    """Get pytest-xdist worker ID for database isolation"""
    worker_id = os.environ.get('PYTEST_XDIST_WORKER', 'master')
    return worker_id.replace('gw', 'w')  # gw0 -> w0, gw1 -> w1, etc.

def get_test_id():
    """Get unique test identifier for complete isolation"""
    if not hasattr(_test_local, 'test_id'):
        _test_local.test_id = uuid.uuid4().hex[:8]
    return _test_local.test_id

def reset_test_id():
    """Reset test ID for new test (called by fixture)"""
    _test_local.test_id = uuid.uuid4().hex[:8]

def get_test_db_name(suffix=""):
    """Get test-specific database name (unique per test per worker)"""
    worker_id = get_worker_id()
    test_id = get_test_id()
    return f"test_db_{worker_id}_{test_id}{suffix}"

def get_test_table_name(suffix=""):
    """Get test-specific table name (unique per test per worker)"""  
    worker_id = get_worker_id()
    test_id = get_test_id()
    return f"test_table_{worker_id}_{test_id}{suffix}"

def get_test_data_dir(suffix=""):
    """Get worker and test isolated data directory (unique per test per worker)"""
    worker_id = get_worker_id()
    test_id = get_test_id()
    return f"/app/binlog_{worker_id}_{test_id}{suffix}"

def get_test_log_dir(suffix=""):
    """Get worker-isolated log directory (unique per worker)"""
    return get_test_data_dir(f"/logs{suffix}")

def get_isolated_binlog_path(database_name=None):
    """Get isolated binlog path for specific database or worker"""
    if database_name:
        return os.path.join(get_test_data_dir(), database_name)
    return get_test_data_dir()

# Initialize with default values - will be updated per test
TEST_DB_NAME = get_test_db_name()
TEST_DB_NAME_2 = get_test_db_name("_2") 
TEST_DB_NAME_2_DESTINATION = f"replication_dest_{get_worker_id()}_{get_test_id()}"
TEST_TABLE_NAME = get_test_table_name()
TEST_TABLE_NAME_2 = get_test_table_name("_2")
TEST_TABLE_NAME_3 = get_test_table_name("_3")

# Isolated path constants
TEST_DATA_DIR = get_test_data_dir()
TEST_LOG_DIR = get_test_log_dir()

def update_test_constants():
    """Update module-level constants with new test IDs"""
    global TEST_DB_NAME, TEST_DB_NAME_2, TEST_DB_NAME_2_DESTINATION
    global TEST_TABLE_NAME, TEST_TABLE_NAME_2, TEST_TABLE_NAME_3
    global TEST_DATA_DIR, TEST_LOG_DIR
    
    reset_test_id()  # Generate new test ID
    
    # Capture the same test_id for all constants to ensure consistency
    worker_id = get_worker_id()
    test_id = get_test_id()
    
    TEST_DB_NAME = f"test_db_{worker_id}_{test_id}"
    TEST_DB_NAME_2 = f"test_db_{worker_id}_{test_id}_2"
    TEST_DB_NAME_2_DESTINATION = f"replication_dest_{worker_id}_{test_id}"
    TEST_TABLE_NAME = f"test_table_{worker_id}_{test_id}"
    TEST_TABLE_NAME_2 = f"test_table_{worker_id}_{test_id}_2"
    TEST_TABLE_NAME_3 = f"test_table_{worker_id}_{test_id}_3"
    
    # Update path constants
    TEST_DATA_DIR = f"/app/binlog_{worker_id}_{test_id}"
    TEST_LOG_DIR = f"/app/binlog_{worker_id}_{test_id}/logs"


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
    # Always ensure the base binlog directory exists (safe for parallel tests)
    os.makedirs(cfg.binlog_replicator.data_dir, exist_ok=True)
    
    # Clean only database-specific subdirectory, never remove the base directory
    db_binlog_dir = os.path.join(cfg.binlog_replicator.data_dir, db_name)
    if os.path.exists(db_binlog_dir):
        # Clean the specific database directory but preserve the base directory
        shutil.rmtree(db_binlog_dir)
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


# Per-test isolation fixture
@pytest.fixture(autouse=True, scope="function")
def isolate_test_databases():
    """Automatically isolate databases for each test"""
    update_test_constants()
    yield
    # Note: cleanup handled by clean_environment fixtures

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

def load_isolated_config(config_file=CONFIG_FILE):
    """Load configuration with worker-isolated paths applied"""
    cfg = config.Settings()
    cfg.load(config_file)
    
    # Apply path isolation
    cfg.binlog_replicator.data_dir = get_test_data_dir()
    
    return cfg

def get_isolated_config_with_paths():
    """Get configuration with all isolated paths configured"""
    cfg = load_isolated_config()
    return cfg

@pytest.fixture
def isolated_config(request):
    """Load configuration with isolated paths for parallel testing"""
    config_file = getattr(request, "param", CONFIG_FILE)
    cfg = load_isolated_config(config_file)
    cfg.config_file = config_file
    return cfg

def cleanup_test_directory():
    """Clean up current test's isolated directory"""
    test_dir = get_test_data_dir()
    if os.path.exists(test_dir):
        shutil.rmtree(test_dir)
        print(f"Cleaned up test directory: {test_dir}")

def cleanup_worker_directories(worker_id=None):
    """Clean up all test directories for a specific worker"""
    import glob
    if worker_id is None:
        worker_id = get_worker_id()
    
    pattern = f"/app/binlog_{worker_id}_*"
    worker_test_dirs = glob.glob(pattern)
    for dir_path in worker_test_dirs:
        if os.path.exists(dir_path):
            shutil.rmtree(dir_path)
            print(f"Cleaned up worker test directory: {dir_path}")

def cleanup_all_isolated_directories():
    """Clean up all isolated test directories"""
    import glob
    patterns = ["/app/binlog_w*", "/app/binlog_main_*"]
    for pattern in patterns:
        test_dirs = glob.glob(pattern)
        for dir_path in test_dirs:
            if os.path.exists(dir_path):
                shutil.rmtree(dir_path)
                print(f"Cleaned up directory: {dir_path}")

def ensure_isolated_directory_exists():
    """Ensure worker-isolated directory exists and is clean"""
    worker_dir = get_test_data_dir()
    if os.path.exists(worker_dir):
        shutil.rmtree(worker_dir)
    os.makedirs(worker_dir, exist_ok=True)
    return worker_dir


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
    # Generate new test-specific database names for this test
    update_test_constants()
    
    # Capture current test-specific database names
    current_test_db = TEST_DB_NAME
    current_test_db_2 = TEST_DB_NAME_2
    current_test_dest = TEST_DB_NAME_2_DESTINATION
    
    prepare_env(test_config, mysql_api_instance, clickhouse_api_instance, db_name=current_test_db)
    
    # Store the database name in the test config so it can be used consistently
    test_config.test_db_name = current_test_db
    
    yield test_config, mysql_api_instance, clickhouse_api_instance
    
    # Cleanup after test - test-specific
    try:
        cleanup_databases = [
            current_test_db,
            current_test_db_2, 
            current_test_dest,
        ]
        
        for db_name in cleanup_databases:
            mysql_drop_database(mysql_api_instance, db_name)
            clickhouse_api_instance.drop_database(db_name)
    except Exception:
        pass  # Ignore cleanup errors


@pytest.fixture
def dynamic_clean_environment(
    dynamic_config, dynamic_mysql_api_instance, dynamic_clickhouse_api_instance
):
    """Provide clean test environment with dynamic config and automatic cleanup"""
    # Generate new test-specific database names for this test
    update_test_constants()
    
    # Capture current test-specific database names
    current_test_db = TEST_DB_NAME
    current_test_db_2 = TEST_DB_NAME_2
    current_test_dest = TEST_DB_NAME_2_DESTINATION
    
    prepare_env(
        dynamic_config, dynamic_mysql_api_instance, dynamic_clickhouse_api_instance, db_name=current_test_db
    )
    yield dynamic_config, dynamic_mysql_api_instance, dynamic_clickhouse_api_instance
    
    # Cleanup after test - test-specific
    try:
        cleanup_databases = [
            current_test_db,
            current_test_db_2,
            current_test_dest,
        ]
        
        for db_name in cleanup_databases:
            mysql_drop_database(dynamic_mysql_api_instance, db_name)
            dynamic_clickhouse_api_instance.drop_database(db_name)
    except Exception:
        pass  # Ignore cleanup errors


@pytest.fixture  
def isolated_clean_environment(isolated_config, mysql_api_instance, clickhouse_api_instance):
    """Provide isolated clean test environment for parallel testing"""
    import tempfile
    import yaml
    
    # Generate new test-specific database names and paths for this test
    update_test_constants()
    
    # Capture current test-specific database names
    current_test_db = TEST_DB_NAME
    current_test_db_2 = TEST_DB_NAME_2
    current_test_dest = TEST_DB_NAME_2_DESTINATION
    
    # Create temporary config file with isolated paths
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as temp_config_file:
        # Convert config object back to dictionary for YAML serialization
        config_dict = {
            'mysql': {
                'host': isolated_config.mysql.host,
                'port': isolated_config.mysql.port,
                'user': isolated_config.mysql.user,
                'password': isolated_config.mysql.password,
                'pool_size': isolated_config.mysql.pool_size,
                'max_overflow': isolated_config.mysql.max_overflow
            },
            'clickhouse': {
                'host': isolated_config.clickhouse.host,
                'port': isolated_config.clickhouse.port,
                'user': isolated_config.clickhouse.user,
                'password': isolated_config.clickhouse.password,
            },
            'binlog_replicator': {
                'data_dir': isolated_config.binlog_replicator.data_dir,
                'records_per_file': isolated_config.binlog_replicator.records_per_file,
                'binlog_retention_period': isolated_config.binlog_replicator.binlog_retention_period,
            },
            'databases': isolated_config.databases,
            'log_level': isolated_config.log_level,
            'optimize_interval': isolated_config.optimize_interval,
            'check_db_updated_interval': isolated_config.check_db_updated_interval,
        }
        
        # Add optional fields if they exist
        if hasattr(isolated_config, 'target_databases') and isolated_config.target_databases:
            config_dict['target_databases'] = isolated_config.target_databases
        if hasattr(isolated_config, 'indexes') and isolated_config.indexes:
            # Convert Index objects to dictionaries for YAML serialization
            config_dict['indexes'] = []
            for index in isolated_config.indexes:
                if hasattr(index, '__dict__'):
                    # Convert Index object to dict manually
                    index_dict = {
                        'databases': index.databases,
                        'tables': index.tables if hasattr(index, 'tables') else [],
                        'index': index.index if hasattr(index, 'index') else ''
                    }
                    config_dict['indexes'].append(index_dict)
                else:
                    config_dict['indexes'].append(index)
        if hasattr(isolated_config, 'http_host'):
            config_dict['http_host'] = isolated_config.http_host
        if hasattr(isolated_config, 'http_port'):
            config_dict['http_port'] = isolated_config.http_port
        if hasattr(isolated_config, 'types_mapping') and isolated_config.types_mapping:
            config_dict['types_mapping'] = isolated_config.types_mapping
            
        yaml.dump(config_dict, temp_config_file)
        temp_config_path = temp_config_file.name
    
    # Store the config file path in the config object
    isolated_config.config_file = temp_config_path
    
    # Prepare environment with isolated paths
    prepare_env(isolated_config, mysql_api_instance, clickhouse_api_instance, db_name=current_test_db)
    
    # Store the database name in the test config so it can be used consistently  
    isolated_config.test_db_name = current_test_db
    
    yield isolated_config, mysql_api_instance, clickhouse_api_instance
    
    # Cleanup the test databases
    try:
        cleanup_databases = [current_test_db, current_test_db_2, current_test_dest]
        for db_name in cleanup_databases:
            mysql_drop_database(mysql_api_instance, db_name)
            clickhouse_drop_database(clickhouse_api_instance, db_name)
    except Exception:
        pass  # Ignore cleanup errors
    
    # Clean up the isolated test directory
    cleanup_test_directory()
    
    # Clean up the temporary config file
    try:
        os.unlink(temp_config_path)
    except:
        pass

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
