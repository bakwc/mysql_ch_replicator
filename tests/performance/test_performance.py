"""Performance tests for mysql-ch-replicator"""

import os
import time

import pytest

from tests.conftest import (
    TEST_DB_NAME,
    TEST_TABLE_NAME,
    BinlogReplicatorRunner,
    DbReplicatorRunner,
    assert_wait,
    get_last_file,
    get_last_insert_from_binlog,
)


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


@pytest.mark.performance
@pytest.mark.optional
@pytest.mark.slow
def test_performance_realtime_replication(clean_environment):
    """Test performance of realtime replication"""
    config_file = "tests/configs/replicator/tests_config_perf.yaml"
    num_records = 100000

    cfg, mysql, ch = clean_environment
    cfg.load(config_file)

    mysql.execute(f"""
    CREATE TABLE `{TEST_TABLE_NAME}` (
        id int NOT NULL AUTO_INCREMENT,
        name varchar(2048),
        age int,
        PRIMARY KEY (id)
    ); 
        """)

    binlog_replicator_runner = BinlogReplicatorRunner(cfg_file=config_file)
    binlog_replicator_runner.run()
    db_replicator_runner = DbReplicatorRunner(TEST_DB_NAME, cfg_file=config_file)
    db_replicator_runner.run()

    time.sleep(1)

    mysql.execute(
        f"INSERT INTO `{TEST_TABLE_NAME}` (name, age) VALUES ('TEST_VALUE_1', 33);",
        commit=True,
    )

    def _get_last_insert_name():
        record = get_last_insert_from_binlog(cfg=cfg, db_name=TEST_DB_NAME)
        if record is None:
            return None
        return record[1].decode("utf-8")

    assert_wait(lambda: _get_last_insert_name() == "TEST_VALUE_1", retry_interval=0.5)

    # Wait for the database and table to be created in ClickHouse
    assert_wait(lambda: TEST_DB_NAME in ch.get_databases(), retry_interval=0.5)
    ch.execute_command(f"USE `{TEST_DB_NAME}`")
    assert_wait(lambda: TEST_TABLE_NAME in ch.get_tables(), retry_interval=0.5)
    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 1, retry_interval=0.5)

    binlog_replicator_runner.stop()
    db_replicator_runner.stop()

    time.sleep(1)

    print("populating mysql data")

    base_value = "a" * 2000

    for i in range(num_records):
        if i % 2000 == 0:
            print(f"populated {i} elements")
        mysql.execute(
            f"INSERT INTO `{TEST_TABLE_NAME}` (name, age) "
            f"VALUES ('TEST_VALUE_{i}_{base_value}', {i});",
            commit=i % 20 == 0,
        )

    mysql.execute(
        f"INSERT INTO `{TEST_TABLE_NAME}` (name, age) VALUES ('TEST_VALUE_FINAL', 0);",
        commit=True,
    )

    print("running binlog_replicator")
    t1 = time.time()
    binlog_replicator_runner = BinlogReplicatorRunner(cfg_file=config_file)
    binlog_replicator_runner.run()

    assert_wait(
        lambda: _get_last_insert_name() == "TEST_VALUE_FINAL",
        retry_interval=0.5,
        max_wait_time=1000,
    )
    t2 = time.time()

    binlog_replicator_runner.stop()

    time_delta = t2 - t1
    rps = num_records / time_delta

    print("\n\n")
    print("*****************************")
    print("Binlog Replicator Performance:")
    print("records per second:", int(rps))
    print("total time (seconds):", round(time_delta, 2))
    print("*****************************")
    print("\n\n")

    # Now test db_replicator performance
    print("running db_replicator")
    t1 = time.time()
    db_replicator_runner = DbReplicatorRunner(TEST_DB_NAME, cfg_file=config_file)
    db_replicator_runner.run()

    # Make sure the database and table exist before querying
    assert_wait(lambda: TEST_DB_NAME in ch.get_databases(), retry_interval=0.5)
    ch.execute_command(f"USE `{TEST_DB_NAME}`")
    assert_wait(lambda: TEST_TABLE_NAME in ch.get_tables(), retry_interval=0.5)
    assert_wait(
        lambda: len(ch.select(TEST_TABLE_NAME)) == num_records + 2,
        retry_interval=0.5,
        max_wait_time=1000,
    )
    t2 = time.time()

    db_replicator_runner.stop()

    time_delta = t2 - t1
    rps = num_records / time_delta

    print("\n\n")
    print("*****************************")
    print("DB Replicator Performance:")
    print("records per second:", int(rps))
    print("total time (seconds):", round(time_delta, 2))
    print("*****************************")
    print("\n\n")


@pytest.mark.performance
@pytest.mark.optional
@pytest.mark.slow
def test_performance_initial_only_replication(clean_environment):
    """Test performance of initial-only replication mode"""
    config_file = "tests/configs/replicator/tests_config_perf.yaml"
    num_records = 300000

    cfg, mysql, ch = clean_environment
    cfg.load(config_file)

    mysql.execute(f"""
    CREATE TABLE `{TEST_TABLE_NAME}` (
        id int NOT NULL AUTO_INCREMENT,
        name varchar(2048),
        age int,
        PRIMARY KEY (id)
    ); 
    """)

    print("populating mysql data")

    base_value = "a" * 2000

    for i in range(num_records):
        if i % 2000 == 0:
            print(f"populated {i} elements")
        mysql.execute(
            f"INSERT INTO `{TEST_TABLE_NAME}` (name, age) "
            f"VALUES ('TEST_VALUE_{i}_{base_value}', {i});",
            commit=i % 20 == 0,
        )

    mysql.execute(
        f"INSERT INTO `{TEST_TABLE_NAME}` (name, age) VALUES ('TEST_VALUE_FINAL', 0);",
        commit=True,
    )
    print(f"finished populating {num_records} records")

    # Now test db_replicator performance in initial_only mode
    print("running db_replicator in initial_only mode")
    t1 = time.time()

    db_replicator_runner = DbReplicatorRunner(
        TEST_DB_NAME, additional_arguments="--initial_only=True", cfg_file=config_file
    )
    db_replicator_runner.run()
    db_replicator_runner.wait_complete()  # Wait for the process to complete

    # Make sure the database and table exist
    assert_wait(lambda: TEST_DB_NAME in ch.get_databases(), retry_interval=0.5)
    ch.execute_command(f"USE `{TEST_DB_NAME}`")
    assert_wait(lambda: TEST_TABLE_NAME in ch.get_tables(), retry_interval=0.5)

    # Check that all records were replicated
    assert_wait(
        lambda: len(ch.select(TEST_TABLE_NAME)) == num_records + 1,
        retry_interval=0.5,
        max_wait_time=300,
    )

    t2 = time.time()

    time_delta = t2 - t1
    rps = num_records / time_delta

    print("\n\n")
    print("*****************************")
    print("DB Replicator Initial Only Mode Performance:")
    print("records per second:", int(rps))
    print("total time (seconds):", round(time_delta, 2))
    print("*****************************")
    print("\n\n")

    # Clean up
    ch.drop_database(TEST_DB_NAME)

    # Now test with parallel replication
    print("running db_replicator with parallel initial replication")

    t1 = time.time()

    # Create a custom config file for testing with parallel replication
    parallel_config_file = "tests/configs/replicator/tests_config_perf_parallel.yaml"
    if os.path.exists(parallel_config_file):
        os.remove(parallel_config_file)

    with open(config_file, "r") as src_file:
        config_content = src_file.read()
    config_content += "\ninitial_replication_threads: 8\n"
    with open(parallel_config_file, "w") as dest_file:
        dest_file.write(config_content)

    # Use the DbReplicator directly to test the new parallel implementation
    db_replicator_runner = DbReplicatorRunner(
        TEST_DB_NAME, cfg_file=parallel_config_file
    )
    db_replicator_runner.run()

    # Make sure the database and table exist
    assert_wait(lambda: TEST_DB_NAME in ch.get_databases(), retry_interval=0.5)
    ch.execute_command(f"USE `{TEST_DB_NAME}`")
    assert_wait(lambda: TEST_TABLE_NAME in ch.get_tables(), retry_interval=0.5)

    # Check that all records were replicated
    assert_wait(
        lambda: len(ch.select(TEST_TABLE_NAME)) == num_records + 1,
        retry_interval=0.5,
        max_wait_time=300,
    )

    t2 = time.time()

    time_delta = t2 - t1
    rps = num_records / time_delta

    print("\n\n")
    print("*****************************")
    print("DB Replicator Parallel Mode Performance:")
    print("workers:", cfg.initial_replication_threads)
    print("records per second:", int(rps))
    print("total time (seconds):", round(time_delta, 2))
    print("*****************************")
    print("\n\n")

    db_replicator_runner.stop()

    # Clean up the temporary config file
    os.remove(parallel_config_file)
