"""Integration tests for advanced replication scenarios"""

import os
import time

import pytest

from mysql_ch_replicator import clickhouse_api, config, mysql_api
from mysql_ch_replicator.binlog_replicator import State as BinlogState
from mysql_ch_replicator.db_replicator import State as DbReplicatorState
from tests.conftest import (
    CONFIG_FILE,
    TEST_DB_NAME,
    TEST_DB_NAME_2,
    TEST_DB_NAME_2_DESTINATION,
    TEST_TABLE_NAME,
    TEST_TABLE_NAME_2,
    BinlogReplicatorRunner,
    DbReplicatorRunner,
    RunAllRunner,
    assert_wait,
    kill_process,
    mysql_create_database,
    mysql_drop_database,
    mysql_drop_table,
    prepare_env,
    read_logs,
)


def get_binlog_replicator_pid(cfg: config.Settings):
    """Get binlog replicator process ID"""
    path = os.path.join(cfg.binlog_replicator.data_dir, "state.json")
    state = BinlogState(path)
    return state.pid


def get_db_replicator_pid(cfg: config.Settings, db_name: str):
    """Get database replicator process ID"""
    path = os.path.join(cfg.binlog_replicator.data_dir, db_name, "state.pckl")
    state = DbReplicatorState(path)
    return state.pid


@pytest.mark.integration
@pytest.mark.parametrize(
    "cfg_file", [CONFIG_FILE, "tests/configs/replicator/tests_config_parallel.yaml"]
)
def test_runner(clean_environment, cfg_file):
    """Test the run_all runner with process restart functionality"""
    cfg, mysql, ch = clean_environment
    cfg.load(cfg_file)

    mysql_drop_database(mysql, TEST_DB_NAME_2)
    ch.drop_database(TEST_DB_NAME_2)
    ch.drop_database(TEST_DB_NAME_2_DESTINATION)

    mysql.execute(
        f"""
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
    """,
        commit=True,
    )

    mysql.execute(
        """
    CREATE TABLE `group` (
        id int NOT NULL AUTO_INCREMENT,
        name varchar(255) NOT NULL,
        age int,
        rate decimal(10,4),
        PRIMARY KEY (id)
    ); 
        """,
        commit=True,
    )

    mysql.execute(
        f"INSERT INTO `{TEST_TABLE_NAME}` (name, age, coordinate) VALUES ('Ivan', 42, POINT(10.0, 20.0));",
        commit=True,
    )
    mysql.execute(
        f"INSERT INTO `{TEST_TABLE_NAME}` (name, age, coordinate) VALUES ('Peter', 33, POINT(10.0, 20.0));",
        commit=True,
    )

    mysql.execute(
        "INSERT INTO `group` (name, age, rate) VALUES ('Peter', 33, 10.2);", commit=True
    )

    run_all_runner = RunAllRunner(cfg_file=cfg_file)
    run_all_runner.run()

    assert_wait(lambda: TEST_DB_NAME in ch.get_databases())

    ch.execute_command(f"USE `{TEST_DB_NAME}`;")

    assert_wait(lambda: "group" in ch.get_tables())

    mysql_drop_table(mysql, "group")

    assert_wait(lambda: "group" not in ch.get_databases())

    assert_wait(lambda: TEST_TABLE_NAME in ch.get_tables())
    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 2)

    mysql.execute(
        f"INSERT INTO `{TEST_TABLE_NAME}` (name, age, coordinate) VALUES ('Xeishfru32', 50, POINT(10.0, 20.0));",
        commit=True,
    )
    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 3)
    assert_wait(
        lambda: ch.select(TEST_TABLE_NAME, where="name='Xeishfru32'")[0]["age"] == 50
    )

    # Test for restarting dead processes
    binlog_repl_pid = get_binlog_replicator_pid(cfg)
    db_repl_pid = get_db_replicator_pid(cfg, TEST_DB_NAME)

    kill_process(binlog_repl_pid)
    kill_process(db_repl_pid, force=True)

    mysql.execute(
        f"INSERT INTO `{TEST_TABLE_NAME}` (name, rate, coordinate) VALUES ('John', 12.5, POINT(10.0, 20.0));",
        commit=True,
    )
    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 4)
    assert_wait(
        lambda: ch.select(TEST_TABLE_NAME, where="name='John'")[0]["rate"] == 12.5
    )

    mysql.execute(f"DELETE FROM `{TEST_TABLE_NAME}` WHERE name='John';", commit=True)
    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 3)

    mysql.execute(
        f"UPDATE `{TEST_TABLE_NAME}` SET age=66 WHERE name='Ivan'", commit=True
    )
    assert_wait(lambda: ch.select(TEST_TABLE_NAME, "name='Ivan'")[0]["age"] == 66)

    mysql.execute(
        f"UPDATE `{TEST_TABLE_NAME}` SET age=77 WHERE name='Ivan'", commit=True
    )
    assert_wait(lambda: ch.select(TEST_TABLE_NAME, "name='Ivan'")[0]["age"] == 77)

    mysql.execute(
        f"UPDATE `{TEST_TABLE_NAME}` SET age=88 WHERE name='Ivan'", commit=True
    )
    assert_wait(lambda: ch.select(TEST_TABLE_NAME, "name='Ivan'")[0]["age"] == 88)

    mysql.execute(
        f"INSERT INTO `{TEST_TABLE_NAME}` (name, age, coordinate) VALUES ('Vlad', 99, POINT(10.0, 20.0));",
        commit=True,
    )

    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 4)

    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME, final=False)) == 4)

    mysql.execute(
        command=f"INSERT INTO `{TEST_TABLE_NAME}` (name, age, coordinate) VALUES (%s, %s, POINT(10.0, 20.0));",
        args=(b"H\xe4llo".decode("latin-1"), 1912),
        commit=True,
    )

    assert_wait(lambda: TEST_DB_NAME in ch.get_databases())

    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 5)
    assert_wait(lambda: ch.select(TEST_TABLE_NAME, "age=1912")[0]["name"] == "Hällo")

    ch.drop_database(TEST_DB_NAME)
    ch.drop_database(TEST_DB_NAME_2)

    import requests

    requests.get("http://localhost:9128/restart_replication")
    time.sleep(1.0)

    assert_wait(lambda: TEST_DB_NAME in ch.get_databases())

    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 5)
    assert_wait(lambda: ch.select(TEST_TABLE_NAME, "age=1912")[0]["name"] == "Hällo")

    mysql_create_database(mysql, TEST_DB_NAME_2)
    assert_wait(lambda: TEST_DB_NAME_2_DESTINATION in ch.get_databases())

    mysql.execute("""
    CREATE TABLE `group` (
        id int NOT NULL AUTO_INCREMENT,
        name varchar(255) NOT NULL,
        age int,
        rate decimal(10,4),
        PRIMARY KEY (id)
    ); 
        """)

    assert_wait(lambda: "group" in ch.get_tables())

    create_query = ch.show_create_table("group")
    assert "INDEX name_idx name TYPE ngrambf_v1" in create_query

    run_all_runner.stop()


@pytest.mark.integration
def test_multi_column_erase(clean_environment):
    """Test multi-column primary key deletion"""
    cfg, mysql, ch = clean_environment

    mysql_drop_database(mysql, TEST_DB_NAME_2)
    ch.drop_database(TEST_DB_NAME_2_DESTINATION)

    mysql.execute(f"""
CREATE TABLE `{TEST_TABLE_NAME}` (
    departments int(11) NOT NULL,
    termine int(11) NOT NULL,
    PRIMARY KEY (departments,termine)
)
""")

    mysql.execute(
        f"INSERT INTO `{TEST_TABLE_NAME}` (departments, termine) VALUES (10, 20);",
        commit=True,
    )
    mysql.execute(
        f"INSERT INTO `{TEST_TABLE_NAME}` (departments, termine) VALUES (30, 40);",
        commit=True,
    )
    mysql.execute(
        f"INSERT INTO `{TEST_TABLE_NAME}` (departments, termine) VALUES (50, 60);",
        commit=True,
    )
    mysql.execute(
        f"INSERT INTO `{TEST_TABLE_NAME}` (departments, termine) VALUES (20, 10);",
        commit=True,
    )
    mysql.execute(
        f"INSERT INTO `{TEST_TABLE_NAME}` (departments, termine) VALUES (40, 30);",
        commit=True,
    )
    mysql.execute(
        f"INSERT INTO `{TEST_TABLE_NAME}` (departments, termine) VALUES (60, 50);",
        commit=True,
    )

    run_all_runner = RunAllRunner(cfg_file=CONFIG_FILE)
    run_all_runner.run()

    assert_wait(lambda: TEST_DB_NAME in ch.get_databases())

    ch.execute_command(f"USE `{TEST_DB_NAME}`")

    assert_wait(lambda: TEST_TABLE_NAME in ch.get_tables())
    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 6)

    mysql.execute(f"DELETE FROM `{TEST_TABLE_NAME}` WHERE departments=10;", commit=True)
    mysql.execute(f"DELETE FROM `{TEST_TABLE_NAME}` WHERE departments=30;", commit=True)
    mysql.execute(f"DELETE FROM `{TEST_TABLE_NAME}` WHERE departments=50;", commit=True)

    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 3)

    run_all_runner.stop()

    assert_wait(lambda: "stopping db_replicator" in read_logs(TEST_DB_NAME))
    assert "Traceback" not in read_logs(TEST_DB_NAME)


@pytest.mark.integration
def test_parallel_initial_replication_record_versions(clean_environment):
    """
    Test that record versions are properly consolidated from worker states
    after parallel initial replication.
    """
    # Only run this test with parallel configuration
    cfg_file = "tests/configs/replicator/tests_config_parallel.yaml"
    cfg, mysql, ch = clean_environment
    cfg.load(cfg_file)

    # Ensure we have parallel replication configured
    assert cfg.initial_replication_threads > 1, (
        "This test requires initial_replication_threads > 1"
    )

    # Create a table with sufficient records for parallel processing
    mysql.execute(f"""
CREATE TABLE `{TEST_TABLE_NAME}` (
    id int NOT NULL AUTO_INCREMENT,
    name varchar(255),
    age int,
    version int NOT NULL DEFAULT 1,
    PRIMARY KEY (id)
); 
    """)

    # Insert a large number of records to ensure parallel processing
    # Use a single connection context to ensure all operations use the same connection
    with mysql.get_connection() as (connection, cursor):
        for i in range(1, 1001):
            cursor.execute(
                f"INSERT INTO `{TEST_TABLE_NAME}` (name, age, version) VALUES ('User{i}', {20 + i % 50}, {i});"
            )
            if i % 100 == 0:  # Commit every 100 records
                connection.commit()

        # Ensure final commit for any remaining uncommitted records (records 901-1000)
        connection.commit()

    # Run initial replication only with parallel workers
    db_replicator_runner = DbReplicatorRunner(TEST_DB_NAME, cfg_file=cfg_file)
    db_replicator_runner.run()

    assert_wait(lambda: TEST_DB_NAME in ch.get_databases(), max_wait_time=10.0)

    ch.execute_command(f"USE `{TEST_DB_NAME}`")

    assert_wait(lambda: TEST_TABLE_NAME in ch.get_tables(), max_wait_time=10.0)
    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 1000, max_wait_time=10.0)

    db_replicator_runner.stop()

    # Verify database and table were created
    assert TEST_DB_NAME in ch.get_databases()
    ch.execute_command(f"USE `{TEST_DB_NAME}`")
    assert TEST_TABLE_NAME in ch.get_tables()

    # Verify all records were replicated
    records = ch.select(TEST_TABLE_NAME)
    assert len(records) == 1000

    # Instead of reading the state file directly, verify the record versions are correctly handled
    # by checking the max _version in the ClickHouse table
    versions_query = ch.query(
        f"SELECT MAX(_version) FROM `{TEST_DB_NAME}`.`{TEST_TABLE_NAME}`"
    )
    max_version_in_ch = versions_query.result_rows[0][0]
    assert max_version_in_ch >= 200, (
        f"Expected max _version to be at least 200, got {max_version_in_ch}"
    )

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
        commit=True,
    )

    # Wait for the record to be replicated
    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 1001)

    # Verify the new record was replicated correctly
    realtime_record = ch.select(TEST_TABLE_NAME, where="name='UserRealtime'")[0]
    assert realtime_record["age"] == 99
    assert realtime_record["version"] == 1001

    # Check that the _version column in CH is a reasonable value
    # With parallel workers, the _version won't be > 1000 because each worker
    # has its own independent version counter and they never intersect
    versions_query = ch.query(
        f"SELECT _version FROM `{TEST_DB_NAME}`.`{TEST_TABLE_NAME}` WHERE name='UserRealtime'"
    )
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


@pytest.mark.integration
def test_database_tables_filtering(clean_environment):
    """Test database and table filtering functionality"""
    cfg, mysql, ch = clean_environment
    cfg.load("tests/configs/replicator/tests_config_databases_tables.yaml")

    mysql = mysql_api.MySQLApi(
        database=None,
        mysql_settings=cfg.mysql,
    )

    ch = clickhouse_api.ClickhouseApi(
        database="test_db_2",
        clickhouse_settings=cfg.clickhouse,
    )

    mysql_drop_database(mysql, "test_db_3")
    mysql_drop_database(mysql, "test_db_12")

    mysql_create_database(mysql, "test_db_3")
    mysql_create_database(mysql, "test_db_12")

    ch.drop_database("test_db_3")
    ch.drop_database("test_db_12")

    prepare_env(cfg, mysql, ch, db_name="test_db_2")

    mysql.execute("""
    CREATE TABLE test_table_15 (
        id int NOT NULL AUTO_INCREMENT,
        name varchar(255),
        age int,
        PRIMARY KEY (id)
    ); 
        """)

    mysql.execute("""
    CREATE TABLE test_table_142 (
        id int NOT NULL AUTO_INCREMENT,
        name varchar(255),
        age int,
        PRIMARY KEY (id)
    ); 
        """)

    mysql.execute("""
    CREATE TABLE test_table_143 (
        id int NOT NULL AUTO_INCREMENT,
        name varchar(255),
        age int,
        PRIMARY KEY (id)
    ); 
        """)

    mysql.execute("""
CREATE TABLE test_table_3 (
    id int NOT NULL AUTO_INCREMENT,
    name varchar(255),
    age int,
    PRIMARY KEY (id)
); 
    """)

    mysql.execute("""
    CREATE TABLE test_table_2 (
        id int NOT NULL AUTO_INCREMENT,
        name varchar(255),
        age int,
        PRIMARY KEY (id)
    ); 
        """)

    mysql.execute(
        "INSERT INTO test_table_3 (name, age) VALUES ('Ivan', 42);", commit=True
    )
    mysql.execute(
        "INSERT INTO test_table_2 (name, age) VALUES ('Ivan', 42);", commit=True
    )

    run_all_runner = RunAllRunner(
        cfg_file="tests/configs/replicator/tests_config_databases_tables.yaml"
    )
    run_all_runner.run()

    assert_wait(lambda: "test_db_2" in ch.get_databases())
    assert "test_db_3" not in ch.get_databases()
    assert "test_db_12" not in ch.get_databases()

    ch.execute_command("USE test_db_2")

    assert_wait(lambda: "test_table_2" in ch.get_tables())
    assert_wait(lambda: len(ch.select("test_table_2")) == 1)

    assert_wait(lambda: "test_table_143" in ch.get_tables())

    assert "test_table_3" not in ch.get_tables()

    assert "test_table_15" not in ch.get_tables()
    assert "test_table_142" not in ch.get_tables()

    run_all_runner.stop()


@pytest.mark.integration
def test_datetime_exception(clean_environment):
    """Test handling of invalid datetime values"""
    cfg, mysql, ch = clean_environment

    # Use a single connection context to ensure SQL mode persists
    # across all operations due to connection pooling
    with mysql.get_connection() as (connection, cursor):
        cursor.execute("SET sql_mode = 'ALLOW_INVALID_DATES';")

        cursor.execute(f"""
CREATE TABLE `{TEST_TABLE_NAME}` (
    id int NOT NULL AUTO_INCREMENT,
    name varchar(255),
    modified_date DateTime(3) NOT NULL,
    test_date date NOT NULL,
        PRIMARY KEY (id)
    );
        """)

        cursor.execute(
            f"INSERT INTO `{TEST_TABLE_NAME}` (name, modified_date, test_date) "
            f"VALUES ('Ivan', '0000-00-00 00:00:00', '2015-05-28');"
        )
        connection.commit()

    binlog_replicator_runner = BinlogReplicatorRunner()
    binlog_replicator_runner.run()
    db_replicator_runner = DbReplicatorRunner(TEST_DB_NAME)
    db_replicator_runner.run()

    assert_wait(lambda: TEST_DB_NAME in ch.get_databases())

    ch.execute_command(f"USE `{TEST_DB_NAME}`")

    assert_wait(lambda: TEST_TABLE_NAME in ch.get_tables())
    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 1)

    # Continue using the same SQL mode for subsequent operations
    with mysql.get_connection() as (connection, cursor):
        cursor.execute("SET sql_mode = 'ALLOW_INVALID_DATES';")

        cursor.execute(
            f"INSERT INTO `{TEST_TABLE_NAME}` (name, modified_date, test_date) "
            f"VALUES ('Alex', '0000-00-00 00:00:00', '2015-06-02');"
        )
        cursor.execute(
            f"INSERT INTO `{TEST_TABLE_NAME}` (name, modified_date, test_date) "
            f"VALUES ('Givi', '2023-01-08 03:11:09', '2015-06-02');"
        )
        connection.commit()
    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 3)
    assert_wait(
        lambda: str(ch.select(TEST_TABLE_NAME, where="name='Alex'")[0]["test_date"])
        == "2015-06-02"
    )
    assert_wait(
        lambda: str(ch.select(TEST_TABLE_NAME, where="name='Ivan'")[0]["test_date"])
        == "2015-05-28"
    )

    db_replicator_runner.stop()
    binlog_replicator_runner.stop()


@pytest.mark.integration
def test_different_types_1(clean_environment):
    """Test various MySQL data types with complex schema"""
    cfg, mysql, ch = clean_environment

    # Use single connection context to ensure SQL mode persists across operations
    with mysql.get_connection() as (connection, cursor):
        cursor.execute("SET sql_mode = 'ALLOW_INVALID_DATES';")

        cursor.execute(f"""
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
        """)

        cursor.execute(
            f"INSERT INTO `{TEST_DB_NAME}`.`{TEST_TABLE_NAME}` (name, modified_date) VALUES ('Ivan', '0000-00-00 00:00:00');"
        )
        connection.commit()

    binlog_replicator_runner = BinlogReplicatorRunner()
    binlog_replicator_runner.run()
    db_replicator_runner = DbReplicatorRunner(TEST_DB_NAME)
    db_replicator_runner.run()

    assert_wait(lambda: TEST_DB_NAME in ch.get_databases())

    ch.execute_command(f"USE `{TEST_DB_NAME}`")

    assert_wait(lambda: TEST_TABLE_NAME in ch.get_tables())
    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 1)

    # Use the same SQL mode for additional invalid date operations
    with mysql.get_connection() as (connection, cursor):
        cursor.execute("SET sql_mode = 'ALLOW_INVALID_DATES';")

        cursor.execute(
            f"INSERT INTO `{TEST_DB_NAME}`.`{TEST_TABLE_NAME}` (name, modified_date) VALUES ('Alex', '0000-00-00 00:00:00');"
        )
        cursor.execute(
            f"INSERT INTO `{TEST_DB_NAME}`.`{TEST_TABLE_NAME}` (name, modified_date) VALUES ('Givi', '2023-01-08 03:11:09');"
        )
        connection.commit()
    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 3)

    mysql.execute(f"""
    CREATE TABLE `{TEST_DB_NAME}`.`{TEST_TABLE_NAME_2}` (
        `id` int unsigned NOT NULL AUTO_INCREMENT,
        name varchar(255),
        PRIMARY KEY (id)
    ); 
        """)

    mysql.execute(
        f"INSERT INTO `{TEST_DB_NAME}`.`{TEST_TABLE_NAME_2}` (name) VALUES ('Ivan');",
        commit=True,
    )

    assert_wait(lambda: TEST_TABLE_NAME_2 in ch.get_tables())

    db_replicator_runner.stop()
    binlog_replicator_runner.stop()
