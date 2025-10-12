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

from common import *


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

    mysql.execute(f"""
CREATE TABLE `{TEST_TABLE_NAME}` (
    departments int(11) NOT NULL COMMENT '事件类型，可选值: ''SYSTEM'', ''BUSINESS''',
    termine int(11) NOT NULL COMMENT '事件类型，可选值: ''SYSTEM'', ''BUSINESS''',
    PRIMARY KEY (departments,termine)
)
""")


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
    cfg_file = 'tests/tests_config_parallel.yaml'
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
    cfg.load('tests/tests_config_databases_tables.yaml')

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

    run_all_runner = RunAllRunner(cfg_file='tests/tests_config_databases_tables.yaml')
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


def test_string_primary_key(monkeypatch):
    cfg = config.Settings()
    cfg.load('tests/tests_config_string_primary_key.yaml')

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

    binlog_replicator_runner = BinlogReplicatorRunner(cfg_file='tests/tests_config_string_primary_key.yaml')
    binlog_replicator_runner.run()
    db_replicator_runner = DbReplicatorRunner(TEST_DB_NAME, cfg_file='tests/tests_config_string_primary_key.yaml')
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


def test_parse_mysql_table_structure():
    query = "CREATE TABLE IF NOT EXISTS user_preferences_portal (\n\t\t\tid char(36) NOT NULL,\n\t\t\tcategory varchar(50) DEFAULT NULL,\n\t\t\tdeleted tinyint(1) DEFAULT 0,\n\t\t\tdate_entered datetime DEFAULT NULL,\n\t\t\tdate_modified datetime DEFAULT NULL,\n\t\t\tassigned_user_id char(36) DEFAULT NULL,\n\t\t\tcontents longtext DEFAULT NULL\n\t\t ) ENGINE=InnoDB DEFAULT CHARSET=utf8"

    converter = MysqlToClickhouseConverter()

    structure = converter.parse_mysql_table_structure(query)

    assert structure.table_name == 'user_preferences_portal'


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


def test_post_initial_replication_commands():
    config_file = 'tests/tests_config_post_commands.yaml'

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

    mysql.execute(f"""
CREATE TABLE `{TEST_TABLE_NAME}` (
    id int(11) NOT NULL AUTO_INCREMENT,
    event_time DATETIME NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
""")

    initial_inserts = [
        ('2024-01-01', 'type_0'),
        ('2024-01-02', 'type_1'),
        ('2024-01-03', 'type_2'),
        ('2024-01-01', 'type_0'),
        ('2024-01-02', 'type_1'),
        ('2024-01-03', 'type_2'),
        ('2024-01-01', 'type_0'),
        ('2024-01-02', 'type_1'),
        ('2024-01-03', 'type_2'),
        ('2024-01-04', 'type_0'),
    ]
    
    for date, event_type in initial_inserts:
        mysql.execute(
            f"INSERT INTO `{TEST_TABLE_NAME}` (event_time, event_type) VALUES ('{date} 10:00:00', '{event_type}');",
            commit=True
        )

    binlog_replicator_runner = BinlogReplicatorRunner(cfg_file=config_file)
    binlog_replicator_runner.run()

    db_replicator_runner = DbReplicatorRunner(TEST_DB_NAME, cfg_file=config_file)
    db_replicator_runner.run()

    assert_wait(lambda: TEST_DB_NAME in ch.get_databases())
    ch.execute_command(f'USE `{TEST_DB_NAME}`')
    assert_wait(lambda: TEST_TABLE_NAME in ch.get_tables())
    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 10)

    assert_wait(lambda: 'events_per_day' in ch.get_tables(), max_wait_time=15)
    assert_wait(lambda: 'events_mv' in ch.get_tables(), max_wait_time=15)
    
    ch.execute_command('OPTIMIZE TABLE events_per_day FINAL')
    
    def check_initial_data_ready():
        records = ch.select('events_per_day ORDER BY event_date, event_type')
        return len(records) > 0
    
    assert_wait(check_initial_data_ready, max_wait_time=10)
    
    events_per_day_records = ch.select('events_per_day ORDER BY event_date, event_type')
    assert len(events_per_day_records) > 0, "events_per_day should have aggregated data from initial replication backfill"
    
    aggregated_data = {}
    for record in events_per_day_records:
        date_str = str(record['event_date'])
        event_type = record['event_type']
        count = record['total_events']
        key = (date_str, event_type)
        aggregated_data[key] = count
    
    assert ('2024-01-01', 'type_0') in aggregated_data, "Should have 2024-01-01 + type_0"
    assert aggregated_data[('2024-01-01', 'type_0')] == 3, f"Expected 3 events for 2024-01-01 + type_0, got {aggregated_data[('2024-01-01', 'type_0')]}"
    
    assert ('2024-01-02', 'type_1') in aggregated_data, "Should have 2024-01-02 + type_1"
    assert aggregated_data[('2024-01-02', 'type_1')] == 3, f"Expected 3 events for 2024-01-02 + type_1, got {aggregated_data[('2024-01-02', 'type_1')]}"
    
    assert ('2024-01-03', 'type_2') in aggregated_data, "Should have 2024-01-03 + type_2"
    assert aggregated_data[('2024-01-03', 'type_2')] == 3, f"Expected 3 events for 2024-01-03 + type_2, got {aggregated_data[('2024-01-03', 'type_2')]}"
    
    assert ('2024-01-04', 'type_0') in aggregated_data, "Should have 2024-01-04 + type_0"
    assert aggregated_data[('2024-01-04', 'type_0')] == 1, f"Expected 1 event for 2024-01-04 + type_0, got {aggregated_data[('2024-01-04', 'type_0')]}"
    
    realtime_inserts = [
        ('2024-01-05', 'type_new', 3),
        ('2024-01-06', 'type_new', 2),
        ('2024-01-01', 'type_0', 2),
    ]
    
    for date, event_type, count in realtime_inserts:
        for _ in range(count):
            mysql.execute(
                f"INSERT INTO `{TEST_TABLE_NAME}` (event_time, event_type) VALUES ('{date} 12:00:00', '{event_type}');",
                commit=True
            )
    
    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 17)
    
    ch.execute_command('OPTIMIZE TABLE events_per_day FINAL')
    
    def check_realtime_aggregated():
        records = ch.select('events_per_day')
        agg = {}
        for r in records:
            key = (str(r['event_date']), r['event_type'])
            agg[key] = r['total_events']
        return (
            agg.get(('2024-01-05', 'type_new'), 0) == 3 and
            agg.get(('2024-01-06', 'type_new'), 0) == 2 and
            agg.get(('2024-01-01', 'type_0'), 0) == 5
        )
    
    assert_wait(check_realtime_aggregated, max_wait_time=15)
    
    ch.execute_command('OPTIMIZE TABLE events_per_day FINAL')
    
    def check_final_aggregated():
        final_records = ch.select('events_per_day ORDER BY event_date, event_type')
        final_aggregated_data = {}
        for record in final_records:
            date_str = str(record['event_date'])
            event_type = record['event_type']
            count = record['total_events']
            key = (date_str, event_type)
            final_aggregated_data[key] = count
        
        return (
            final_aggregated_data.get(('2024-01-05', 'type_new')) == 3 and
            final_aggregated_data.get(('2024-01-06', 'type_new')) == 2 and
            final_aggregated_data.get(('2024-01-01', 'type_0')) == 5
        )
    
    assert_wait(check_final_aggregated, max_wait_time=15)

    db_replicator_runner.stop()
    binlog_replicator_runner.stop()
