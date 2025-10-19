import datetime
import json
import os
import uuid
import zoneinfo

from common import *
from mysql_ch_replicator import clickhouse_api
from mysql_ch_replicator import config
from mysql_ch_replicator import mysql_api


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
    assert str(value) == '2023-08-15 14:40:00'

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
    config_file = 'tests/tests_config_timezone.yaml'
    
    cfg = config.Settings()
    cfg.load(config_file)
    
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
    run_all_runner = RunAllRunner(cfg_file=config_file)
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


def test_timezone_conversion_values():
    """
    Test that MySQL timestamp values are correctly preserved with timezone conversion.
    This test reproduces the issue from GitHub issue #177.
    """
    config_file = 'tests/tests_config_timezone.yaml'
    cfg = config.Settings()
    cfg.load(config_file)
    
    mysql = mysql_api.MySQLApi(
        database=None,
        mysql_settings=cfg.mysql,
        mysql_timezone=cfg.mysql_timezone,
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
        created_at timestamp NULL,
        updated_at timestamp(3) NULL,
        PRIMARY KEY (id)
    );
    ''')

    mysql.execute(
        f"INSERT INTO `{TEST_TABLE_NAME}` (name, created_at, updated_at) "
        f"VALUES ('test_timezone', '2023-08-15 14:30:00', '2023-08-15 14:30:00.123');",
        commit=True,
    )

    run_all_runner = RunAllRunner(cfg_file=config_file)
    run_all_runner.run()

    assert_wait(lambda: TEST_DB_NAME in ch.get_databases())
    ch.execute_command(f'USE `{TEST_DB_NAME}`')
    assert_wait(lambda: TEST_TABLE_NAME in ch.get_tables())
    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 1)

    results = ch.select(TEST_TABLE_NAME)
    assert len(results) == 1
    assert results[0]['name'] == 'test_timezone'
    
    created_at_value = results[0]['created_at']
    updated_at_value = results[0]['updated_at']
    
    expected_dt = datetime.datetime(2023, 8, 15, 14, 30, 0)
    ny_tz = zoneinfo.ZoneInfo('America/New_York')
    expected_dt_with_tz = expected_dt.replace(tzinfo=ny_tz)
    
    assert created_at_value == expected_dt_with_tz, f"Expected {expected_dt_with_tz}, got {created_at_value}"
    
    expected_dt_with_microseconds = datetime.datetime(2023, 8, 15, 14, 30, 0, 123000)
    expected_dt_with_microseconds_tz = expected_dt_with_microseconds.replace(tzinfo=ny_tz)
    assert updated_at_value == expected_dt_with_microseconds_tz, f"Expected {expected_dt_with_microseconds_tz}, got {updated_at_value}"
    
    run_all_runner.stop()


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

    # Create a table with polygon and multipolygon types
    mysql.execute(f'''
    CREATE TABLE `{TEST_TABLE_NAME}` (
        id INT NOT NULL AUTO_INCREMENT,
        name VARCHAR(50) NOT NULL,
        area POLYGON NOT NULL,
        nullable_area POLYGON,
        multi_area MULTIPOLYGON,
        PRIMARY KEY (id)
    )
    ''')

    # Insert test data with polygons
    # Using ST_GeomFromText to create polygons from WKT (Well-Known Text) format
    mysql.execute(f'''
    INSERT INTO `{TEST_TABLE_NAME}` (name, area, nullable_area, multi_area) VALUES 
    ('Square', ST_GeomFromText('POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))'), ST_GeomFromText('POLYGON((0 0, 0 2, 2 2, 2 0, 0 0))'), NULL),
    ('Triangle', ST_GeomFromText('POLYGON((0 0, 1 0, 0.5 1, 0 0))'), NULL, NULL),
    ('Complex', ST_GeomFromText('POLYGON((0 0, 0 3, 3 3, 3 0, 0 0))'), ST_GeomFromText('POLYGON((1 1, 1 2, 2 2, 2 1, 1 1))'), NULL);
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
    assert results[0]['multi_area'] == []  # NULL multipolygon values are returned as empty list
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
    assert results[1]['multi_area'] == []  # NULL multipolygon values are returned as empty list
    # Verify some specific points
    assert results[1]['area'][0] == {'x': 0.0, 'y': 0.0}
    assert results[1]['area'][1] == {'x': 1.0, 'y': 0.0}
    assert results[1]['area'][2] == {'x': 0.5, 'y': 1.0}
    assert results[1]['area'][3] == {'x': 0.0, 'y': 0.0}  # Closing point
    
    # Check third row (Complex)
    assert results[2]['name'] == 'Complex'
    assert len(results[2]['area']) == 5  # Outer square
    assert len(results[2]['nullable_area']) == 5  # Inner square
    assert results[2]['multi_area'] == []  # NULL multipolygon values are returned as empty list
    # Verify some specific points
    assert results[2]['area'][0] == {'x': 0.0, 'y': 0.0}
    assert results[2]['area'][2] == {'x': 3.0, 'y': 3.0}
    assert results[2]['nullable_area'][0] == {'x': 1.0, 'y': 1.0}
    assert results[2]['nullable_area'][2] == {'x': 2.0, 'y': 2.0}

    # Test realtime replication by adding more records
    mysql.execute(f'''
    INSERT INTO `{TEST_TABLE_NAME}` (name, area, nullable_area, multi_area) VALUES 
    ('Pentagon', ST_GeomFromText('POLYGON((0 0, 1 0, 1.5 1, 0.5 1.5, 0 0))'), ST_GeomFromText('POLYGON((0.2 0.2, 0.8 0.2, 1 0.8, 0.5 1, 0.2 0.2))'), NULL),
    ('Hexagon', ST_GeomFromText('POLYGON((0 0, 1 0, 1.5 0.5, 1 1, 0.5 1, 0 0))'), NULL, NULL),
    ('Circle', ST_GeomFromText('POLYGON((0 0, 0 2, 2 2, 2 0, 0 0))'), ST_GeomFromText('POLYGON((0.5 0.5, 0.5 1.5, 1.5 1.5, 1.5 0.5, 0.5 0.5))'), NULL);
    ''', commit=True)

    # Wait for new records to be replicated
    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 6)

    # Verify the new records using WHERE clauses
    # Check Pentagon
    pentagon = ch.select(TEST_TABLE_NAME, where="name='Pentagon'")[0]
    assert pentagon['name'] == 'Pentagon'
    assert len(pentagon['area']) == 5  # Pentagon has 5 points
    assert len(pentagon['nullable_area']) == 5  # Inner pentagon
    assert pentagon['multi_area'] == []  # NULL multipolygon values are returned as empty list
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
    assert hexagon['multi_area'] == []  # NULL multipolygon values are returned as empty list
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
    assert circle['multi_area'] == []  # NULL multipolygon values are returned as empty list
    assert abs(circle['area'][0]['x'] - 0.0) < 1e-6
    assert abs(circle['area'][0]['y'] - 0.0) < 1e-6
    assert abs(circle['area'][2]['x'] - 2.0) < 1e-6
    assert abs(circle['area'][2]['y'] - 2.0) < 1e-6
    assert abs(circle['nullable_area'][0]['x'] - 0.5) < 1e-6
    assert abs(circle['nullable_area'][0]['y'] - 0.5) < 1e-6
    assert abs(circle['nullable_area'][2]['x'] - 1.5) < 1e-6
    assert abs(circle['nullable_area'][2]['y'] - 1.5) < 1e-6

    # Test multipolygon type - insert a record with multipolygon data
    mysql.execute(f'''
    INSERT INTO `{TEST_TABLE_NAME}` (name, area, nullable_area, multi_area) VALUES 
    ('MultiSquares', 
     ST_GeomFromText('POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))'), 
     NULL,
     ST_GeomFromText('MULTIPOLYGON(((0 0, 0 1, 1 1, 1 0, 0 0)), ((2 2, 2 3, 3 3, 3 2, 2 2)))')
    );
    ''', commit=True)

    # Wait for the new record with multipolygon to be replicated
    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 7)
    
    # Verify the multipolygon data
    multi_squares = ch.select(TEST_TABLE_NAME, where="name='MultiSquares'")[0]
    assert multi_squares['name'] == 'MultiSquares'
    
    # Check that multi_area contains multiple polygons
    # The multipolygon should be represented as an array of polygon arrays
    assert isinstance(multi_squares['multi_area'], list)
    assert len(multi_squares['multi_area']) == 2  # Two polygons in the multipolygon
    
    # Check first polygon in multipolygon
    first_polygon = multi_squares['multi_area'][0]
    assert len(first_polygon) == 5  # Square has 5 points (including closing point)
    assert first_polygon[0] == {'x': 0.0, 'y': 0.0}
    assert first_polygon[1] == {'x': 0.0, 'y': 1.0}
    assert first_polygon[2] == {'x': 1.0, 'y': 1.0}
    assert first_polygon[3] == {'x': 1.0, 'y': 0.0}
    assert first_polygon[4] == {'x': 0.0, 'y': 0.0}  # Closing point
    
    # Check second polygon in multipolygon
    second_polygon = multi_squares['multi_area'][1]
    assert len(second_polygon) == 5  # Square has 5 points (including closing point)
    assert second_polygon[0] == {'x': 2.0, 'y': 2.0}
    assert second_polygon[1] == {'x': 2.0, 'y': 3.0}
    assert second_polygon[2] == {'x': 3.0, 'y': 3.0}
    assert second_polygon[3] == {'x': 3.0, 'y': 2.0}
    assert second_polygon[4] == {'x': 2.0, 'y': 2.0}  # Closing point

    run_all_runner.stop()
    assert_wait(lambda: 'stopping db_replicator' in read_logs(TEST_DB_NAME))
    assert('Traceback' not in read_logs(TEST_DB_NAME))


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


def test_charset_configuration():
    """
    Test that charset configuration is properly loaded and used for MySQL connections.
    This test verifies that utf8mb4 charset can be configured to properly handle
    4-byte Unicode characters in JSON fields.
    """
    config_file = 'tests/tests_config_charset.yaml'
    
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
