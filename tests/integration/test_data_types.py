"""Integration tests for MySQL data type handling and conversion"""

import datetime
import json
import uuid

import pytest

from tests.conftest import (
    CONFIG_FILE,
    TEST_DB_NAME,
    TEST_TABLE_NAME,
    RunAllRunner,
    assert_wait,
)


@pytest.mark.integration
def test_numeric_types_and_limits(clean_environment):
    """Test various numeric types and their limits"""
    cfg, mysql, ch = clean_environment

    mysql.execute("SET sql_mode = 'ALLOW_INVALID_DATES';")

    mysql.execute(f"""
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
    """)

    mysql.execute(
        f"INSERT INTO `{TEST_TABLE_NAME}` (name, test1, test2, test3, test4, test5, test6, test7, test8) VALUES "
        f"('Ivan', -20000, 50000, -30, 100, 16777200, 4294967290, 18446744073709551586, NULL);",
        commit=True,
    )

    run_all_runner = RunAllRunner()
    run_all_runner.run()

    assert_wait(lambda: TEST_DB_NAME in ch.get_databases())
    ch.execute_command(f"USE `{TEST_DB_NAME}`")
    assert_wait(lambda: TEST_TABLE_NAME in ch.get_tables())
    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 1)

    mysql.execute(
        f"INSERT INTO `{TEST_TABLE_NAME}` (name, test1, test2, test3, test4, test5, test6, test7, test8) VALUES "
        f"('Peter', -10000, 60000, -120, 250, 16777200, 4294967280, 18446744073709551586, NULL);",
        commit=True,
    )
    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 2)
    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME, "test2=60000")) == 1)
    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME, "test4=250")) == 1)
    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME, "test5=16777200")) == 2)
    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME, "test6=4294967290")) == 1)
    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME, "test6=4294967280")) == 1)
    assert_wait(
        lambda: len(ch.select(TEST_TABLE_NAME, "test7=18446744073709551586")) == 2
    )

    run_all_runner.stop()


@pytest.mark.integration
def test_complex_data_types(clean_environment):
    """Test complex data types like bit, point, binary, set, enum, timestamp, etc."""
    cfg, mysql, ch = clean_environment

    mysql.execute("SET sql_mode = 'ALLOW_INVALID_DATES';")

    mysql.execute(f"""
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
    """)

    mysql.execute(
        f"INSERT INTO `{TEST_TABLE_NAME}` (test1, test2, test3, test4, test5, test6, test7) VALUES "
        f"(0, POINT(10.0, 20.0), 'azaza', '1,3,5', '2023-08-15 14:30:00', '550e8400-e29b-41d4-a716-446655440000', 'def');",
        commit=True,
    )

    run_all_runner = RunAllRunner()
    run_all_runner.run()

    assert_wait(lambda: TEST_DB_NAME in ch.get_databases())
    ch.execute_command(f"USE `{TEST_DB_NAME}`")
    assert_wait(lambda: TEST_TABLE_NAME in ch.get_tables())
    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 1)

    mysql.execute(
        f"INSERT INTO `{TEST_TABLE_NAME}` (test1, test2, test4, test5, test6, test7) VALUES "
        f"(1, POINT(15.0, 14.0), '2,4,5', '2023-08-15 14:40:00', '110e6103-e39b-51d4-a716-826755413099', 'point');",
        commit=True,
    )

    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 2)
    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME, "test1=True")) == 1)

    assert ch.select(TEST_TABLE_NAME, "test1=True")[0]["test2"]["x"] == 15.0
    assert ch.select(TEST_TABLE_NAME, "test1=True")[0]["test7"] == "point"
    assert ch.select(TEST_TABLE_NAME, "test1=False")[0]["test2"]["y"] == 20.0
    assert ch.select(TEST_TABLE_NAME, "test1=False")[0]["test7"] == "def"
    assert (
        ch.select(TEST_TABLE_NAME, "test1=False")[0]["test3"]
        == "azaza\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"
    )

    assert ch.select(TEST_TABLE_NAME, "test1=True")[0]["test4"] == "2,4,5"
    assert ch.select(TEST_TABLE_NAME, "test1=False")[0]["test4"] == "1,3,5"

    value = ch.select(TEST_TABLE_NAME, "test1=True")[0]["test5"]
    assert isinstance(value, datetime.datetime)
    assert str(value) == "2023-08-15 14:40:00+00:00"

    assert ch.select(TEST_TABLE_NAME, "test1=True")[0]["test6"] == uuid.UUID(
        "110e6103-e39b-51d4-a716-826755413099"
    )

    mysql.execute(
        f"INSERT INTO `{TEST_TABLE_NAME}` (test1, test2) VALUES (0, NULL);",
        commit=True,
    )
    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 3)

    run_all_runner.stop()


@pytest.mark.integration
def test_json_data_type(clean_environment):
    """Test JSON data type handling"""
    cfg, mysql, ch = clean_environment

    mysql.execute("SET sql_mode = 'ALLOW_INVALID_DATES';")

    mysql.execute(f"""
CREATE TABLE `{TEST_TABLE_NAME}` (
    `id` int unsigned NOT NULL AUTO_INCREMENT,
    name varchar(255),
    data json,
    PRIMARY KEY (id)
); 
    """)

    mysql.execute(
        f"INSERT INTO `{TEST_TABLE_NAME}` (name, data) VALUES "
        + """('Ivan', '{"a": "b", "c": [1,2,3]}');""",
        commit=True,
    )

    run_all_runner = RunAllRunner()
    run_all_runner.run()

    assert_wait(lambda: TEST_DB_NAME in ch.get_databases())
    ch.execute_command(f"USE `{TEST_DB_NAME}`")
    assert_wait(lambda: TEST_TABLE_NAME in ch.get_tables())
    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 1)

    mysql.execute(
        f"INSERT INTO `{TEST_TABLE_NAME}` (name, data) VALUES "
        + """('Peter', '{"b": "b", "c": [3,2,1]}');""",
        commit=True,
    )
    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 2)

    assert json.loads(ch.select(TEST_TABLE_NAME, "name='Ivan'")[0]["data"])["c"] == [
        1,
        2,
        3,
    ]
    assert json.loads(ch.select(TEST_TABLE_NAME, "name='Peter'")[0]["data"])["c"] == [
        3,
        2,
        1,
    ]

    run_all_runner.stop()


@pytest.mark.integration
def test_json_unicode(clean_environment):
    """Test JSON with unicode characters"""
    cfg, mysql, ch = clean_environment

    mysql.execute("SET sql_mode = 'ALLOW_INVALID_DATES';")

    mysql.execute(f"""
CREATE TABLE `{TEST_TABLE_NAME}` (
    `id` int unsigned NOT NULL AUTO_INCREMENT,
    name varchar(255),
    data json,
    PRIMARY KEY (id)
); 
    """)

    mysql.execute(
        f"INSERT INTO `{TEST_TABLE_NAME}` (name, data) VALUES "
        + """('Ivan', '{"а": "б", "в": [1,2,3]}');""",
        commit=True,
    )

    run_all_runner = RunAllRunner()
    run_all_runner.run()

    assert_wait(lambda: TEST_DB_NAME in ch.get_databases())
    ch.execute_command(f"USE `{TEST_DB_NAME}`")
    assert_wait(lambda: TEST_TABLE_NAME in ch.get_tables())
    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 1)

    mysql.execute(
        f"INSERT INTO `{TEST_TABLE_NAME}` (name, data) VALUES "
        + """('Peter', '{"в": "б", "а": [3,2,1]}');""",
        commit=True,
    )
    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 2)

    assert json.loads(ch.select(TEST_TABLE_NAME, "name='Ivan'")[0]["data"])["в"] == [
        1,
        2,
        3,
    ]
    assert json.loads(ch.select(TEST_TABLE_NAME, "name='Peter'")[0]["data"])["в"] == "б"

    run_all_runner.stop()


@pytest.mark.integration
def test_year_type(clean_environment):
    """Test that MySQL YEAR type is properly converted to UInt16 in ClickHouse"""
    cfg, mysql, ch = clean_environment

    mysql.execute(f"""
    CREATE TABLE `{TEST_TABLE_NAME}` (
        id INT NOT NULL AUTO_INCREMENT,
        year_field YEAR NOT NULL,
        nullable_year YEAR,
        PRIMARY KEY (id)
    )
    """)

    # Insert test data with various year values
    mysql.execute(
        f"""
    INSERT INTO `{TEST_TABLE_NAME}` (year_field, nullable_year) VALUES 
    (2024, 2024),
    (1901, NULL),
    (2155, 2000),
    (2000, 1999);
    """,
        commit=True,
    )

    run_all_runner = RunAllRunner(cfg_file=CONFIG_FILE)
    run_all_runner.run()

    assert_wait(lambda: TEST_DB_NAME in ch.get_databases())
    ch.execute_command(f"USE `{TEST_DB_NAME}`")
    assert_wait(lambda: TEST_TABLE_NAME in ch.get_tables())
    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 4)

    # Get the ClickHouse data
    results = ch.select(TEST_TABLE_NAME)

    # Verify the data
    assert results[0]["year_field"] == 2024
    assert results[0]["nullable_year"] == 2024
    assert results[1]["year_field"] == 1901
    assert results[1]["nullable_year"] is None
    assert results[2]["year_field"] == 2155
    assert results[2]["nullable_year"] == 2000
    assert results[3]["year_field"] == 2000
    assert results[3]["nullable_year"] == 1999

    # Test realtime replication by adding more records
    mysql.execute(
        f"""
    INSERT INTO `{TEST_TABLE_NAME}` (year_field, nullable_year) VALUES 
    (2025, 2025),
    (1999, NULL),
    (2100, 2100);
    """,
        commit=True,
    )

    # Wait for new records to be replicated
    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 7)

    # Verify the new records - include order by in the where clause
    new_results = ch.select(
        TEST_TABLE_NAME, where="year_field >= 2025 ORDER BY year_field ASC"
    )
    assert len(new_results) == 3

    # Check specific values
    assert new_results[0]["year_field"] == 2025
    assert new_results[0]["nullable_year"] == 2025
    assert new_results[1]["year_field"] == 2100
    assert new_results[1]["nullable_year"] == 2100
    assert new_results[2]["year_field"] == 2155
    assert new_results[2]["nullable_year"] == 2000

    run_all_runner.stop()


@pytest.mark.integration
def test_enum_conversion(clean_environment):
    """Test that enum values are properly converted to lowercase in ClickHouse"""
    cfg, mysql, ch = clean_environment

    mysql.execute(f"""
    CREATE TABLE `{TEST_TABLE_NAME}` (
        id INT NOT NULL AUTO_INCREMENT, 
        status_mixed_case ENUM('Purchase','Sell','Transfer') NOT NULL,
        status_empty ENUM('Yes','No','Maybe'),
        PRIMARY KEY (id)
    )
    """)

    # Insert values with mixed case and NULL values
    mysql.execute(
        f"""
    INSERT INTO `{TEST_TABLE_NAME}` (status_mixed_case, status_empty) VALUES 
    ('Purchase', 'Yes'),
    ('Sell', NULL),
    ('Transfer', NULL);
    """,
        commit=True,
    )

    run_all_runner = RunAllRunner(cfg_file=CONFIG_FILE)
    run_all_runner.run()

    assert_wait(lambda: TEST_DB_NAME in ch.get_databases())
    ch.execute_command(f"USE `{TEST_DB_NAME}`")
    assert_wait(lambda: TEST_TABLE_NAME in ch.get_tables())
    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 3)

    # Get the ClickHouse data
    results = ch.select(TEST_TABLE_NAME)

    # Verify all values are properly converted
    assert results[0]["status_mixed_case"] == "purchase"
    assert results[1]["status_mixed_case"] == "sell"
    assert results[2]["status_mixed_case"] == "transfer"

    # Status_empty should handle NULL values correctly
    assert results[0]["status_empty"] == "yes"
    assert results[1]["status_empty"] is None
    assert results[2]["status_empty"] is None

    run_all_runner.stop()


@pytest.mark.integration
@pytest.mark.slow
def test_polygon_type(clean_environment):
    """Test that polygon type is properly converted and handled between MySQL and ClickHouse"""
    cfg, mysql, ch = clean_environment

    # Create a table with polygon type
    mysql.execute(f"""
    CREATE TABLE `{TEST_TABLE_NAME}` (
        id INT NOT NULL AUTO_INCREMENT,
        name VARCHAR(50) NOT NULL,
        area POLYGON NOT NULL,
        nullable_area POLYGON,
        PRIMARY KEY (id)
    )
    """)

    # Insert test data with polygons
    mysql.execute(
        f"""
    INSERT INTO `{TEST_TABLE_NAME}` (name, area, nullable_area) VALUES 
    ('Square', ST_GeomFromText('POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))'), ST_GeomFromText('POLYGON((0 0, 0 2, 2 2, 2 0, 0 0))')),
    ('Triangle', ST_GeomFromText('POLYGON((0 0, 1 0, 0.5 1, 0 0))'), NULL),
    ('Complex', ST_GeomFromText('POLYGON((0 0, 0 3, 3 3, 3 0, 0 0))'), ST_GeomFromText('POLYGON((1 1, 1 2, 2 2, 2 1, 1 1))'));
    """,
        commit=True,
    )

    run_all_runner = RunAllRunner(cfg_file=CONFIG_FILE)
    run_all_runner.run()

    assert_wait(lambda: TEST_DB_NAME in ch.get_databases())
    ch.execute_command(f"USE `{TEST_DB_NAME}`")
    assert_wait(lambda: TEST_TABLE_NAME in ch.get_tables())
    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 3)

    # Get the ClickHouse data
    results = ch.select(TEST_TABLE_NAME)

    # Verify the data
    assert len(results) == 3

    # Check first row (Square)
    assert results[0]["name"] == "Square"
    assert len(results[0]["area"]) == 5  # Square has 5 points (including closing point)
    assert len(results[0]["nullable_area"]) == 5
    # Verify some specific points
    assert results[0]["area"][0] == {"x": 0.0, "y": 0.0}
    assert results[0]["area"][1] == {"x": 0.0, "y": 1.0}
    assert results[0]["area"][2] == {"x": 1.0, "y": 1.0}
    assert results[0]["area"][3] == {"x": 1.0, "y": 0.0}
    assert results[0]["area"][4] == {"x": 0.0, "y": 0.0}  # Closing point

    # Check second row (Triangle)
    assert results[1]["name"] == "Triangle"
    assert (
        len(results[1]["area"]) == 4
    )  # Triangle has 4 points (including closing point)
    assert results[1]["nullable_area"] == []  # NULL values are returned as empty list

    run_all_runner.stop()
