"""Integration tests for special configuration scenarios"""

import os
import tempfile
import time

import pytest
import yaml

from tests.conftest import (
    CONFIG_FILE,
    TEST_DB_NAME,
    TEST_TABLE_NAME,
    BinlogReplicatorRunner,
    DbReplicatorRunner,
    RunAllRunner,
    assert_wait,
    read_logs,
)


@pytest.mark.integration
def test_string_primary_key(clean_environment):
    """Test replication with string primary keys"""
    cfg, mysql, ch = clean_environment
    cfg.load("tests/configs/replicator/tests_config_string_primary_key.yaml")

    mysql.execute("SET sql_mode = 'ALLOW_INVALID_DATES';")

    mysql.execute(f"""
CREATE TABLE `{TEST_TABLE_NAME}` (
    `id` char(30) NOT NULL,
    name varchar(255),
    PRIMARY KEY (id)
); 
    """)

    mysql.execute(
        f"INSERT INTO `{TEST_TABLE_NAME}` (id, name) VALUES " + """('01', 'Ivan');""",
        commit=True,
    )
    mysql.execute(
        f"INSERT INTO `{TEST_TABLE_NAME}` (id, name) VALUES " + """('02', 'Peter');""",
        commit=True,
    )

    binlog_replicator_runner = BinlogReplicatorRunner(
        cfg_file="tests/configs/replicator/tests_config_string_primary_key.yaml"
    )
    binlog_replicator_runner.run()
    db_replicator_runner = DbReplicatorRunner(
        TEST_DB_NAME,
        cfg_file="tests/configs/replicator/tests_config_string_primary_key.yaml",
    )
    db_replicator_runner.run()

    assert_wait(lambda: TEST_DB_NAME in ch.get_databases())

    ch.execute_command(f"USE `{TEST_DB_NAME}`")

    assert_wait(lambda: TEST_TABLE_NAME in ch.get_tables())
    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 2)

    mysql.execute(
        f"INSERT INTO `{TEST_TABLE_NAME}` (id, name) VALUES " + """('03', 'Filipp');""",
        commit=True,
    )
    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 3)

    db_replicator_runner.stop()
    binlog_replicator_runner.stop()


@pytest.mark.integration
def test_ignore_deletes(clean_environment):
    """Test ignore_deletes configuration option"""
    # Create a temporary config file with ignore_deletes=True
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".yaml", delete=False
    ) as temp_config_file:
        config_file = temp_config_file.name

        # Read the original config
        with open(CONFIG_FILE, "r") as original_config:
            config_data = yaml.safe_load(original_config)

        # Add ignore_deletes=True
        config_data["ignore_deletes"] = True

        # Write to the temp file
        yaml.dump(config_data, temp_config_file)

    try:
        cfg, mysql, ch = clean_environment
        cfg.load(config_file)

        # Verify the ignore_deletes option was set
        assert cfg.ignore_deletes is True

        # Create a table with a composite primary key
        mysql.execute(f"""
        CREATE TABLE `{TEST_TABLE_NAME}` (
            departments int(11) NOT NULL,
            termine int(11) NOT NULL,
            data varchar(255) NOT NULL,
            PRIMARY KEY (departments,termine)
        )
        """)

        # Insert initial records
        mysql.execute(
            f"INSERT INTO `{TEST_TABLE_NAME}` (departments, termine, data) VALUES (10, 20, 'data1');",
            commit=True,
        )
        mysql.execute(
            f"INSERT INTO `{TEST_TABLE_NAME}` (departments, termine, data) VALUES (30, 40, 'data2');",
            commit=True,
        )
        mysql.execute(
            f"INSERT INTO `{TEST_TABLE_NAME}` (departments, termine, data) VALUES (50, 60, 'data3');",
            commit=True,
        )

        # Run the replicator with ignore_deletes=True
        run_all_runner = RunAllRunner(cfg_file=config_file)
        run_all_runner.run()

        # Wait for replication to complete
        assert_wait(lambda: TEST_DB_NAME in ch.get_databases())
        ch.execute_command(f"USE `{TEST_DB_NAME}`")
        assert_wait(lambda: TEST_TABLE_NAME in ch.get_tables())
        assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 3)

        # Delete some records from MySQL
        mysql.execute(
            f"DELETE FROM `{TEST_TABLE_NAME}` WHERE departments=10;", commit=True
        )
        mysql.execute(
            f"DELETE FROM `{TEST_TABLE_NAME}` WHERE departments=30;", commit=True
        )

        # Wait a moment to ensure replication processes the events
        time.sleep(5)

        # Verify records are NOT deleted in ClickHouse (since ignore_deletes=True)
        # The count should still be 3
        assert len(ch.select(TEST_TABLE_NAME)) == 3, (
            "Deletions were processed despite ignore_deletes=True"
        )

        # Insert a new record and verify it's added
        mysql.execute(
            f"INSERT INTO `{TEST_TABLE_NAME}` (departments, termine, data) VALUES (70, 80, 'data4');",
            commit=True,
        )
        assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 4)

        # Verify the new record is correctly added
        result = ch.select(TEST_TABLE_NAME, where="departments=70 AND termine=80")
        assert len(result) == 1
        assert result[0]["data"] == "data4"

        # Clean up
        run_all_runner.stop()

        # Verify no errors occurred
        assert_wait(lambda: "stopping db_replicator" in read_logs(TEST_DB_NAME))
        assert "Traceback" not in read_logs(TEST_DB_NAME)

    finally:
        # Clean up the temporary config file
        os.unlink(config_file)


@pytest.mark.integration
def test_timezone_conversion(clean_environment):
    """
    Test that MySQL timestamp fields are converted to ClickHouse DateTime64 with custom timezone.
    This test reproduces the issue from GitHub issue #170.
    """
    # Create a temporary config file with custom timezone
    config_content = """
mysql:
  host: 'localhost'
  port: 9306
  user: 'root'
  password: 'admin'

clickhouse:
  host: 'localhost'
  port: 9123
  user: 'default'
  password: 'admin'

binlog_replicator:
  data_dir: '/app/binlog/'
  records_per_file: 100000

databases: '*test*'
log_level: 'debug'
mysql_timezone: 'America/New_York'
"""

    # Create temporary config file
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write(config_content)
        temp_config_file = f.name

    try:
        cfg, mysql, ch = clean_environment
        cfg.load(temp_config_file)

        # Verify timezone is loaded correctly
        assert cfg.mysql_timezone == "America/New_York"

        # Create table with timestamp fields
        mysql.execute(f"""
        CREATE TABLE `{TEST_TABLE_NAME}` (
            id int NOT NULL AUTO_INCREMENT,
            name varchar(255),
            created_at timestamp NULL,
            updated_at timestamp(3) NULL,
            PRIMARY KEY (id)
        );
        """)

        # Insert test data with specific timestamp
        mysql.execute(
            f"INSERT INTO `{TEST_TABLE_NAME}` (name, created_at, updated_at) "
            f"VALUES ('test_timezone', '2023-08-15 14:30:00', '2023-08-15 14:30:00.123');",
            commit=True,
        )

        # Run replication
        run_all_runner = RunAllRunner(cfg_file=temp_config_file)
        run_all_runner.run()

        assert_wait(lambda: TEST_DB_NAME in ch.get_databases())
        ch.execute_command(f"USE `{TEST_DB_NAME}`")
        assert_wait(lambda: TEST_TABLE_NAME in ch.get_tables())
        assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 1)

        # Get the table structure from ClickHouse
        table_info = ch.query(f"DESCRIBE `{TEST_TABLE_NAME}`")

        # Check that timestamp fields are converted to DateTime64 with timezone
        created_at_type = None
        updated_at_type = None
        for row in table_info.result_rows:
            if row[0] == "created_at":
                created_at_type = row[1]
            elif row[0] == "updated_at":
                updated_at_type = row[1]

        # Verify the types include the timezone
        assert created_at_type is not None
        assert updated_at_type is not None
        assert "America/New_York" in created_at_type
        assert "America/New_York" in updated_at_type

        # Verify data was inserted correctly
        results = ch.select(TEST_TABLE_NAME)
        assert len(results) == 1
        assert results[0]["name"] == "test_timezone"

        run_all_runner.stop()

    finally:
        # Clean up temporary config file
        os.unlink(temp_config_file)
