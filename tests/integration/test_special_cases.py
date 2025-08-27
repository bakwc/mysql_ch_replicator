"""Integration tests for special cases and edge scenarios"""

import os
import tempfile
import time

import pytest
import yaml

from mysql_ch_replicator import clickhouse_api, mysql_api
from mysql_ch_replicator.binlog_replicator import BinlogReplicator
from mysql_ch_replicator.converter import MysqlToClickhouseConverter
from mysql_ch_replicator.db_replicator import State as DbReplicatorState
from tests.conftest import (
    CONFIG_FILE,
    TEST_DB_NAME,
    TEST_TABLE_NAME,
    BinlogReplicatorRunner,
    DbReplicatorRunner,
    RunAllRunner,
    assert_wait,
    get_binlog_replicator_pid,
    get_db_replicator_pid,
    kill_process,
    mysql_create_database,
    mysql_drop_database,
    prepare_env,
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
def test_schema_evolution_with_db_mapping(clean_environment):
    """Test case to reproduce issue where schema evolution doesn't work with database mapping."""
    # Use the predefined config file with database mapping
    config_file = "tests/configs/replicator/tests_config_db_mapping.yaml"

    cfg, mysql, ch = clean_environment
    cfg.load(config_file)

    # Note: Not setting a specific database in MySQL API
    mysql = mysql_api.MySQLApi(
        database=None,
        mysql_settings=cfg.mysql,
    )

    ch = clickhouse_api.ClickhouseApi(
        database="mapped_target_db",
        clickhouse_settings=cfg.clickhouse,
    )

    ch.drop_database("mapped_target_db")
    assert_wait(lambda: "mapped_target_db" not in ch.get_databases())

    prepare_env(cfg, mysql, ch, db_name=TEST_DB_NAME)

    # Create a test table with some columns using fully qualified name
    mysql.execute(f"""
CREATE TABLE `{TEST_DB_NAME}`.`{TEST_TABLE_NAME}` (
  `id` int NOT NULL,
  `name` varchar(255) NOT NULL,
  PRIMARY KEY (`id`)); 
    """)

    mysql.execute(
        f"INSERT INTO `{TEST_DB_NAME}`.`{TEST_TABLE_NAME}` (id, name) VALUES (1, 'Original')",
        commit=True,
    )

    # Start the replication
    binlog_replicator_runner = BinlogReplicatorRunner(cfg_file=config_file)
    binlog_replicator_runner.run()
    db_replicator_runner = DbReplicatorRunner(TEST_DB_NAME, cfg_file=config_file)
    db_replicator_runner.run()

    # Make sure initial replication works with the database mapping
    assert_wait(lambda: "mapped_target_db" in ch.get_databases())
    ch.execute_command("USE `mapped_target_db`")
    assert_wait(lambda: TEST_TABLE_NAME in ch.get_tables())
    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 1)

    # Now follow user's sequence of operations with fully qualified names (excluding RENAME operation)
    # 1. Add new column
    mysql.execute(
        f"ALTER TABLE `{TEST_DB_NAME}`.`{TEST_TABLE_NAME}` ADD COLUMN added_new_column char(1)",
        commit=True,
    )

    # 2. Rename the column
    mysql.execute(
        f"ALTER TABLE `{TEST_DB_NAME}`.`{TEST_TABLE_NAME}` RENAME COLUMN added_new_column TO rename_column_name",
        commit=True,
    )

    # 3. Modify column type
    mysql.execute(
        f"ALTER TABLE `{TEST_DB_NAME}`.`{TEST_TABLE_NAME}` MODIFY rename_column_name varchar(5)",
        commit=True,
    )

    # 4. Insert data using the modified schema
    mysql.execute(
        f"INSERT INTO `{TEST_DB_NAME}`.`{TEST_TABLE_NAME}` (id, name, rename_column_name) VALUES (2, 'Second', 'ABCDE')",
        commit=True,
    )

    # 5. Drop the column - this is where the error was reported
    mysql.execute(
        f"ALTER TABLE `{TEST_DB_NAME}`.`{TEST_TABLE_NAME}` DROP COLUMN rename_column_name",
        commit=True,
    )

    # 6. Add more inserts after schema changes to verify ongoing replication
    mysql.execute(
        f"INSERT INTO `{TEST_DB_NAME}`.`{TEST_TABLE_NAME}` (id, name) VALUES (3, 'Third record after drop column')",
        commit=True,
    )

    # Check if all changes were replicated correctly
    time.sleep(5)  # Allow time for processing the changes
    result = ch.select(TEST_TABLE_NAME)
    print(f"ClickHouse table contents: {result}")

    # Verify all records are present
    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 3)

    # Verify specific records exist
    records = ch.select(TEST_TABLE_NAME)
    print(f"Record type: {type(records[0])}")  # Debug the record type

    # Access by field name 'id' instead of by position
    record_ids = [record["id"] for record in records]
    assert 1 in record_ids, "Original record (id=1) not found"
    assert 3 in record_ids, "New record (id=3) after schema changes not found"

    # Note: This test confirms our fix for schema evolution with database mapping

    # Clean up
    db_replicator_runner.stop()
    binlog_replicator_runner.stop()


@pytest.mark.integration
def test_dynamic_column_addition_user_config(clean_environment):
    """Test to verify handling of dynamically added columns using user's exact configuration.

    This test reproduces the issue where columns are added on-the-fly via UPDATE
    rather than through ALTER TABLE statements, leading to an index error in the converter.
    """
    config_path = "tests/configs/replicator/tests_config_dynamic_column.yaml"

    cfg, mysql, ch = clean_environment
    cfg.load(config_path)

    mysql = mysql_api.MySQLApi(
        database=None,
        mysql_settings=cfg.mysql,
    )

    ch = clickhouse_api.ClickhouseApi(
        database=None,
        clickhouse_settings=cfg.clickhouse,
    )

    prepare_env(cfg, mysql, ch, db_name="test_replication")

    # Prepare environment - drop and recreate databases
    mysql_drop_database(mysql, "test_replication")
    mysql_create_database(mysql, "test_replication")
    mysql.set_database("test_replication")
    ch.drop_database("test_replication_ch")
    assert_wait(lambda: "test_replication_ch" not in ch.get_databases())

    # Create the exact table structure from the user's example
    mysql.execute("""
    CREATE TABLE test_replication.replication_data (
        code VARCHAR(255) NOT NULL PRIMARY KEY,
        val_1 VARCHAR(255) NOT NULL
    );
    """)

    # Insert initial data
    mysql.execute(
        "INSERT INTO test_replication.replication_data(code, val_1) VALUE ('test-1', '1');",
        commit=True,
    )

    # Start the replication processes
    binlog_replicator_runner = BinlogReplicatorRunner(cfg_file=config_path)
    binlog_replicator_runner.run()
    db_replicator_runner = DbReplicatorRunner("test_replication", cfg_file=config_path)
    db_replicator_runner.run()

    # Wait for initial replication to complete
    assert_wait(lambda: "test_replication_ch" in ch.get_databases())

    # Set the database before checking tables
    ch.execute_command("USE test_replication_ch")
    assert_wait(lambda: "replication_data" in ch.get_tables())
    assert_wait(lambda: len(ch.select("replication_data")) == 1)

    # Verify initial data was replicated correctly
    assert_wait(
        lambda: ch.select("replication_data", where="code='test-1'")[0]["val_1"] == "1"
    )

    # Update an existing field - this should work fine
    mysql.execute(
        "UPDATE test_replication.replication_data SET val_1 = '1200' WHERE code = 'test-1';",
        commit=True,
    )
    assert_wait(
        lambda: ch.select("replication_data", where="code='test-1'")[0]["val_1"]
        == "1200"
    )

    mysql.execute("USE test_replication")

    # Add val_2 column
    mysql.execute(
        "ALTER TABLE replication_data ADD COLUMN val_2 VARCHAR(255);", commit=True
    )

    # Now try to update with a field that doesn't exist
    # This would have caused an error before our fix
    mysql.execute(
        "UPDATE test_replication.replication_data SET val_2 = '100' WHERE code = 'test-1';",
        commit=True,
    )

    # Verify replication processes are still running
    binlog_pid = get_binlog_replicator_pid(cfg)
    db_pid = get_db_replicator_pid(cfg, "test_replication")

    assert binlog_pid is not None, "Binlog replicator process died"
    assert db_pid is not None, "DB replicator process died"

    # Verify the replication is still working after the dynamic column update
    mysql.execute(
        "UPDATE test_replication.replication_data SET val_1 = '1500' WHERE code = 'test-1';",
        commit=True,
    )
    assert_wait(
        lambda: ch.select("replication_data", where="code='test-1'")[0]["val_1"]
        == "1500"
    )

    print("Test passed - dynamic column was skipped without breaking replication")

    # Cleanup
    binlog_pid = get_binlog_replicator_pid(cfg)
    if binlog_pid:
        kill_process(binlog_pid)

    db_pid = get_db_replicator_pid(cfg, "test_replication")
    if db_pid:
        kill_process(db_pid)


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
def test_resume_initial_replication_with_ignore_deletes(clean_environment):
    """
    Test that resuming initial replication works correctly with ignore_deletes=True.

    This reproduces the bug from https://github.com/bakwc/mysql_ch_replicator/issues/172
    where resuming initial replication would fail with "Database sirocco_tmp does not exist"
    when ignore_deletes=True because the code would try to use the _tmp database instead
    of the target database directly.
    """
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

        # Set initial_replication_batch_size to 1 for testing
        config_data["initial_replication_batch_size"] = 1

        # Write to the temp file
        yaml.dump(config_data, temp_config_file)

    try:
        cfg, mysql, ch = clean_environment
        cfg.load(config_file)

        # Verify the ignore_deletes option was set
        assert cfg.ignore_deletes is True

        # Create a table with many records to ensure initial replication takes time
        mysql.execute(f"""
        CREATE TABLE `{TEST_TABLE_NAME}` (
            id int NOT NULL AUTO_INCREMENT,
            name varchar(255),
            data varchar(1000),
            PRIMARY KEY (id)
        )
        """)

        # Insert many records to make initial replication take longer
        for i in range(100):
            mysql.execute(
                f"INSERT INTO `{TEST_TABLE_NAME}` (name, data) VALUES ('test_{i}', 'data_{i}');",
                commit=True,
            )

        # Start binlog replicator
        binlog_replicator_runner = BinlogReplicatorRunner(cfg_file=config_file)
        binlog_replicator_runner.run()

        # Start db replicator for initial replication with test flag to exit early
        db_replicator_runner = DbReplicatorRunner(
            TEST_DB_NAME,
            cfg_file=config_file,
            additional_arguments="--initial-replication-test-fail-records 30",
        )
        db_replicator_runner.run()

        # Wait for initial replication to start
        assert_wait(lambda: TEST_DB_NAME in ch.get_databases())
        ch.execute_command(f"USE `{TEST_DB_NAME}`")
        assert_wait(lambda: TEST_TABLE_NAME in ch.get_tables())

        # Wait for some records to be replicated but not all (should hit the 30 record limit)
        assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) > 0)

        # The db replicator should have stopped automatically due to the test flag
        # But we still call stop() to ensure proper cleanup
        db_replicator_runner.stop()

        # Verify the state is still PERFORMING_INITIAL_REPLICATION
        state_path = os.path.join(
            cfg.binlog_replicator.data_dir, TEST_DB_NAME, "state.pckl"
        )
        state = DbReplicatorState(state_path)
        assert state.status.value == 2  # PERFORMING_INITIAL_REPLICATION

        # Add more records while replication is stopped
        for i in range(100, 150):
            mysql.execute(
                f"INSERT INTO `{TEST_TABLE_NAME}` (name, data) VALUES ('test_{i}', 'data_{i}');",
                commit=True,
            )

        # Verify that sirocco_tmp database does NOT exist (it should use sirocco directly)
        assert f"{TEST_DB_NAME}_tmp" not in ch.get_databases(), (
            "Temporary database should not exist with ignore_deletes=True"
        )

        # Resume initial replication - this should NOT fail with "Database sirocco_tmp does not exist"
        db_replicator_runner_2 = DbReplicatorRunner(TEST_DB_NAME, cfg_file=config_file)
        db_replicator_runner_2.run()

        # Wait for all records to be replicated (100 original + 50 extra = 150)
        assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 150, max_wait_time=30)

        # Verify the replication completed successfully
        records = ch.select(TEST_TABLE_NAME)
        assert len(records) == 150, f"Expected 150 records, got {len(records)}"

        # Verify we can continue with realtime replication
        mysql.execute(
            f"INSERT INTO `{TEST_TABLE_NAME}` (name, data) VALUES ('realtime_test', 'realtime_data');",
            commit=True,
        )
        assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 151)

        # Clean up
        db_replicator_runner_2.stop()
        binlog_replicator_runner.stop()

    finally:
        # Clean up temp config file
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


@pytest.mark.unit
def test_parse_mysql_table_structure():
    """Test parsing MySQL table structure from CREATE TABLE statement"""
    query = "CREATE TABLE IF NOT EXISTS user_preferences_portal (\n\t\t\tid char(36) NOT NULL,\n\t\t\tcategory varchar(50) DEFAULT NULL,\n\t\t\tdeleted tinyint(1) DEFAULT 0,\n\t\t\tdate_entered datetime DEFAULT NULL,\n\t\t\tdate_modified datetime DEFAULT NULL,\n\t\t\tassigned_user_id char(36) DEFAULT NULL,\n\t\t\tcontents longtext DEFAULT NULL\n\t\t ) ENGINE=InnoDB DEFAULT CHARSET=utf8"

    converter = MysqlToClickhouseConverter()

    structure = converter.parse_mysql_table_structure(query)

    assert structure.table_name == "user_preferences_portal"


@pytest.mark.unit
@pytest.mark.parametrize(
    "query,expected",
    [
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
        (
            """
        CREATE TABLE IF NOT EXISTS
        `multidb`
        .
        `multitable`
        (
          id INT,
          name VARCHAR(100)
        )
    """,
            "multidb",
        ),
        (
            """
        ALTER TABLE
        `justtable`
        ADD COLUMN age INT;
    """,
            "",
        ),
        (
            """
    CREATE TABLE `replication-test_db`.`test_table_2` (
        `id` int unsigned NOT NULL AUTO_INCREMENT,
        name varchar(255),
        PRIMARY KEY (id)
    )
    """,
            "replication-test_db",
        ),
        ("BEGIN", ""),
    ],
)
def test_parse_db_name_from_query(query, expected):
    """Test parsing database name from SQL queries"""
    assert BinlogReplicator._try_parse_db_name_from_query(query) == expected


@pytest.mark.unit
def test_alter_tokens_split():
    """Test ALTER TABLE token splitting functionality"""
    examples = [
        # basic examples from the prompt:
        ("test_name VARCHAR(254) NULL", ["test_name", "VARCHAR(254)", "NULL"]),
        (
            "factor NUMERIC(5, 2) DEFAULT NULL",
            ["factor", "NUMERIC(5, 2)", "DEFAULT", "NULL"],
        ),
        # backquoted column name:
        ("`test_name` VARCHAR(254) NULL", ["`test_name`", "VARCHAR(254)", "NULL"]),
        ("`order` INT NOT NULL", ["`order`", "INT", "NOT", "NULL"]),
        # type that contains a parenthesized list with quoted values:
        (
            "status ENUM('active','inactive') DEFAULT 'active'",
            ["status", "ENUM('active','inactive')", "DEFAULT", "'active'"],
        ),
        # multi‐word type definitions:
        ("col DOUBLE PRECISION DEFAULT 0", ["col", "DOUBLE PRECISION", "DEFAULT", "0"]),
        ("col INT UNSIGNED DEFAULT 0", ["col", "INT UNSIGNED", "DEFAULT", "0"]),
        # a case with a quoted string containing spaces and punctuation:
        (
            "message VARCHAR(100) DEFAULT 'Hello, world!'",
            ["message", "VARCHAR(100)", "DEFAULT", "'Hello, world!'"],
        ),
        # longer definition with more options:
        (
            "col DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
            [
                "col",
                "DATETIME",
                "DEFAULT",
                "CURRENT_TIMESTAMP",
                "ON",
                "UPDATE",
                "CURRENT_TIMESTAMP",
            ],
        ),
        # type with a COMMENT clause (here the type is given, then a parameter keyword)
        (
            "col VARCHAR(100) COMMENT 'This is a test comment'",
            ["col", "VARCHAR(100)", "COMMENT", "'This is a test comment'"],
        ),
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


@pytest.mark.integration
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
    mysql_structure, ch_structure = converter.parse_create_table_query(
        create_table_query
    )

    # Verify the parsing worked correctly
    assert mysql_structure.table_name == "test_table"
    assert len(mysql_structure.fields) == 17  # All columns should be parsed
    assert mysql_structure.primary_keys == ["id", "col_e"]


@pytest.mark.integration
@pytest.mark.skip(reason="Known bug - TRUNCATE operation not implemented")
def test_truncate_operation_bug_issue_155(clean_environment):
    """
    Test to reproduce the bug from issue #155.

    Bug Description: TRUNCATE operation is not replicated - data is not cleared on ClickHouse side

    This test should FAIL until the bug is fixed.
    When the bug is present: TRUNCATE will not clear ClickHouse data and the test will FAIL
    When the bug is fixed: TRUNCATE will clear ClickHouse data and the test will PASS
    """
    cfg, mysql, ch = clean_environment

    # Create a test table
    mysql.execute(f"""
CREATE TABLE `{TEST_TABLE_NAME}` (
    id int NOT NULL AUTO_INCREMENT,
    name varchar(255),
    age int,
    PRIMARY KEY (id)
); 
    """)

    # Insert test data
    mysql.execute(
        f"INSERT INTO `{TEST_TABLE_NAME}` (name, age) VALUES ('Alice', 25);",
        commit=True,
    )
    mysql.execute(
        f"INSERT INTO `{TEST_TABLE_NAME}` (name, age) VALUES ('Bob', 30);", commit=True
    )
    mysql.execute(
        f"INSERT INTO `{TEST_TABLE_NAME}` (name, age) VALUES ('Charlie', 35);",
        commit=True,
    )

    # Start replication
    binlog_replicator_runner = BinlogReplicatorRunner()
    binlog_replicator_runner.run()
    db_replicator_runner = DbReplicatorRunner(TEST_DB_NAME)
    db_replicator_runner.run()

    # Wait for initial replication
    assert_wait(lambda: TEST_DB_NAME in ch.get_databases())
    ch.execute_command(f"USE `{TEST_DB_NAME}`")
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
    assert ch_count_after_truncate == 0, (
        f"ClickHouse table should be empty after TRUNCATE, but contains {ch_count_after_truncate} records"
    )

    # Insert new data to verify replication still works after TRUNCATE
    mysql.execute(
        f"INSERT INTO `{TEST_TABLE_NAME}` (name, age) VALUES ('Dave', 40);", commit=True
    )
    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 1)

    # Verify the new record
    new_record = ch.select(TEST_TABLE_NAME, where="name='Dave'")
    assert len(new_record) == 1
    assert new_record[0]["age"] == 40

    # Clean up
    db_replicator_runner.stop()
    binlog_replicator_runner.stop()
