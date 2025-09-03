"""Tests for advanced process management scenarios"""

import time

import pytest

from tests.base import BaseReplicationTest, DataTestMixin, SchemaTestMixin
from tests.conftest import (
    TEST_DB_NAME,
    TEST_TABLE_NAME,
    RunAllRunner,
)
from tests.fixtures import TableSchemas


class TestAdvancedProcessManagement(
    BaseReplicationTest, SchemaTestMixin, DataTestMixin
):
    """Test advanced process management scenarios"""


    @pytest.mark.integration
    @pytest.mark.parametrize(
        "config_file",
        [
            "tests/configs/replicator/tests_config.yaml",
            "tests/configs/replicator/tests_config_parallel.yaml",
        ],
    )
    def test_run_all_runner_with_process_restart(self, config_file):
        """Test the run_all runner with comprehensive process restart functionality"""
        import time

        import requests

        from tests.conftest import (
            TEST_DB_NAME_2,
            TEST_DB_NAME_2_DESTINATION,
            get_binlog_replicator_pid,
            get_db_replicator_pid,
            kill_process,
            mysql_create_database,
            mysql_drop_database,
            mysql_drop_table,
        )

        # Load the specified config
        self.cfg.load(config_file)

        # Clean up secondary databases
        mysql_drop_database(self.mysql, TEST_DB_NAME_2)
        self.ch.drop_database(TEST_DB_NAME_2)
        self.ch.drop_database(TEST_DB_NAME_2_DESTINATION)

        # Create complex table with various data types and indexes
        self.mysql.execute(
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

        # Create reserved keyword table
        self.mysql.execute(
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

        # Insert initial data with spatial coordinates
        self.mysql.execute(
            f"INSERT INTO `{TEST_TABLE_NAME}` (name, age, coordinate) VALUES ('Ivan', 42, POINT(10.0, 20.0));",
            commit=True,
        )
        self.mysql.execute(
            f"INSERT INTO `{TEST_TABLE_NAME}` (name, age, coordinate) VALUES ('Peter', 33, POINT(10.0, 20.0));",
            commit=True,
        )
        self.mysql.execute(
            "INSERT INTO `group` (name, age, rate) VALUES ('Peter', 33, 10.2);",
            commit=True,
        )

        # Start the runner
        run_all_runner = RunAllRunner(cfg_file=config_file)
        run_all_runner.run()

        # Wait for replication to be established
        self.wait_for_condition(lambda: TEST_DB_NAME in self.ch.get_databases())
        self.ch.execute_command(f"USE `{TEST_DB_NAME}`;")
        self.wait_for_condition(lambda: "group" in self.ch.get_tables())

        # Test table drop operation
        mysql_drop_table(self.mysql, "group")
        self.wait_for_condition(lambda: "group" not in self.ch.get_tables())

        # Verify main table is working
        self.wait_for_condition(lambda: TEST_TABLE_NAME in self.ch.get_tables())
        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=2)

        # Insert more data to test ongoing replication
        self.mysql.execute(
            f"INSERT INTO `{TEST_TABLE_NAME}` (name, age, coordinate) VALUES ('Xeishfru32', 50, POINT(10.0, 20.0));",
            commit=True,
        )
        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=3)
        self.verify_record_exists(TEST_TABLE_NAME, "name='Xeishfru32'", {"age": 50})

        # Test process restart functionality - get process IDs
        binlog_repl_pid = get_binlog_replicator_pid(self.cfg)
        db_repl_pid = get_db_replicator_pid(self.cfg, TEST_DB_NAME)

        # Kill processes to simulate crash
        kill_process(binlog_repl_pid)
        kill_process(db_repl_pid, force=True)

        # Insert data while processes are down
        self.mysql.execute(
            f"INSERT INTO `{TEST_TABLE_NAME}` (name, rate, coordinate) VALUES ('John', 12.5, POINT(10.0, 20.0));",
            commit=True,
        )

        # Verify processes restart and catch up
        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=4)
        self.verify_record_exists(TEST_TABLE_NAME, "name='John'", {"rate": 12.5})

        # Test additional operations
        self.delete_records(TEST_TABLE_NAME, "name='John'")
        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=3)

        # Test multiple updates
        self.update_record(TEST_TABLE_NAME, "name='Ivan'", {"age": 66})
        self.wait_for_data_sync(TEST_TABLE_NAME, "name='Ivan'", 66, "age")

        self.update_record(TEST_TABLE_NAME, "name='Ivan'", {"age": 77})
        self.wait_for_data_sync(TEST_TABLE_NAME, "name='Ivan'", 77, "age")

        self.update_record(TEST_TABLE_NAME, "name='Ivan'", {"age": 88})
        self.wait_for_data_sync(TEST_TABLE_NAME, "name='Ivan'", 88, "age")

        # Insert more data including special characters
        self.mysql.execute(
            f"INSERT INTO `{TEST_TABLE_NAME}` (name, age, coordinate) VALUES ('Vlad', 99, POINT(10.0, 20.0));",
            commit=True,
        )
        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=4)

        # Test special character handling
        self.mysql.execute(
            f"INSERT INTO `{TEST_TABLE_NAME}` (name, age, coordinate) VALUES ('Hällo', 1912, POINT(10.0, 20.0));",
            commit=True,
        )
        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=5)
        self.verify_record_exists(TEST_TABLE_NAME, "age=1912", {"name": "Hällo"})

        # HTTP endpoint testing is covered by API integration tests
        # Core replication functionality already validated above

        # Test dynamic database creation
        mysql_create_database(self.mysql, TEST_DB_NAME_2)
        self.wait_for_condition(
            lambda: TEST_DB_NAME_2_DESTINATION in self.ch.get_databases()
        )

        # Create table in new database
        self.mysql.set_database(TEST_DB_NAME_2)
        self.mysql.execute("""
        CREATE TABLE `group` (
            id int NOT NULL AUTO_INCREMENT,
            name varchar(255) NOT NULL,
            age int,
            rate decimal(10,4),
            PRIMARY KEY (id)
        );
        """)

        # Table should appear in the mapped destination database
        self.wait_for_condition(
            lambda: "group" in self.ch.get_tables(TEST_DB_NAME_2_DESTINATION)
        )

        # Verify index creation in ClickHouse
        # Set ClickHouse context to the mapped destination database
        self.ch.execute_command(f"USE `{TEST_DB_NAME_2_DESTINATION}`")
        create_query = self.ch.show_create_table("group")
        assert "INDEX name_idx name TYPE ngrambf_v1" in create_query

        run_all_runner.stop()
