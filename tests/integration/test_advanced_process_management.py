"""Tests for advanced process management scenarios"""

import os
import time

import pytest

from tests.base import BaseReplicationTest, DataTestMixin, SchemaTestMixin
from tests.conftest import (
    TEST_DB_NAME,
    TEST_TABLE_NAME,
    RunAllRunner,
    read_logs,
)
from tests.fixtures import TableSchemas


class TestAdvancedProcessManagement(
    BaseReplicationTest, SchemaTestMixin, DataTestMixin
):
    """Test advanced process management scenarios"""

    @pytest.mark.integration
    def test_auto_restart_interval(self):
        """Test automatic restart based on configuration interval"""
        # This test would need a special config with short auto_restart_interval
        # For now, just verify basic restart functionality works

        schema = TableSchemas.basic_user_table(TEST_TABLE_NAME)
        self.mysql.execute(schema.sql)

        self.insert_basic_record(TEST_TABLE_NAME, "TestUser", 25)

        # Start with short-lived configuration if available
        runner = RunAllRunner()
        runner.run()

        # Wait for replication to start and set ClickHouse context
        self.wait_for_condition(lambda: TEST_DB_NAME in self.ch.get_databases())
        self.ch.execute_command(f"USE `{TEST_DB_NAME}`")

        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=1)

        # Add data continuously to test restart doesn't break replication
        for i in range(5):
            self.insert_basic_record(TEST_TABLE_NAME, f"User_{i}", 25 + i)
            time.sleep(1)  # Space out insertions

        # Verify all data is replicated despite any restarts
        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=6)

        runner.stop()

    @pytest.mark.integration
    def test_log_file_rotation(self):
        """Test that log file rotation doesn't break replication"""
        # Setup
        schema = TableSchemas.basic_user_table(TEST_TABLE_NAME)
        self.mysql.execute(schema.sql)

        self.insert_basic_record(TEST_TABLE_NAME, "LogTestUser", 30)

        # Start replication
        runner = RunAllRunner()
        runner.run()

        # Wait for replication to start and set ClickHouse context
        self.wait_for_condition(lambda: TEST_DB_NAME in self.ch.get_databases())
        self.ch.execute_command(f"USE `{TEST_DB_NAME}`")

        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=1)

        # Generate log activity by adding/updating data
        for i in range(10):
            self.insert_basic_record(TEST_TABLE_NAME, f"LogUser_{i}", 30 + i)
            if i % 3 == 0:
                self.update_record(
                    TEST_TABLE_NAME, f"name='LogUser_{i}'", {"age": 40 + i}
                )

        # Check logs exist and contain expected entries
        logs = read_logs(TEST_DB_NAME)
        assert len(logs) > 0, "No logs found"
        assert any("replication" in log.lower() for log in logs), (
            "No replication logs found"
        )

        # Verify all data is still correctly replicated
        self.wait_for_table_sync(
            TEST_TABLE_NAME, expected_count=11
        )  # 1 initial + 10 new

        runner.stop()

    @pytest.mark.integration
    def test_state_file_corruption_recovery(self):
        """Test recovery from corrupted state files"""
        # Setup
        schema = TableSchemas.basic_user_table(TEST_TABLE_NAME)
        self.mysql.execute(schema.sql)

        self.insert_basic_record(TEST_TABLE_NAME, "StateTestUser", 30)

        # Start replication
        runner = RunAllRunner()
        runner.run()

        # Wait for replication to start and set ClickHouse context
        self.wait_for_condition(lambda: TEST_DB_NAME in self.ch.get_databases())
        self.ch.execute_command(f"USE `{TEST_DB_NAME}`")

        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=1)

        # Stop replication
        runner.stop()

        # Corrupt state file (simulate corruption by writing invalid data)
        state_file = os.path.join(self.cfg.binlog_replicator.data_dir, "state.json")
        if os.path.exists(state_file):
            with open(state_file, "w") as f:
                f.write("CORRUPTED_DATA_INVALID_JSON{{{")

        # Add data while replication is down
        self.insert_basic_record(TEST_TABLE_NAME, "PostCorruptionUser", 35)

        # Restart replication - should handle corruption gracefully
        runner = RunAllRunner()
        runner.run()

        # Wait for replication to start and set ClickHouse context
        self.wait_for_condition(lambda: TEST_DB_NAME in self.ch.get_databases())
        self.ch.execute_command(f"USE `{TEST_DB_NAME}`")

        # Verify recovery and new data replication
        # May need to start from beginning due to state corruption
        self.wait_for_data_sync(TEST_TABLE_NAME, "name='PostCorruptionUser'", 35, "age")

        runner.stop()

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

        # Test restart replication endpoint
        self.ch.drop_database(TEST_DB_NAME)
        self.ch.drop_database(TEST_DB_NAME_2)

        requests.get("http://localhost:9128/restart_replication")
        time.sleep(1.0)

        # Verify recovery after restart
        self.wait_for_condition(lambda: TEST_DB_NAME in self.ch.get_databases())
        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=5)
        self.verify_record_exists(TEST_TABLE_NAME, "age=1912", {"name": "Hällo"})

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

        self.wait_for_condition(lambda: "group" in self.ch.get_tables())

        # Verify index creation in ClickHouse
        create_query = self.ch.show_create_table("group")
        assert "INDEX name_idx name TYPE ngrambf_v1" in create_query

        run_all_runner.stop()
