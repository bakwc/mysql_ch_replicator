"""End-to-end integration test scenarios"""

import pytest

from tests.base import BaseReplicationTest, DataTestMixin, SchemaTestMixin
from tests.conftest import TEST_DB_NAME, TEST_TABLE_NAME, BinlogReplicatorRunner, DbReplicatorRunner


class TestE2EScenarios(BaseReplicationTest, SchemaTestMixin, DataTestMixin):
    """End-to-end test scenarios covering complete replication workflows"""

    @pytest.mark.integration
    def test_e2e_regular_replication(self):
        """Test regular end-to-end replication with binlog and db replicators"""
        # Create test table with various fields and comments
        self.mysql.execute(f"""
        CREATE TABLE `{TEST_TABLE_NAME}` (
            id int NOT NULL AUTO_INCREMENT,
            name varchar(255) COMMENT 'Dân tộc, ví dụ: Kinh',
            age int COMMENT 'CMND Cũ',
            field1 text,
            field2 blob,
            PRIMARY KEY (id)
        ); 
        """)

        # Insert initial test data
        self.mysql.execute(
            f"INSERT INTO `{TEST_TABLE_NAME}` (name, age, field1, field2) VALUES ('Ivan', 42, 'test1', 'test2');",
            commit=True,
        )
        self.mysql.execute(
            f"INSERT INTO `{TEST_TABLE_NAME}` (name, age) VALUES ('Peter', 33);",
            commit=True,
        )

        # Start replication
        self.start_replication()

        # Verify database and table creation
        self.wait_for_database()
        self.ch.execute_command(f"USE `{TEST_DB_NAME}`")
        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=2)

        # Verify data replication
        self.verify_record_exists(TEST_TABLE_NAME, "name='Ivan'", {"age": 42, "field1": "test1"})
        self.verify_record_exists(TEST_TABLE_NAME, "name='Peter'", {"age": 33})

        # Test real-time updates
        self.mysql.execute(
            f"INSERT INTO `{TEST_TABLE_NAME}` (name, age, field1) VALUES ('Maria', 28, 'test3');",
            commit=True,
        )
        
        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=3)
        self.verify_record_exists(TEST_TABLE_NAME, "name='Maria'", {"age": 28, "field1": "test3"})

        # Test updates
        self.mysql.execute(
            f"UPDATE `{TEST_TABLE_NAME}` SET age = 29 WHERE name = 'Maria';",
            commit=True,
        )
        
        self.wait_for_record_update(TEST_TABLE_NAME, "name='Maria'", {"age": 29})

        # Test deletes
        self.mysql.execute(
            f"DELETE FROM `{TEST_TABLE_NAME}` WHERE name = 'Peter';",
            commit=True,
        )
        
        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=2)

    @pytest.mark.integration
    def test_e2e_multistatement_transactions(self):
        """Test multi-statement transactions in end-to-end replication"""
        # Create test table
        self.create_basic_table(TEST_TABLE_NAME)

        # Start replication
        self.start_replication()
        
        # Wait for table to be created
        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=0)

        # Execute multi-statement transaction
        self.mysql.execute("BEGIN;")
        self.mysql.execute(
            f"INSERT INTO `{TEST_TABLE_NAME}` (name, age) VALUES ('John', 25);"
        )
        self.mysql.execute(
            f"INSERT INTO `{TEST_TABLE_NAME}` (name, age) VALUES ('Jane', 30);"
        )
        self.mysql.execute(
            f"UPDATE `{TEST_TABLE_NAME}` SET age = 26 WHERE name = 'John';"
        )
        self.mysql.execute("COMMIT;", commit=True)

        # Verify all changes replicated correctly
        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=2)
        self.verify_record_exists(TEST_TABLE_NAME, "name='John'", {"age": 26})
        self.verify_record_exists(TEST_TABLE_NAME, "name='Jane'", {"age": 30})

        # Test rollback scenario
        self.mysql.execute("BEGIN;")
        self.mysql.execute(
            f"INSERT INTO `{TEST_TABLE_NAME}` (name, age) VALUES ('Bob', 35);"
        )
        self.mysql.execute(
            f"UPDATE `{TEST_TABLE_NAME}` SET age = 27 WHERE name = 'John';"
        )
        self.mysql.execute("ROLLBACK;", commit=True)

        # Verify rollback - should still have original data
        self.wait_for_stable_state(TEST_TABLE_NAME, expected_count=2, wait_time=5)
        self.verify_record_exists(TEST_TABLE_NAME, "name='John'", {"age": 26})
        self.verify_record_does_not_exist(TEST_TABLE_NAME, "name='Bob'")

    @pytest.mark.integration
    def test_runner_integration(self):
        """Test runner integration and process management"""
        # Create multiple tables for comprehensive testing
        tables = [f"{TEST_TABLE_NAME}_1", f"{TEST_TABLE_NAME}_2", f"{TEST_TABLE_NAME}_3"]
        
        for table in tables:
            self.create_basic_table(table)
            self.insert_multiple_records(
                table, [{"name": f"User_{table}", "age": 25 + len(table)}]
            )

        # Start replication with runner
        self.start_replication()

        # Verify all tables replicated
        for table in tables:
            self.wait_for_table_sync(table, expected_count=1)
            self.verify_record_exists(table, f"name='User_{table}'")

        # Test concurrent operations across tables
        for i, table in enumerate(tables):
            self.mysql.execute(
                f"INSERT INTO `{table}` (name, age) VALUES ('Concurrent_{i}', {30 + i});",
                commit=True,
            )

        # Verify concurrent operations
        for i, table in enumerate(tables):
            self.wait_for_table_sync(table, expected_count=2)
            self.verify_record_exists(table, f"name='Concurrent_{i}'", {"age": 30 + i})