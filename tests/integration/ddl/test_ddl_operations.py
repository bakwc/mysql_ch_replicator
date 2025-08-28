"""Tests for DDL (Data Definition Language) operations during replication"""

import pytest

from tests.base import BaseReplicationTest, DataTestMixin, SchemaTestMixin
from tests.conftest import TEST_TABLE_NAME
from tests.fixtures import TableSchemas, TestDataGenerator


class TestDdlOperations(BaseReplicationTest, SchemaTestMixin, DataTestMixin):
    """Test DDL operations like ALTER TABLE, CREATE TABLE, etc."""

    @pytest.mark.integration
    def test_add_column_operations(self):
        """Test adding columns to existing table"""
        # Setup initial table
        schema = TableSchemas.basic_user_table(TEST_TABLE_NAME)
        self.mysql.execute(schema.sql)

        initial_data = TestDataGenerator.basic_users()[:2]
        self.insert_multiple_records(TEST_TABLE_NAME, initial_data)

        # Start replication
        self.start_replication()
        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=2)

        # Add columns with different types
        self.add_column(TEST_TABLE_NAME, "last_name varchar(255)")
        self.add_column(TEST_TABLE_NAME, "price decimal(10,2) DEFAULT NULL")

        # Insert data with new columns
        self.mysql.execute(
            f"INSERT INTO `{TEST_TABLE_NAME}` (name, age, last_name, price) VALUES ('Mary', 24, 'Smith', 3.2);",
            commit=True,
        )

        # Verify schema and data changes
        self.wait_for_data_sync(TEST_TABLE_NAME, "name='Mary'", "Smith", "last_name")
        self.wait_for_data_sync(TEST_TABLE_NAME, "name='Mary'", 3.2, "price")

    @pytest.mark.integration
    def test_add_column_with_position(self):
        """Test adding columns with FIRST and AFTER clauses"""
        # Setup
        schema = TableSchemas.basic_user_table(TEST_TABLE_NAME)
        self.mysql.execute(schema.sql)

        self.insert_basic_record(TEST_TABLE_NAME, "TestUser", 42)

        # Start replication
        self.start_replication()
        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=1)

        # Add column FIRST
        self.add_column(TEST_TABLE_NAME, "c1 INT", "FIRST")
        self.mysql.execute(
            f"INSERT INTO `{TEST_TABLE_NAME}` (id, c1, name, age) VALUES (43, 11, 'User2', 25);",
            commit=True,
        )

        # Add column AFTER
        self.add_column(TEST_TABLE_NAME, "c2 INT", "AFTER c1")
        self.mysql.execute(
            f"INSERT INTO `{TEST_TABLE_NAME}` (id, c1, c2, name, age) VALUES (44, 111, 222, 'User3', 30);",
            commit=True,
        )

        # Verify data
        self.wait_for_data_sync(TEST_TABLE_NAME, "id=43", 11, "c1")
        self.wait_for_data_sync(TEST_TABLE_NAME, "id=44", 111, "c1")
        self.wait_for_data_sync(TEST_TABLE_NAME, "id=44", 222, "c2")

    @pytest.mark.integration
    def test_drop_column_operations(self):
        """Test dropping columns from table"""
        # Setup with extra columns
        self.mysql.execute(f"""
        CREATE TABLE `{TEST_TABLE_NAME}` (
            id int NOT NULL AUTO_INCREMENT,
            name varchar(255),
            age int,
            temp_field varchar(100),
            PRIMARY KEY (id)
        );
        """)

        self.insert_basic_record(
            TEST_TABLE_NAME, "TestUser", 42, temp_field="temporary"
        )

        # Start replication
        self.start_replication()
        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=1)

        # Drop column
        self.drop_column(TEST_TABLE_NAME, "temp_field")

        # Insert new data without the dropped column
        self.insert_basic_record(TEST_TABLE_NAME, "User2", 25)

        # Verify column is gone and data still works
        self.wait_for_data_sync(TEST_TABLE_NAME, "name='User2'", 25, "age")

    @pytest.mark.integration
    def test_modify_column_operations(self):
        """Test modifying existing columns"""
        # Setup
        schema = TableSchemas.basic_user_table(TEST_TABLE_NAME)
        self.mysql.execute(schema.sql)

        # Add a column that we'll modify
        self.add_column(TEST_TABLE_NAME, "last_name varchar(255)")

        self.mysql.execute(
            f"INSERT INTO `{TEST_TABLE_NAME}` (name, age, last_name) VALUES ('Test', 25, '');",
            commit=True,
        )

        # Start replication
        self.start_replication()
        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=1)

        # Update the existing record to have empty string (not NULL)
        self.mysql.execute(
            f"UPDATE `{TEST_TABLE_NAME}` SET last_name = '' WHERE last_name IS NULL;",
            commit=True,
        )

        # Modify column to be NOT NULL
        self.modify_column(TEST_TABLE_NAME, "last_name varchar(1024) NOT NULL")

        # Insert data with the modified column
        self.mysql.execute(
            f"INSERT INTO `{TEST_TABLE_NAME}` (name, age, last_name) VALUES ('User2', 30, 'ValidName');",
            commit=True,
        )

        # Verify the change works
        self.wait_for_data_sync(
            TEST_TABLE_NAME, "name='User2'", "ValidName", "last_name"
        )

    @pytest.mark.integration
    def test_index_operations(self):
        """Test adding and dropping indexes"""
        # Setup
        schema = TableSchemas.basic_user_table(TEST_TABLE_NAME)
        self.mysql.execute(schema.sql)

        self.add_column(TEST_TABLE_NAME, "price decimal(10,2)")
        self.insert_basic_record(TEST_TABLE_NAME, "TestUser", 42, price=10.50)

        # Start replication
        self.start_replication()
        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=1)

        # Add index
        self.add_index(TEST_TABLE_NAME, "price_idx", "price", "UNIQUE")

        # Drop and recreate index with different name
        self.drop_index(TEST_TABLE_NAME, "price_idx")
        self.add_index(TEST_TABLE_NAME, "age_idx", "age", "UNIQUE")

        # Insert more data to verify indexes work
        self.insert_basic_record(TEST_TABLE_NAME, "User2", 25, price=15.75)

        # Verify data is still replicated correctly
        self.wait_for_data_sync(TEST_TABLE_NAME, "name='User2'", 25, "age")

    @pytest.mark.integration
    def test_create_table_during_replication(self):
        """Test creating new tables while replication is running"""
        # Setup initial table
        schema = TableSchemas.basic_user_table(TEST_TABLE_NAME)
        self.mysql.execute(schema.sql)

        self.insert_basic_record(TEST_TABLE_NAME, "InitialUser", 30)

        # Start replication
        self.start_replication()
        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=1)

        # Create new table during replication
        new_table = "test_table_2"
        new_schema = TableSchemas.basic_user_table(new_table)
        self.mysql.execute(new_schema.sql)

        # Insert data into new table
        self.insert_basic_record(new_table, "NewTableUser", 35)

        # Verify new table is replicated
        self.wait_for_table_sync(new_table, expected_count=1)
        self.wait_for_data_sync(new_table, "name='NewTableUser'", 35, "age")

    @pytest.mark.integration
    def test_drop_table_operations(self):
        """Test dropping tables during replication"""
        # Create two tables
        schema1 = TableSchemas.basic_user_table(TEST_TABLE_NAME)
        schema2 = TableSchemas.basic_user_table("temp_table")

        self.mysql.execute(schema1.sql)
        self.mysql.execute(schema2.sql)

        self.insert_basic_record(TEST_TABLE_NAME, "User1", 25)
        self.insert_basic_record("temp_table", "TempUser", 30)

        # Start replication
        self.start_replication()
        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=1)
        self.wait_for_table_sync("temp_table", expected_count=1)

        # Drop the temporary table
        self.drop_table("temp_table")

        # Verify main table still works
        self.insert_basic_record(TEST_TABLE_NAME, "User2", 35)
        self.wait_for_data_sync(TEST_TABLE_NAME, "name='User2'", 35, "age")

    @pytest.mark.integration
    def test_rename_table_operations(self):
        """Test renaming tables during replication"""
        # Setup
        schema = TableSchemas.basic_user_table(TEST_TABLE_NAME)
        self.mysql.execute(schema.sql)

        self.insert_basic_record(TEST_TABLE_NAME, "OriginalUser", 40)

        # Start replication
        self.start_replication()
        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=1)

        # Rename table
        new_name = "renamed_table"
        self.rename_table(TEST_TABLE_NAME, new_name)

        # Insert data into renamed table
        self.insert_basic_record(new_name, "RenamedUser", 45)

        # Verify renamed table works
        self.wait_for_table_sync(new_name, expected_count=2)
        self.wait_for_data_sync(new_name, "name='RenamedUser'", 45, "age")

    @pytest.mark.integration
    def test_truncate_table_operations(self):
        """Test truncating tables during replication"""
        # Setup
        schema = TableSchemas.basic_user_table(TEST_TABLE_NAME)
        self.mysql.execute(schema.sql)

        initial_data = TestDataGenerator.basic_users()[:3]
        self.insert_multiple_records(TEST_TABLE_NAME, initial_data)

        # Start replication
        self.start_replication()
        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=3)

        # Truncate table
        self.truncate_table(TEST_TABLE_NAME)

        # Verify table is empty in ClickHouse
        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=0)

        # Insert new data after truncate
        self.insert_basic_record(TEST_TABLE_NAME, "PostTruncateUser", 50)

        # Verify new data is replicated
        self.wait_for_data_sync(TEST_TABLE_NAME, "name='PostTruncateUser'", 50, "age")
