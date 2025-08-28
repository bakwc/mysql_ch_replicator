"""Advanced DDL operations tests including column modifications and conditional statements"""

import pytest

from tests.base import BaseReplicationTest, DataTestMixin, SchemaTestMixin
from tests.conftest import TEST_TABLE_NAME


class TestAdvancedDdlOperations(BaseReplicationTest, SchemaTestMixin, DataTestMixin):
    """Test advanced DDL operations during replication"""

    @pytest.mark.integration
    def test_add_column_first_after_and_drop_column(self):
        """Test ADD COLUMN FIRST/AFTER and DROP COLUMN operations"""
        # Create initial table
        self.mysql.execute(f"""
        CREATE TABLE `{TEST_TABLE_NAME}` (
            id int NOT NULL AUTO_INCREMENT,
            name varchar(255),
            age int,
            PRIMARY KEY (id)
        );
        """)

        # Insert initial data
        self.insert_multiple_records(
            TEST_TABLE_NAME,
            [
                {"name": "John", "age": 30},
                {"name": "Jane", "age": 25},
            ]
        )

        # Start replication
        self.start_replication()
        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=2)

        # Test ADD COLUMN FIRST
        self.mysql.execute(
            f"ALTER TABLE `{TEST_TABLE_NAME}` ADD COLUMN priority int DEFAULT 1 FIRST;",
            commit=True,
        )

        # Test ADD COLUMN AFTER
        self.mysql.execute(
            f"ALTER TABLE `{TEST_TABLE_NAME}` ADD COLUMN email varchar(255) AFTER name;",
            commit=True,
        )

        # Test ADD COLUMN at end (no position specified)
        self.mysql.execute(
            f"ALTER TABLE `{TEST_TABLE_NAME}` ADD COLUMN status varchar(50) DEFAULT 'active';",
            commit=True,
        )

        # Wait for DDL to replicate
        self.wait_for_ddl_replication()

        # Insert new data to test new columns
        self.mysql.execute(
            f"INSERT INTO `{TEST_TABLE_NAME}` (priority, name, email, age, status) VALUES (2, 'Bob', 'bob@example.com', 35, 'inactive');",
            commit=True,
        )

        # Update existing records with new columns
        self.mysql.execute(
            f"UPDATE `{TEST_TABLE_NAME}` SET email = 'john@example.com', priority = 3 WHERE name = 'John';",
            commit=True,
        )

        # Verify new data structure
        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=3)
        self.verify_record_exists(
            TEST_TABLE_NAME,
            "name='Bob'",
            {"priority": 2, "email": "bob@example.com", "status": "inactive"}
        )
        self.verify_record_exists(
            TEST_TABLE_NAME,
            "name='John'",
            {"priority": 3, "email": "john@example.com"}
        )

        # Test DROP COLUMN
        self.mysql.execute(
            f"ALTER TABLE `{TEST_TABLE_NAME}` DROP COLUMN priority;",
            commit=True,
        )

        # Wait for DROP to replicate
        self.wait_for_ddl_replication()

        # Insert data without the dropped column
        self.mysql.execute(
            f"INSERT INTO `{TEST_TABLE_NAME}` (name, email, age, status) VALUES ('Alice', 'alice@example.com', 28, 'active');",
            commit=True,
        )

        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=4)
        self.verify_record_exists(
            TEST_TABLE_NAME,
            "name='Alice'",
            {"email": "alice@example.com", "age": 28}
        )

    @pytest.mark.integration
    def test_if_exists_if_not_exists_ddl(self):
        """Test IF EXISTS and IF NOT EXISTS DDL statements"""
        # Test CREATE TABLE IF NOT EXISTS
        self.mysql.execute(f"""
        CREATE TABLE IF NOT EXISTS `{TEST_TABLE_NAME}` (
            id int NOT NULL AUTO_INCREMENT,
            name varchar(255),
            email varchar(255),
            PRIMARY KEY (id)
        );
        """)

        # Try to create the same table again (should not fail)
        self.mysql.execute(f"""
        CREATE TABLE IF NOT EXISTS `{TEST_TABLE_NAME}` (
            id int NOT NULL AUTO_INCREMENT,
            different_name varchar(255),
            different_email varchar(255),
            PRIMARY KEY (id)
        );
        """)

        # Insert test data
        self.insert_multiple_records(
            TEST_TABLE_NAME,
            [
                {"name": "Test1", "email": "test1@example.com"},
                {"name": "Test2", "email": "test2@example.com"},
            ]
        )

        # Start replication
        self.start_replication()
        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=2)

        # Test ADD COLUMN IF NOT EXISTS (should work)
        self.mysql.execute(
            f"ALTER TABLE `{TEST_TABLE_NAME}` ADD COLUMN IF NOT EXISTS age int DEFAULT 0;",
            commit=True,
        )

        # Try to add the same column again (should not fail)
        self.mysql.execute(
            f"ALTER TABLE `{TEST_TABLE_NAME}` ADD COLUMN IF NOT EXISTS age int DEFAULT 0;",
            commit=True,
        )

        self.wait_for_ddl_replication()

        # Update with new column
        self.mysql.execute(
            f"UPDATE `{TEST_TABLE_NAME}` SET age = 30 WHERE name = 'Test1';",
            commit=True,
        )

        self.wait_for_record_update(TEST_TABLE_NAME, "name='Test1'", {"age": 30})

        # Test DROP COLUMN IF EXISTS (should work)
        self.mysql.execute(
            f"ALTER TABLE `{TEST_TABLE_NAME}` DROP COLUMN IF EXISTS age;",
            commit=True,
        )

        # Try to drop the same column again (should not fail)
        self.mysql.execute(
            f"ALTER TABLE `{TEST_TABLE_NAME}` DROP COLUMN IF EXISTS age;",
            commit=True,
        )

        self.wait_for_ddl_replication()

        # Test CREATE INDEX IF NOT EXISTS
        self.mysql.execute(
            f"CREATE INDEX IF NOT EXISTS idx_{TEST_TABLE_NAME}_email ON `{TEST_TABLE_NAME}` (email);",
            commit=True,
        )

        # Try to create the same index again (should not fail)
        self.mysql.execute(
            f"CREATE INDEX IF NOT EXISTS idx_{TEST_TABLE_NAME}_email ON `{TEST_TABLE_NAME}` (email);",
            commit=True,
        )

        # Test DROP INDEX IF EXISTS
        self.mysql.execute(
            f"DROP INDEX IF EXISTS idx_{TEST_TABLE_NAME}_email ON `{TEST_TABLE_NAME}`;",
            commit=True,
        )

        # Try to drop the same index again (should not fail)
        self.mysql.execute(
            f"DROP INDEX IF EXISTS idx_{TEST_TABLE_NAME}_email ON `{TEST_TABLE_NAME}`;",
            commit=True,
        )

        # Final verification
        self.wait_for_stable_state(TEST_TABLE_NAME, expected_count=2)

    @pytest.mark.integration
    def test_percona_migration_scenarios(self):
        """Test Percona-specific migration scenarios"""
        # Create Percona-style table with specific features
        self.mysql.execute(f"""
        CREATE TABLE `{TEST_TABLE_NAME}` (
            id int NOT NULL AUTO_INCREMENT,
            name varchar(255),
            data longtext,
            created_at timestamp DEFAULT CURRENT_TIMESTAMP,
            updated_at timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            PRIMARY KEY (id),
            KEY idx_name (name),
            KEY idx_created (created_at)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
        """)

        # Insert test data with various character encodings
        percona_data = [
            {
                "name": "ASCII Test",
                "data": "Simple ASCII data",
            },
            {
                "name": "UTF8 Test",
                "data": "UTF-8 Data: 中文测试 العربية русский язык 🎉 αβγδ",
            },
            {
                "name": "Large Text Test",
                "data": "Large data content " * 1000,  # Create large text
            },
            {
                "name": "JSON-like Text",
                "data": '{"complex": {"nested": {"data": ["array", "values", 123, true]}}}',
            },
        ]

        self.insert_multiple_records(TEST_TABLE_NAME, percona_data)

        # Start replication
        self.start_replication()
        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=4)

        # Verify character encoding preservation
        self.verify_record_exists(TEST_TABLE_NAME, "name='UTF8 Test'")
        self.verify_record_exists(TEST_TABLE_NAME, "name='Large Text Test'")

        # Test Percona-specific operations
        # Online DDL operations (common in Percona)
        self.mysql.execute(
            f"ALTER TABLE `{TEST_TABLE_NAME}` ADD COLUMN status enum('active','inactive','pending') DEFAULT 'active';",
            commit=True,
        )

        self.wait_for_ddl_replication()

        # Test ENUM updates
        self.mysql.execute(
            f"UPDATE `{TEST_TABLE_NAME}` SET status = 'inactive' WHERE name = 'Large Text Test';",
            commit=True,
        )

        self.wait_for_record_update(
            TEST_TABLE_NAME, 
            "name='Large Text Test'", 
            {"status": "inactive"}
        )

        # Test table charset modifications
        self.mysql.execute(
            f"ALTER TABLE `{TEST_TABLE_NAME}` CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;",
            commit=True,
        )

        self.wait_for_ddl_replication()

        # Insert more data after charset change
        self.mysql.execute(
            f"INSERT INTO `{TEST_TABLE_NAME}` (name, data, status) VALUES ('Post Charset', 'Data after charset change', 'pending');",
            commit=True,
        )

        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=5)
        self.verify_record_exists(
            TEST_TABLE_NAME,
            "name='Post Charset'",
            {"status": "pending"}
        )