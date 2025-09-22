"""Tests for conditional DDL operations (IF EXISTS, IF NOT EXISTS, duplicate handling)"""

import pytest

from tests.base import IsolatedBaseReplicationTest, DataTestMixin, SchemaTestMixin
from tests.conftest import TEST_TABLE_NAME


class TestConditionalDdlOperations(IsolatedBaseReplicationTest, SchemaTestMixin, DataTestMixin):
    """Test conditional DDL operations and duplicate statement handling"""

    @pytest.mark.integration
    def test_conditional_ddl_operations(self):
        """Test conditional DDL statements and duplicate operation handling"""
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

        # Test ADD COLUMN (MySQL doesn't support IF NOT EXISTS for ALTER TABLE ADD COLUMN)
        self.mysql.execute(
            f"ALTER TABLE `{TEST_TABLE_NAME}` ADD COLUMN age int DEFAULT 0;",
            commit=True,
        )

        # Try to add the same column again (should fail, so we'll catch the exception)
        try:
            self.mysql.execute(
                f"ALTER TABLE `{TEST_TABLE_NAME}` ADD COLUMN age int DEFAULT 0;",
                commit=True,
            )
            # If we get here, the duplicate column addition didn't fail as expected
            pytest.fail("Expected duplicate column addition to fail, but it succeeded")
        except Exception:
            # Expected behavior - duplicate column should cause an error
            pass

        self.wait_for_ddl_replication()

        # Update with new column
        self.mysql.execute(
            f"UPDATE `{TEST_TABLE_NAME}` SET age = 30 WHERE name = 'Test1';",
            commit=True,
        )

        self.wait_for_record_update(TEST_TABLE_NAME, "name='Test1'", {"age": 30})

        # Test DROP COLUMN (MySQL doesn't support IF EXISTS for ALTER TABLE DROP COLUMN)
        self.mysql.execute(
            f"ALTER TABLE `{TEST_TABLE_NAME}` DROP COLUMN age;",
            commit=True,
        )

        # Try to drop the same column again (should fail, so we'll catch the exception)
        try:
            self.mysql.execute(
                f"ALTER TABLE `{TEST_TABLE_NAME}` DROP COLUMN age;",
                commit=True,
            )
            # If we get here, the duplicate column drop didn't fail as expected
            pytest.fail("Expected duplicate column drop to fail, but it succeeded")
        except Exception:
            # Expected behavior - dropping non-existent column should cause an error
            pass

        self.wait_for_ddl_replication()

        # Test CREATE INDEX
        self.mysql.execute(
            f"CREATE INDEX idx_{TEST_TABLE_NAME}_email ON `{TEST_TABLE_NAME}` (email);",
            commit=True,
        )

        # Try to create the same index again (should fail, so we'll catch the exception)
        try:
            self.mysql.execute(
                f"CREATE INDEX idx_{TEST_TABLE_NAME}_email ON `{TEST_TABLE_NAME}` (email);",
                commit=True,
            )
            # If we get here, the duplicate index creation didn't fail as expected
            pytest.fail("Expected duplicate index creation to fail, but it succeeded")
        except Exception:
            # Expected behavior - duplicate index should cause an error
            pass

        # Test DROP INDEX
        self.mysql.execute(
            f"DROP INDEX idx_{TEST_TABLE_NAME}_email ON `{TEST_TABLE_NAME}`;",
            commit=True,
        )

        # Try to drop the same index again (should fail, so we'll catch the exception)
        try:
            self.mysql.execute(
                f"DROP INDEX idx_{TEST_TABLE_NAME}_email ON `{TEST_TABLE_NAME}`;",
                commit=True,
            )
            # If we get here, the duplicate index drop didn't fail as expected
            pytest.fail("Expected duplicate index drop to fail, but it succeeded")
        except Exception:
            # Expected behavior - dropping non-existent index should cause an error
            pass

        # Final verification
        self.wait_for_stable_state(TEST_TABLE_NAME, expected_count=2)