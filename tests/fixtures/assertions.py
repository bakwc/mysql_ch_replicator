"""Reusable assertion helpers for tests"""

from tests.conftest import assert_wait


class AssertionHelpers:
    """Collection of reusable assertion methods"""

    def __init__(self, mysql_api, clickhouse_api):
        self.mysql = mysql_api
        self.ch = clickhouse_api

    def assert_table_exists(self, table_name, database=None):
        """Assert table exists in ClickHouse"""
        if database:
            self.ch.execute_command(f"USE `{database}`")
        assert_wait(lambda: table_name in self.ch.get_tables())

    def assert_table_count(self, table_name, expected_count, database=None):
        """Assert table has expected number of records"""
        if database:
            self.ch.execute_command(f"USE `{database}`")
        assert_wait(lambda: len(self.ch.select(table_name)) == expected_count)

    def assert_record_exists(self, table_name, where_clause, database=None):
        """Assert record exists matching condition"""
        if database:
            self.ch.execute_command(f"USE `{database}`")
        assert_wait(lambda: len(self.ch.select(table_name, where=where_clause)) > 0)

    def assert_field_value(
        self, table_name, where_clause, field, expected_value, database=None
    ):
        """Assert field has expected value"""
        if database:
            self.ch.execute_command(f"USE `{database}`")
        assert_wait(
            lambda: self.ch.select(table_name, where=where_clause)[0].get(field)
            == expected_value
        )

    def assert_field_not_null(self, table_name, where_clause, field, database=None):
        """Assert field is not null"""
        if database:
            self.ch.execute_command(f"USE `{database}`")
        assert_wait(
            lambda: self.ch.select(table_name, where=where_clause)[0].get(field)
            is not None
        )

    def assert_field_is_null(self, table_name, where_clause, field, database=None):
        """Assert field is null"""
        if database:
            self.ch.execute_command(f"USE `{database}`")
        assert_wait(
            lambda: self.ch.select(table_name, where=where_clause)[0].get(field) is None
        )

    def assert_column_exists(self, table_name, column_name, database=None):
        """Assert column exists in table schema"""
        if database:
            self.ch.execute_command(f"USE `{database}`")

        def column_exists():
            try:
                # Try to select the column - will fail if it doesn't exist
                self.ch.execute_query(
                    f"SELECT {column_name} FROM `{table_name}` LIMIT 1"
                )
                return True
            except:
                return False

        assert_wait(column_exists)

    def assert_column_not_exists(self, table_name, column_name, database=None):
        """Assert column does not exist in table schema"""
        if database:
            self.ch.execute_command(f"USE `{database}`")

        def column_not_exists():
            try:
                # Try to select the column - should fail if it doesn't exist
                self.ch.execute_query(
                    f"SELECT {column_name} FROM `{table_name}` LIMIT 1"
                )
                return False
            except:
                return True

        assert_wait(column_not_exists)

    def assert_database_exists(self, database_name):
        """Assert database exists"""
        assert_wait(lambda: database_name in self.ch.get_databases())

    def assert_counts_match(self, table_name, mysql_table=None, where_clause=""):
        """Assert MySQL and ClickHouse have same record count"""
        mysql_table = mysql_table or table_name
        where = f" WHERE {where_clause}" if where_clause else ""

        # Get MySQL count
        with self.mysql.get_connection() as (connection, cursor):
            cursor.execute(f"SELECT COUNT(*) FROM `{mysql_table}`{where}")
            mysql_count = cursor.fetchone()[0]

        # Get ClickHouse count
        def counts_match():
            result = self.ch.execute_query(
                f"SELECT COUNT(*) FROM `{table_name}`{where}"
            )
            ch_count = result[0][0] if result else 0
            return mysql_count == ch_count

        assert_wait(counts_match)

    def assert_partition_clause(self, table_name, expected_partition, database=None):
        """Assert table has expected partition clause"""
        if database:
            self.ch.execute_command(f"USE `{database}`")

        def has_partition():
            create_query = self.ch.show_create_table(table_name)
            return expected_partition in create_query

        assert_wait(has_partition)
