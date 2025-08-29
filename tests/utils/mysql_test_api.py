from contextlib import contextmanager
from logging import getLogger

import mysql.connector

from mysql_ch_replicator.config import MysqlSettings

logger = getLogger(__name__)


class MySQLTestApi:
    """
    MySQL API specifically designed for testing scenarios.

    This class uses direct connections (no connection pooling) and is optimized
    for test scenarios where we need:
    - Persistent connection state for commands like SET sql_mode
    - Simple connection management without pooling complexity
    - Proper cleanup for test isolation
    """

    def __init__(self, database: str, mysql_settings: MysqlSettings):
        self.database = database
        self.mysql_settings = mysql_settings
        logger.info(
            f"MySQLTestApi initialized with database '{database}' using direct connections"
        )

    @contextmanager
    def get_connection(self):
        """Get a direct MySQL connection with automatic cleanup"""
        # Use standardized connection configuration
        config = self.mysql_settings.get_connection_config(
            database=self.database, autocommit=False
        )
        connection = mysql.connector.connect(**config)
        try:
            cursor = connection.cursor()
            try:
                yield connection, cursor
            finally:
                # Properly handle any unread results before closing
                try:
                    cursor.fetchall()  # Consume any remaining results
                except Exception:
                    pass  # Ignore if there are no results to consume
                finally:
                    cursor.close()
        finally:
            connection.close()

    def close(self):
        """Close method for compatibility - direct connections are auto-closed"""
        logger.debug("MySQLTestApi.close() called - direct connections are auto-closed")

    def execute(self, command, commit=False, args=None):
        """Execute a SQL command with optional commit"""
        with self.get_connection() as (connection, cursor):
            if args:
                cursor.execute(command, args)
            else:
                cursor.execute(command)

            # Consume any results to avoid "Unread result found" errors
            try:
                cursor.fetchall()
            except Exception:
                pass  # Ignore if there are no results to fetch

            if commit:
                connection.commit()

    def execute_batch(self, commands, commit=False):
        """Execute multiple SQL commands in the same connection context"""
        with self.get_connection() as (connection, cursor):
            for command in commands:
                if isinstance(command, tuple):
                    # Command with args
                    cmd, args = command
                    cursor.execute(cmd, args)
                else:
                    # Simple command
                    cursor.execute(command)

                # Consume any results to avoid "Unread result found" errors
                try:
                    cursor.fetchall()
                except Exception:
                    pass  # Ignore if there are no results to fetch

            if commit:
                connection.commit()

    def set_database(self, database):
        self.database = database

    def get_databases(self):
        with self.get_connection() as (connection, cursor):
            cursor.execute("SHOW DATABASES")
            res = cursor.fetchall()
            databases = [x[0] for x in res]
            return databases

    def get_tables(self):
        with self.get_connection() as (connection, cursor):
            cursor.execute("SHOW FULL TABLES")
            res = cursor.fetchall()
            tables = [x[0] for x in res if x[1] == "BASE TABLE"]
            return tables

    def get_binlog_files(self):
        with self.get_connection() as (connection, cursor):
            cursor.execute("SHOW BINARY LOGS")
            res = cursor.fetchall()
            binlog_files = [x[0] for x in res]
            return binlog_files

    def get_table_create_statement(self, table_name) -> str:
        with self.get_connection() as (connection, cursor):
            cursor.execute(f"SHOW CREATE TABLE `{table_name}`")
            res = cursor.fetchall()
            create_statement = res[0][1].strip()
            return create_statement

    def get_records(
        self,
        table_name,
        order_by,
        limit,
        start_value=None,
        worker_id=None,
        total_workers=None,
    ):
        with self.get_connection() as (connection, cursor):
            # Escape column names with backticks to avoid issues with reserved keywords like "key"
            order_by_escaped = [f"`{col}`" for col in order_by]
            order_by_str = ",".join(order_by_escaped)

            where = ""
            if start_value is not None:
                # Build the start_value condition for pagination
                start_value_str = ",".join(map(str, start_value))
                where = f"WHERE ({order_by_str}) > ({start_value_str}) "

            # Add partitioning filter for parallel processing (e.g., sharded crawling)
            if (
                worker_id is not None
                and total_workers is not None
                and total_workers > 1
            ):
                # Escape column names in COALESCE expressions
                coalesce_expressions = [f"COALESCE(`{key}`, '')" for key in order_by]
                concat_keys = f"CONCAT_WS('|', {', '.join(coalesce_expressions)})"
                hash_condition = f"CRC32({concat_keys}) % {total_workers} = {worker_id}"

                if where:
                    where += f"AND {hash_condition} "
                else:
                    where = f"WHERE {hash_condition} "

            # Construct final query
            query = f"SELECT * FROM `{table_name}` {where}ORDER BY {order_by_str} LIMIT {limit}"

            logger.debug(f"Executing query: {query}")

            # Execute the query
            cursor.execute(query)
            res = cursor.fetchall()
            records = [x for x in res]
            return records

    def fetch_all(self, query):
        """Execute a SELECT query and return all results"""
        with self.get_connection() as (connection, cursor):
            cursor.execute(query)
            res = cursor.fetchall()
            return res
    
    def fetch_one(self, query):
        """Execute a SELECT query and return one result"""
        with self.get_connection() as (connection, cursor):
            cursor.execute(query)
            res = cursor.fetchone()
            return res
