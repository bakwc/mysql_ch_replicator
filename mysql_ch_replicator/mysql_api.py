from contextlib import contextmanager
from logging import getLogger

from .config import MysqlSettings
from .connection_pool import PooledConnection, get_pool_manager

logger = getLogger(__name__)


class MySQLApi:
    def __init__(self, database: str, mysql_settings: MysqlSettings):
        self.database = database
        self.mysql_settings = mysql_settings
        self.pool_manager = get_pool_manager()
        self.connection_pool = self.pool_manager.get_or_create_pool(
            mysql_settings=mysql_settings,
            pool_name=mysql_settings.pool_name,
            pool_size=mysql_settings.pool_size,
            max_overflow=mysql_settings.max_overflow,
        )
        logger.info(
            f"MySQLApi initialized with database '{database}' using connection pool '{mysql_settings.pool_name}'"
        )

    @contextmanager
    def get_connection(self):
        """Get a connection from the pool with automatic cleanup"""
        with PooledConnection(self.connection_pool) as (connection, cursor):
            # Set database if specified
            if self.database is not None:
                cursor.execute(f"USE `{self.database}`")
            yield connection, cursor

    def close(self):
        """Close method for compatibility - pool handles connection lifecycle"""
        logger.debug("MySQLApi.close() called - connection pool will handle cleanup")

    def execute(self, command, commit=False, args=None):
        with self.get_connection() as (connection, cursor):
            if args:
                cursor.execute(command, args)
            else:
                cursor.execute(command)
            if commit:
                connection.commit()

    def set_database(self, database):
        self.database = database

    def get_databases(self):
        with self.get_connection() as (connection, cursor):
            # Use connection without specific database for listing databases
            cursor.execute("USE INFORMATION_SCHEMA")  # Ensure we can list all databases
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
            query_params = []

            if start_value is not None:
                # Build the start_value condition for pagination using parameterized query
                # This prevents SQL injection and handles special characters properly

                # 🐛 FIX: For single-column PKs, use simple comparison, not tuple syntax
                # Tuple comparison `WHERE (col) > (val)` can cause infinite loops with string PKs
                if len(start_value) == 1:
                    # Single column: WHERE `col` > %s
                    where = f"WHERE {order_by_str} > %s "
                    query_params.append(start_value[0])
                else:
                    # Multiple columns: WHERE (col1, col2) > (%s, %s)
                    placeholders = ",".join(["%s"] * len(start_value))
                    where = f"WHERE ({order_by_str}) > ({placeholders}) "
                    query_params.extend(start_value)

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

            # 🔍 PHASE 2.1: Enhanced query logging for worker investigation
            logger.info(f"🔎 SQL QUERY: table='{table_name}', worker={worker_id}/{total_workers}, query='{query}'")
            if query_params:
                logger.info(f"🔎 SQL PARAMS: table='{table_name}', worker={worker_id}, params={query_params}")

            # Log query details for debugging
            logger.debug(f"Executing query: {query}")
            if query_params:
                logger.debug(f"Query parameters: {query_params}")

            # Execute the query with proper parameterization
            try:
                if query_params:
                    cursor.execute(query, tuple(query_params))
                else:
                    cursor.execute(query)
                res = cursor.fetchall()
                records = [x for x in res]
                return records
            except Exception as e:
                logger.error(f"Query execution failed: {query}")
                if query_params:
                    logger.error(f"Query parameters: {query_params}")
                logger.error(f"Error details: {e}")
                raise
