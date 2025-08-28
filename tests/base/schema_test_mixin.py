"""Mixin for schema-related test operations"""


class SchemaTestMixin:
    """Mixin providing common schema operation methods"""

    def create_basic_table(self, table_name, additional_columns=""):
        """Create a basic test table with id, name, age"""
        columns = """
            id int NOT NULL AUTO_INCREMENT,
            name varchar(255),
            age int,
            PRIMARY KEY (id)
        """
        if additional_columns:
            columns += f",\n{additional_columns}"

        self.mysql.execute(f"""
        CREATE TABLE `{table_name}` (
            {columns}
        );
        """)

    def create_complex_table(self, table_name):
        """Create a complex table with various data types"""
        self.mysql.execute(f"""
        CREATE TABLE `{table_name}` (
            id int NOT NULL AUTO_INCREMENT,
            name varchar(255),
            age int,
            price decimal(10,2),
            created_date datetime,
            is_active boolean,
            data_blob blob,
            data_text text,
            coordinate point,
            PRIMARY KEY (id),
            INDEX idx_age (age),
            INDEX idx_price (price)
        );
        """)

    def add_column(self, table_name, column_definition, position=""):
        """Add a column to existing table"""
        self.mysql.execute(
            f"ALTER TABLE `{table_name}` ADD COLUMN {column_definition} {position}"
        )

    def drop_column(self, table_name, column_name):
        """Drop a column from table"""
        self.mysql.execute(f"ALTER TABLE `{table_name}` DROP COLUMN {column_name}")

    def modify_column(self, table_name, column_definition):
        """Modify existing column"""
        self.mysql.execute(f"ALTER TABLE `{table_name}` MODIFY {column_definition}")

    def add_index(self, table_name, index_name, columns, index_type=""):
        """Add index to table"""
        self.mysql.execute(
            f"ALTER TABLE `{table_name}` ADD {index_type} INDEX {index_name} ({columns})"
        )

    def drop_index(self, table_name, index_name):
        """Drop index from table"""
        self.mysql.execute(f"ALTER TABLE `{table_name}` DROP INDEX {index_name}")

    def create_table_like(self, new_table, source_table):
        """Create table using LIKE syntax"""
        self.mysql.execute(f"CREATE TABLE `{new_table}` LIKE `{source_table}`")

    def rename_table(self, old_name, new_name):
        """Rename table"""
        self.mysql.execute(f"RENAME TABLE `{old_name}` TO `{new_name}`")

    def truncate_table(self, table_name):
        """Truncate table"""
        self.mysql.execute(f"TRUNCATE TABLE `{table_name}`")

    def drop_table(self, table_name, if_exists=True):
        """Drop table"""
        if_exists_clause = "IF EXISTS" if if_exists else ""
        self.mysql.execute(f"DROP TABLE {if_exists_clause} `{table_name}`")

    def wait_for_ddl_replication(self, max_wait_time=10.0):
        """Wait for DDL operations to replicate to ClickHouse"""
        import time
        # DDL operations typically replicate quickly, so we use a shorter wait
        # This gives time for schema changes to propagate through the replication system
        time.sleep(2.0)

    def wait_for_database(self, database_name=None, max_wait_time=20.0):
        """Wait for database to be created in ClickHouse"""
        from tests.conftest import assert_wait, TEST_DB_NAME
        db_name = database_name or TEST_DB_NAME
        assert_wait(lambda: db_name in self.ch.get_databases(), max_wait_time=max_wait_time)
