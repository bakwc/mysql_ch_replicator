import datetime
import time
import clickhouse_connect

from logging import getLogger

from .config import ClickhouseSettings
from .table_structure import TableStructure, TableField


logger = getLogger(__name__)


CREATE_TABLE_QUERY = '''
CREATE TABLE {db_name}.{table_name}
(
{fields},
    `_version` UInt64,
    INDEX _version _version TYPE minmax GRANULARITY 1,
    INDEX idx_id {primary_key} TYPE bloom_filter GRANULARITY 1
)
ENGINE = ReplacingMergeTree(_version)
{partition_by}ORDER BY {primary_key}
SETTINGS index_granularity = 8192
'''

DELETE_QUERY = '''
DELETE FROM {db_name}.{table_name} WHERE {field_name} IN ({field_values})
'''


class ClickhouseApi:
    MAX_RETRIES = 5
    RETRY_INTERVAL = 30
    def __init__(self, database: str, clickhouse_settings: ClickhouseSettings):
        self.database = database
        self.clickhouse_settings = clickhouse_settings
        self.client = clickhouse_connect.get_client(
            host=clickhouse_settings.host,
            port=clickhouse_settings.port,
            username=clickhouse_settings.user,
            password=clickhouse_settings.password,
            connect_timeout=clickhouse_settings.connection_timeout,
            send_receive_timeout=clickhouse_settings.send_receive_timeout,
        )
        self.tables_last_record_version = {}  # table_name => last used row version
        self.execute_command('SET final = 1;')

    def get_tables(self):
        result = self.client.query('SHOW TABLES')
        tables = result.result_rows
        table_list = [row[0] for row in tables]
        return table_list

    def get_table_structure(self, table_name):
        return {}

    def get_databases(self):
        result = self.client.query('SHOW DATABASES')
        databases = result.result_rows
        database_list = [row[0] for row in databases]
        return database_list

    def execute_command(self, query):
        #print(' === executing ch query', query)

        for attempt in range(ClickhouseApi.MAX_RETRIES):
            try:
                self.client.command(query)
                break
            except clickhouse_connect.driver.exceptions.OperationalError as e:
                logger.error(f'error executing command {query}: {e}', exc_info=e)
                if attempt == ClickhouseApi.MAX_RETRIES - 1:
                    raise e
                time.sleep(ClickhouseApi.RETRY_INTERVAL)

    def recreate_database(self):
        #print(' === creating database', self.database)
        self.execute_command(f'DROP DATABASE IF EXISTS {self.database}')
        self.execute_command(f'CREATE DATABASE {self.database}')

    def get_last_used_version(self, table_name):
        return self.tables_last_record_version.get(table_name, 0)

    def set_last_used_version(self, table_name, last_used_version):
        self.tables_last_record_version[table_name] = last_used_version

    def create_table(self, structure: TableStructure):
        if not structure.primary_key:
            raise Exception(f'missing primary key for {structure.table_name}')

        primary_key_type = ''
        for field in structure.fields:
            if field.name == structure.primary_key:
                primary_key_type = field.field_type
        if not primary_key_type:
            raise Exception(f'failed to get type of primary key {structure.table_name} {structure.primary_key}')

        fields = [
            f'    `{field.name}` {field.field_type}' for field in structure.fields
        ]
        fields = ',\n'.join(fields)
        partition_by = ''

        if 'int' in primary_key_type.lower():
            partition_by = f'PARTITION BY intDiv({structure.primary_key}, 4294967)\n'

        query = CREATE_TABLE_QUERY.format(**{
            'db_name': self.database,
            'table_name': structure.table_name,
            'fields': fields,
            'primary_key': structure.primary_key,
            'partition_by': partition_by,
        })
        self.execute_command(query)

    def insert(self, table_name, records, table_structure: TableStructure = None):
        current_version = self.get_last_used_version(table_name) + 1

        records_to_insert = []
        for record in records:
            new_record = []
            for i, e in enumerate(record):
                if isinstance(e, datetime.datetime):
                    try:
                        e.timestamp()
                    except ValueError:
                        e = 0
                if table_structure is not None:
                    field: TableField = table_structure.fields[i]
                    is_datetime = (
                        ('DateTime' in field.field_type) or
                        ('Date32' in field.field_type)
                    )
                    if is_datetime and 'Nullable' not in field.field_type:
                        try:
                            e.timestamp()
                        except (ValueError, AttributeError):
                            e = datetime.datetime(1970, 1, 1)
                new_record.append(e)
            record = new_record

            records_to_insert.append(tuple(record) + (current_version,))
            current_version += 1

        full_table_name = table_name
        if '.' not in full_table_name:
            full_table_name = f'{self.database}.{table_name}'

        for attempt in range(ClickhouseApi.MAX_RETRIES):
            try:
                self.client.insert(table=full_table_name, data=records_to_insert)
                break
            except clickhouse_connect.driver.exceptions.OperationalError as e:
                logger.error(f'error inserting data: {e}', exc_info=e)
                if attempt == ClickhouseApi.MAX_RETRIES - 1:
                    raise e
                time.sleep(ClickhouseApi.RETRY_INTERVAL)

        self.set_last_used_version(table_name, current_version)

    def erase(self, table_name, field_name, field_values):
        field_values = ', '.join(list(map(str, field_values)))
        query = DELETE_QUERY.format(**{
            'db_name': self.database,
            'table_name': table_name,
            'field_name': field_name,
            'field_values': field_values,
        })
        self.execute_command(query)

    def drop_database(self, db_name):
        self.execute_command(f'DROP DATABASE IF EXISTS {db_name}')

    def create_database(self, db_name):
        self.cursor.execute(f'CREATE DATABASE {db_name}')

    def select(self, table_name, where=None):
        query = f'SELECT * FROM {table_name}'
        if where:
            query += f' WHERE {where}'
        result = self.client.query(query)
        rows = result.result_rows
        columns = result.column_names

        results = []
        for row in rows:
            results.append(dict(zip(columns, row)))
        return results

    def get_system_setting(self, name):
        results = self.select('system.settings', f"name = '{name}'")
        if not results:
            return None
        return results[0].get('value', None)
