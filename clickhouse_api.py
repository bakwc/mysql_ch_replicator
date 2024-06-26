import clickhouse_connect

from config import ClickhouseSettings
from table_structure import TableStructure, TableField


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
    def __init__(self, database: str, clickhouse_settings: ClickhouseSettings):
        self.database = database
        self.clickhouse_settings = clickhouse_settings
        self.client = clickhouse_connect.get_client(
            host=clickhouse_settings.host,
            port=clickhouse_settings.port,
            username=clickhouse_settings.user,
            password=clickhouse_settings.password,
        )
        self.tables_last_record_version = {}  # table_name => last used row version

    def get_tables(self):
        return []

    def get_table_structure(self, table_name):
        return {}

    def execute_command(self, query):
        self.client.command(query)

    def recreate_database(self):
        self.execute_command(f'DROP DATABASE IF EXISTS {self.database}')
        self.execute_command(f'CREATE DATABASE {self.database}')

    def get_last_used_version(self, table_name):
        return self.tables_last_record_version.get(table_name, 0)

    def set_last_used_version(self, table_name, last_used_version):
        self.tables_last_record_version[table_name] = last_used_version

    def create_table(self, table_name, structure: TableStructure):
        if not structure.primary_key:
            raise Exception(f'missing primary key for {table_name}')

        primary_key_type = ''
        for field in structure.fields:
            if field.name == structure.primary_key:
                primary_key_type = field.field_type
        if not primary_key_type:
            raise Exception(f'failed to get type of primary key {table_name} {structure.primary_key}')

        fields = [
            f'    `{field.name}` {field.field_type}' for field in structure.fields
        ]
        fields = ',\n'.join(fields)
        partition_by = ''

        if 'int' in primary_key_type.lower():
            partition_by = f'PARTITION BY intDiv({structure.primary_key}, 4294967)\n'

        query = CREATE_TABLE_QUERY.format(**{
            'db_name': self.database,
            'table_name': table_name,
            'fields': fields,
            'primary_key': structure.primary_key,
            'partition_by': partition_by,
        })
        self.execute_command(query)

    def insert(self, table_name, records):
        current_version = self.get_last_used_version(table_name) + 1

        records_to_insert = []
        for record in records:
            records_to_insert.append(tuple(record) + (current_version,))
            current_version += 1

        full_table_name = table_name
        if '.' not in full_table_name:
            full_table_name = f'{self.database}.{table_name}'

        self.client.insert(table=full_table_name, data=records_to_insert)

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
