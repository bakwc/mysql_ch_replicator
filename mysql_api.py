import time
import mysql.connector
from pyparsing import Word, alphas, alphanums

from config import MysqlSettings
from table_structure import TableStructure, TableField


class MySQLApi:
    RECONNECT_INTERVAL = 3 * 60

    def __init__(self, database: str | None, mysql_settings: MysqlSettings):
        self.database = database
        self.mysql_settings = mysql_settings
        self.last_connect_time = 0
        self.reconnect_if_required()

    def close(self):
        self.db.close()

    def reconnect_if_required(self):
        curr_time = time.time()
        if curr_time - self.last_connect_time < MySQLApi.RECONNECT_INTERVAL:
            return
        print('(re)connecting to mysql')
        self.db = mysql.connector.connect(
            host=self.mysql_settings.host,
            port=self.mysql_settings.port,
            user=self.mysql_settings.user,
            passwd=self.mysql_settings.password,
        )
        self.cursor = self.db.cursor()
        if self.database is not None:
            self.cursor.execute(f'USE {self.database}')
        self.last_connect_time = curr_time

    def drop_database(self, db_name):
        self.cursor.execute(f'DROP DATABASE IF EXISTS {db_name}')

    def create_database(self, db_name):
        self.cursor.execute(f'CREATE DATABASE {db_name}')

    def execute(self, command, commit=False):
        print(f'Executing: <{command}>')
        self.cursor.execute(command)
        if commit:
            self.db.commit()

    def get_tables(self):
        self.reconnect_if_required()
        self.cursor.execute('SHOW TABLES')
        res = self.cursor.fetchall()
        tables = [x[0] for x in res]
        return tables

    def get_binlog_files(self):
        self.reconnect_if_required()
        self.cursor.execute('SHOW BINARY LOGS')
        res = self.cursor.fetchall()
        tables = [x[0] for x in res]
        return tables

    def get_table_structure(self, table_name) -> TableStructure:
        self.reconnect_if_required()
        self.cursor.execute(f'SHOW CREATE TABLE {table_name}')
        res = self.cursor.fetchall()
        create_statement = res[0][1].strip()

        lines = create_statement.split('\n')
        inside_create = False

        structure = TableStructure()

        for line in lines:
            line = line.strip()
            if not inside_create:
                # CREATE TABLE `auth_group` (
                pattern = 'CREATE TABLE `' + Word(alphanums + '_') + '` ('
                result = pattern.parseString(line)
                parsed_table_name = result[1]
                if parsed_table_name != table_name:
                    raise Exception('failed to parse ' + table_name)
                inside_create = True
                continue
            if line.startswith('`'):
                if line.endswith(','):
                    line = line[:-1]
                definition = line.split(' ')
                field_name = definition[0]
                if field_name[0] != '`' or field_name[-1] != '`':
                    raise Exception('wrong field ' + field_name)
                field_name = field_name[1:-1]
                field_type = definition[1]
                field_parameters = ''
                if len(definition) > 2:
                    field_parameters = ' '.join(definition[2:])

                structure.fields.append(TableField(
                    name=field_name,
                    field_type=field_type,
                    parameters=field_parameters,
                ))
                continue
            if line.startswith('PRIMARY KEY'):
                # PRIMARY KEY (`policyaction_ptr_id`),
                pattern = 'PRIMARY KEY (`' + Word(alphanums + '_') + '`)'
                result = pattern.parseString(line)
                structure.primary_key = result[1]
            if line.startswith(')'):
                inside_create = False
                continue
        structure.preprocess()
        return structure

    def get_records(self, table_name, order_by, limit, start_value=None):
        self.reconnect_if_required()
        where = ''
        if start_value is not None:
            where = f'WHERE {order_by} > {start_value} '
        query = f'SELECT * FROM {table_name} {where}ORDER BY {order_by} LIMIT {limit}'
        self.cursor.execute(query)
        res = self.cursor.fetchall()
        records = [x for x in res]
        return records
