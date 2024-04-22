import mysql.connector
from pyparsing import Word, alphas, alphanums

from config import MysqlSettings
from table_structure import TableStructure, TableField


class MySQLApi:
    def __init__(self, database: str | None, mysql_settings: MysqlSettings):
        self.database = database
        self.mysql_settings = mysql_settings
        self.db = mysql.connector.connect(
            host=mysql_settings.host,
            port=mysql_settings.port,
            user=mysql_settings.user,
            passwd=mysql_settings.password,
        )
        self.cursor = self.db.cursor()
        if database is not None:
            self.cursor.execute(f'USE {database}')

    def get_tables(self):
        self.cursor.execute('SHOW TABLES')
        res = self.cursor.fetchall()
        tables = [x[0] for x in res]
        return tables

    def get_binlog_files(self):
        self.cursor.execute('SHOW BINARY LOGS')
        res = self.cursor.fetchall()
        tables = [x[0] for x in res]
        return tables

    def get_table_structure(self, table_name) -> TableStructure:
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

    def get_records(self, table_name, order_by, limit, start_value = None):
        where = ''
        if start_value is not None:
            where = f'WHERE {order_by} > {start_value} '
        query = f'SELECT * FROM {table_name} {where}ORDER BY {order_by} LIMIT {limit}'
        self.cursor.execute(query)
        res = self.cursor.fetchall()
        records = [x for x in res]
        return records
