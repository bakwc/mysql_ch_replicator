import json

from table_structure import TableStructure, TableField


def convert_bytes(obj):
    if isinstance(obj, dict):
        new_obj = {}
        for k, v in obj.items():
            new_key = k.decode('utf-8') if isinstance(k, bytes) else k
            new_value = convert_bytes(v)
            new_obj[new_key] = new_value
        return new_obj
    elif isinstance(obj, (tuple, list)):
        new_obj = []
        for item in obj:
            new_obj.append(convert_bytes(item))
        if isinstance(obj, tuple):
            return tuple(new_obj)
        return new_obj
    elif isinstance(obj, bytes):
        return obj.decode('utf-8')
    else:
        return obj


class MysqlToClickhouseConverter:
    def __init__(self, db_replicator: 'DbReplicator' = None):
        self.db_replicator = db_replicator

    def convert_type(self, mysql_type):
        if mysql_type == 'int':
            return 'Int32'
        if mysql_type == 'bigint':
            return 'Int64'
        if mysql_type == 'double':
            return 'Float64'
        if mysql_type == 'real':
            return 'Float64'
        if mysql_type == 'float':
            return 'Float32'
        if mysql_type == 'date':
            return 'Date32'
        if mysql_type == 'tinyint(1)':
            return 'Bool'
        if mysql_type == 'smallint':
            return 'Int16'
        if 'datetime' in mysql_type:
            return mysql_type.replace('datetime', 'DateTime64')
        if 'longtext' in mysql_type:
            return 'String'
        if 'varchar' in mysql_type:
            return 'String'
        if 'char' in mysql_type:
            return 'String'
        if 'json' in mysql_type:
            return 'String'
        if mysql_type.startswith('time'):
            return 'String'
        raise Exception(f'unknown mysql type "{mysql_type}"')

    def convert_field_type(self, mysql_type, mysql_parameters):
        mysql_type = mysql_type.lower()
        mysql_parameters = mysql_parameters.lower()
        not_null = 'not null' in mysql_parameters
        clickhouse_type = self.convert_type(mysql_type)
        if not not_null:
            clickhouse_type = f'Nullable({clickhouse_type})'
        return clickhouse_type

    def convert_table_structure(self, mysql_structure: TableStructure) -> TableStructure:
        clickhouse_structure = TableStructure()
        for field in mysql_structure.fields:
            clickhouse_field_type = self.convert_field_type(field.field_type, field.parameters)
            clickhouse_structure.fields.append(TableField(
                name=field.name,
                field_type=clickhouse_field_type,
            ))
        clickhouse_structure.primary_key = mysql_structure.primary_key
        clickhouse_structure.preprocess()
        return clickhouse_structure

    def convert_records(self, mysql_records, mysql_structure: TableStructure, clickhouse_structure: TableStructure):
        mysql_field_types = [field.field_type for field in mysql_structure.fields]
        clickhouse_filed_types = [field.field_type for field in clickhouse_structure.fields]

        clickhouse_records = []
        for mysql_record in mysql_records:
            clickhouse_record = self.convert_record(mysql_record, mysql_field_types, clickhouse_filed_types)
            clickhouse_records.append(clickhouse_record)
        return clickhouse_records

    def convert_record(self, mysql_record, mysql_field_types, clickhouse_field_types):
        clickhouse_record = []
        for idx, mysql_field_value in enumerate(mysql_record):
            clickhouse_field_value = mysql_field_value
            mysql_field_type = mysql_field_types[idx]
            clickhouse_field_type = clickhouse_field_types[idx]
            if mysql_field_type.startswith('time') and 'String' in clickhouse_field_type:
                clickhouse_field_value = str(mysql_field_value)
            if mysql_field_type == 'json' and 'String' in clickhouse_field_type:
                if not isinstance(clickhouse_field_value, str):
                    clickhouse_field_value = json.dumps(convert_bytes(clickhouse_field_value))
            clickhouse_record.append(clickhouse_field_value)
        return tuple(clickhouse_record)

    def __basic_validate_query(self, mysql_query):
        mysql_query = mysql_query.strip()
        if mysql_query.endswith(';'):
            mysql_query = mysql_query[:-1]
        if mysql_query.find(';') != -1:
            raise Exception('multi-query statement not supported')
        return mysql_query

    def convert_alter_query(self, mysql_query, db_name):
        mysql_query = self.__basic_validate_query(mysql_query)

        tokens = mysql_query.split()
        if tokens[0].lower() != 'alter':
            raise Exception('wrong query')

        if tokens[1].lower() != 'table':
            raise Exception('wrong query')

        table_name = tokens[2]
        if table_name.find('.') != -1:
            db_name, table_name = table_name.split('.')

        op_name = tokens[3].lower()
        if op_name == 'add':
            tokens = tokens[4:]
            if tokens[0].lower() == 'column':
                tokens = tokens[1:]
            return self.__convert_alter_table_add_column(db_name, table_name, tokens)

        raise Exception('not implement')

    def __convert_alter_table_add_column(self, db_name, table_name, tokens):
        if len(tokens) < 2:
            raise Exception('wrong tokens count', tokens)

        if ',' in ' '.join(tokens):
            raise Exception('add multiple columns not implemented', tokens)

        column_after = None
        if tokens[-2].lower() == 'after':
            column_after = tokens[-1]
            tokens = tokens[:-2]
            if len(tokens) < 2:
                raise Exception('wrong tokens count', tokens)

        column_name = tokens[0]
        column_type_mysql = tokens[1]
        column_type_mysql_parameters = ' '.join(tokens[2:])

        column_type_ch = self.convert_field_type(column_type_mysql, column_type_mysql_parameters)

        # update table structure
        if self.db_replicator:
            table_structure = self.db_replicator.state.tables_structure[table_name]
            mysql_table_structure: TableStructure = table_structure[0]
            ch_table_structure: TableStructure = table_structure[1]

            if column_after is None:
                column_after = mysql_table_structure.fields[-1].name

            mysql_table_structure.add_field_after(
                TableField(name=column_name, field_type=column_type_mysql),
                column_after,
            )

            ch_table_structure.add_field_after(
                TableField(name=column_name, field_type=column_type_ch),
                column_after,
            )

        query = f'ALTER TABLE {db_name}.{table_name} ADD COLUMN {column_name} {column_type_ch}'
        if column_after is not None:
            query += f' AFTER {column_after}'

        return query

    def convert_create_table_query(self, mysql_query):
        raise Exception('not implement')

    def convert_drop_table_query(self, mysql_query):
        raise Exception('not implement')
