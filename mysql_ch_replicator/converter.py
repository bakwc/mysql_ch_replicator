import struct
import json
import sqlparse
import re
from pyparsing import Suppress, CaselessKeyword, Word, alphas, alphanums, delimitedList

from .table_structure import TableStructure, TableField


CHARSET_MYSQL_TO_PYTHON = {
    'armscii8': None,          # ARMSCII-8 is not directly supported in Python
    'ascii': 'ascii',
    'big5': 'big5',
    'binary': 'latin1',        # Treat binary data as Latin-1 in Python
    'cp1250': 'cp1250',
    'cp1251': 'cp1251',
    'cp1256': 'cp1256',
    'cp1257': 'cp1257',
    'cp850': 'cp850',
    'cp852': 'cp852',
    'cp866': 'cp866',
    'cp932': 'cp932',
    'dec8': 'latin1',          # DEC8 is similar to Latin-1
    'eucjpms': 'euc_jp',       # Map to EUC-JP
    'euckr': 'euc_kr',
    'gb18030': 'gb18030',
    'gb2312': 'gb2312',
    'gbk': 'gbk',
    'geostd8': None,           # GEOSTD8 is not directly supported in Python
    'greek': 'iso8859_7',
    'hebrew': 'iso8859_8',
    'hp8': None,               # HP8 is not directly supported in Python
    'keybcs2': None,           # KEYBCS2 is not directly supported in Python
    'koi8r': 'koi8_r',
    'koi8u': 'koi8_u',
    'latin1': 'cp1252',        # MySQL's latin1 corresponds to Windows-1252
    'latin2': 'iso8859_2',
    'latin5': 'iso8859_9',
    'latin7': 'iso8859_13',
    'macce': 'mac_latin2',
    'macroman': 'mac_roman',
    'sjis': 'shift_jis',
    'swe7': None,              # SWE7 is not directly supported in Python
    'tis620': 'tis_620',
    'ucs2': 'utf_16',          # UCS-2 can be mapped to UTF-16
    'ujis': 'euc_jp',
    'utf16': 'utf_16',
    'utf16le': 'utf_16_le',
    'utf32': 'utf_32',
    'utf8mb3': 'utf_8',        # Both utf8mb3 and utf8mb4 can be mapped to UTF-8
    'utf8mb4': 'utf_8',
    'utf8': 'utf_8',
}


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


def parse_mysql_point(binary):
    """
    Parses the binary representation of a MySQL POINT data type
    and returns a tuple (x, y) representing the coordinates.

    :param binary: The binary data representing the POINT.
    :return: A tuple (x, y) with the coordinate values.
    """
    if binary is None:
        return 0, 0

    if len(binary) == 21:
        # No SRID. Proceed as per WKB POINT
        # Read the byte order
        byte_order = binary[0]
        if byte_order == 0:
            endian = '>'
        elif byte_order == 1:
            endian = '<'
        else:
            raise ValueError("Invalid byte order in WKB POINT")
        # Read the WKB Type
        wkb_type = struct.unpack(endian + 'I', binary[1:5])[0]
        if wkb_type != 1:  # WKB type 1 means POINT
            raise ValueError("Not a WKB POINT type")
        # Read X and Y coordinates
        x = struct.unpack(endian + 'd', binary[5:13])[0]
        y = struct.unpack(endian + 'd', binary[13:21])[0]
    elif len(binary) == 25:
        # With SRID included
        # First 4 bytes are the SRID
        srid = struct.unpack('>I', binary[0:4])[0]  # SRID is big-endian
        # Next byte is byte order
        byte_order = binary[4]
        if byte_order == 0:
            endian = '>'
        elif byte_order == 1:
            endian = '<'
        else:
            raise ValueError("Invalid byte order in WKB POINT")
        # Read the WKB Type
        wkb_type = struct.unpack(endian + 'I', binary[5:9])[0]
        if wkb_type != 1:  # WKB type 1 means POINT
            raise ValueError("Not a WKB POINT type")
        # Read X and Y coordinates
        x = struct.unpack(endian + 'd', binary[9:17])[0]
        y = struct.unpack(endian + 'd', binary[17:25])[0]
    else:
        raise ValueError("Invalid binary length for WKB POINT")
    return (x, y)


def strip_sql_name(name):
    name = name.strip()
    if name.startswith('`'):
        name = name[1:]
    if name.endswith('`'):
        name = name[:-1]
    return name


def split_high_level(data, token):
    results = []
    level = 0
    curr_data = ''
    for c in data:
        if c == token and level == 0:
            results.append(curr_data.strip())
            curr_data = ''
            continue
        if c == '(':
            level += 1
        if c == ')':
            level -= 1
        curr_data += c
    if curr_data:
        results.append(curr_data.strip())
    return results


def strip_sql_comments(sql_statement):
    return sqlparse.format(sql_statement, strip_comments=True).strip()


def convert_timestamp_to_datetime64(input_str):

    # Define the regex pattern
    pattern = r'^timestamp(?:\((\d+)\))?$'

    # Attempt to match the pattern
    match = re.match(pattern, input_str.strip(), re.IGNORECASE)

    if match:
        # If a precision is provided, include it in the replacement
        precision = match.group(1)
        if precision is not None:
            return f'DateTime64({precision})'
        else:
            return 'DateTime64'
    else:
        raise ValueError(f"Invalid input string format: '{input_str}'")


class MysqlToClickhouseConverter:
    def __init__(self, db_replicator: 'DbReplicator' = None):
        self.db_replicator = db_replicator

    def convert_type(self, mysql_type, parameters):
        is_unsigned = 'unsigned' in parameters.lower()

        if mysql_type == 'point':
            return 'Tuple(x Float32, y Float32)'

        if mysql_type == 'int':
            if is_unsigned:
                return 'UInt32'
            return 'Int32'
        if mysql_type == 'integer':
            if is_unsigned:
                return 'UInt32'
            return 'Int32'
        if mysql_type == 'bigint':
            if is_unsigned:
                return 'UInt64'
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
        if mysql_type == 'bit(1)':
            return 'Bool'
        if mysql_type == 'bool':
            return 'Bool'
        if 'smallint' in mysql_type:
            if is_unsigned:
                return 'UInt16'
            return 'Int16'
        if 'tinyint' in mysql_type:
            if is_unsigned:
                return 'UInt8'
            return 'Int8'
        if 'mediumint' in mysql_type:
            if is_unsigned:
                return 'UInt32'
            return 'Int32'
        if 'datetime' in mysql_type:
            return mysql_type.replace('datetime', 'DateTime64')
        if 'longtext' in mysql_type:
            return 'String'
        if 'varchar' in mysql_type:
            return 'String'
        if 'enum' in mysql_type:
            return 'String'
        if 'text' in mysql_type:
            return 'String'
        if 'blob' in mysql_type:
            return 'String'
        if 'char' in mysql_type:
            return 'String'
        if 'json' in mysql_type:
            return 'String'
        if 'decimal' in mysql_type:
            return 'Float64'
        if 'float' in mysql_type:
            return 'Float32'
        if 'double' in mysql_type:
            return 'Float64'
        if 'bigint' in mysql_type:
            if is_unsigned:
                return 'UInt64'
            return 'Int64'
        if 'integer' in mysql_type or 'int(' in mysql_type:
            if is_unsigned:
                return 'UInt32'
            return 'Int32'
        if 'real' in mysql_type:
            return 'Float64'
        if mysql_type.startswith('timestamp'):
            return convert_timestamp_to_datetime64(mysql_type)
        if mysql_type.startswith('time'):
            return 'String'
        if 'varbinary' in mysql_type:
            return 'String'
        if 'binary' in mysql_type:
            return 'String'
        if 'set(' in mysql_type:
            return 'String'
        raise Exception(f'unknown mysql type "{mysql_type}"')

    def convert_field_type(self, mysql_type, mysql_parameters):
        mysql_type = mysql_type.lower()
        mysql_parameters = mysql_parameters.lower()
        not_null = 'not null' in mysql_parameters
        clickhouse_type = self.convert_type(mysql_type, mysql_parameters)
        if 'Tuple' in clickhouse_type:
            not_null = True
        if not not_null:
            clickhouse_type = f'Nullable({clickhouse_type})'
        return clickhouse_type

    def convert_table_structure(self, mysql_structure: TableStructure) -> TableStructure:
        clickhouse_structure = TableStructure()
        clickhouse_structure.table_name = mysql_structure.table_name
        clickhouse_structure.if_not_exists = mysql_structure.if_not_exists
        for field in mysql_structure.fields:
            clickhouse_field_type = self.convert_field_type(field.field_type, field.parameters)
            clickhouse_structure.fields.append(TableField(
                name=field.name,
                field_type=clickhouse_field_type,
            ))
        clickhouse_structure.primary_keys = mysql_structure.primary_keys
        clickhouse_structure.preprocess()
        return clickhouse_structure

    def convert_records(
            self, mysql_records, mysql_structure: TableStructure, clickhouse_structure: TableStructure,
            only_primary: bool = False,
    ):
        mysql_field_types = [field.field_type for field in mysql_structure.fields]
        clickhouse_filed_types = [field.field_type for field in clickhouse_structure.fields]

        clickhouse_records = []
        for mysql_record in mysql_records:
            clickhouse_record = self.convert_record(
                mysql_record, mysql_field_types, clickhouse_filed_types, mysql_structure, only_primary,
            )
            clickhouse_records.append(clickhouse_record)
        return clickhouse_records

    def convert_record(
            self, mysql_record, mysql_field_types, clickhouse_field_types, mysql_structure: TableStructure,
            only_primary: bool,
    ):
        clickhouse_record = []
        for idx, mysql_field_value in enumerate(mysql_record):
            if only_primary and idx not in mysql_structure.primary_key_ids:
                clickhouse_record.append(mysql_field_value)
                continue

            clickhouse_field_value = mysql_field_value
            mysql_field_type = mysql_field_types[idx]
            clickhouse_field_type = clickhouse_field_types[idx]
            if mysql_field_type.startswith('time') and 'String' in clickhouse_field_type:
                clickhouse_field_value = str(mysql_field_value)
            if mysql_field_type == 'json' and 'String' in clickhouse_field_type:
                if not isinstance(clickhouse_field_value, str):
                    clickhouse_field_value = json.dumps(convert_bytes(clickhouse_field_value))

            if clickhouse_field_value is not None:
                if 'UInt16' in clickhouse_field_type and clickhouse_field_value < 0:
                    clickhouse_field_value = 65536 + clickhouse_field_value
                if 'UInt8' in clickhouse_field_type and clickhouse_field_value < 0:
                    clickhouse_field_value = 256 + clickhouse_field_value
                if 'mediumint' in mysql_field_type.lower() and clickhouse_field_value < 0:
                    clickhouse_field_value = 16777216 + clickhouse_field_value
                if 'UInt32' in clickhouse_field_type and clickhouse_field_value < 0:
                    clickhouse_field_value = 4294967296 + clickhouse_field_value
                if 'UInt64' in clickhouse_field_type and clickhouse_field_value < 0:
                    clickhouse_field_value = 18446744073709551616 + clickhouse_field_value

                if 'String' in clickhouse_field_type and (
                        'text' in mysql_field_type or 'char' in mysql_field_type
                ):
                    if isinstance(clickhouse_field_value, bytes):
                        charset = mysql_structure.charset_python or 'utf-8'
                        clickhouse_field_value = clickhouse_field_value.decode(charset)

                if 'set(' in mysql_field_type:
                    set_values = mysql_structure.fields[idx].additional_data
                    if isinstance(clickhouse_field_value, int):
                        bit_mask = clickhouse_field_value
                        clickhouse_field_value = [
                            val
                            for idx, val in enumerate(set_values)
                            if bit_mask & (1 << idx)
                        ]
                    elif isinstance(clickhouse_field_value, set):
                        clickhouse_field_value = [
                            v for v in set_values if v in clickhouse_field_value
                        ]
                    clickhouse_field_value = ','.join(clickhouse_field_value)

            if 'point' in mysql_field_type:
                clickhouse_field_value = parse_mysql_point(clickhouse_field_value)

            clickhouse_record.append(clickhouse_field_value)
        return tuple(clickhouse_record)

    def __basic_validate_query(self, mysql_query):
        mysql_query = mysql_query.strip()
        if mysql_query.endswith(';'):
            mysql_query = mysql_query[:-1]
        if mysql_query.find(';') != -1:
            raise Exception('multi-query statement not supported')
        return mysql_query
    
    def get_db_and_table_name(self, token, db_name):
        if '.' in token:
            db_name, table_name = token.split('.')
        else:
            table_name = token
        db_name = strip_sql_name(db_name)
        table_name = strip_sql_name(table_name)
        if self.db_replicator:
            if db_name == self.db_replicator.database:
                db_name = self.db_replicator.target_database
            matches_config = (
                self.db_replicator.config.is_database_matches(db_name)
                and self.db_replicator.config.is_table_matches(table_name))
        else:
            matches_config = True

        return db_name, table_name, matches_config

    def convert_alter_query(self, mysql_query, db_name):
        mysql_query = self.__basic_validate_query(mysql_query)

        tokens = mysql_query.split()
        if tokens[0].lower() != 'alter':
            raise Exception('wrong query')

        if tokens[1].lower() != 'table':
            raise Exception('wrong query')

        db_name, table_name, matches_config = self.get_db_and_table_name(tokens[2], db_name)

        if not matches_config:
            return

        subqueries = ' '.join(tokens[3:])
        subqueries = split_high_level(subqueries, ',')

        for subquery in subqueries:
            subquery = subquery.strip()
            tokens = subquery.split()

            op_name = tokens[0].lower()
            tokens = tokens[1:]

            if tokens[0].lower() == 'column':
                tokens = tokens[1:]

            if op_name == 'add':
                if tokens[0].lower() in ('constraint', 'index', 'foreign'):
                    continue
                self.__convert_alter_table_add_column(db_name, table_name, tokens)
                continue

            if op_name == 'drop':
                if tokens[0].lower() in ('constraint', 'index', 'foreign'):
                    continue
                self.__convert_alter_table_drop_column(db_name, table_name, tokens)
                continue

            if op_name == 'modify':
                self.__convert_alter_table_modify_column(db_name, table_name, tokens)
                continue

            if op_name == 'alter':
                continue

            if op_name == 'change':
                self.__convert_alter_table_change_column(db_name, table_name, tokens)
                continue

            raise Exception(f'operation {op_name} not implement, query: {subquery}')

    def __convert_alter_table_add_column(self, db_name, table_name, tokens):
        if len(tokens) < 2:
            raise Exception('wrong tokens count', tokens)

        if ',' in ' '.join(tokens):
            raise Exception('add multiple columns not implemented', tokens)

        column_after = None
        column_first = False
        if tokens[-2].lower() == 'after':
            column_after = strip_sql_name(tokens[-1])
            tokens = tokens[:-2]
            if len(tokens) < 2:
                raise Exception('wrong tokens count', tokens)
        elif tokens[-1].lower() == 'first':
            column_first = True

        column_name = strip_sql_name(tokens[0])
        column_type_mysql = tokens[1]
        column_type_mysql_parameters = ' '.join(tokens[2:])

        column_type_ch = self.convert_field_type(column_type_mysql, column_type_mysql_parameters)

        # update table structure
        if self.db_replicator:
            table_structure = self.db_replicator.state.tables_structure[table_name]
            mysql_table_structure: TableStructure = table_structure[0]
            ch_table_structure: TableStructure = table_structure[1]

            if column_first:
                mysql_table_structure.add_field_first(
                    TableField(name=column_name, field_type=column_type_mysql)
                )
                
                ch_table_structure.add_field_first(
                    TableField(name=column_name, field_type=column_type_ch)
                )
            else:
                if column_after is None:
                    column_after = strip_sql_name(mysql_table_structure.fields[-1].name)

                mysql_table_structure.add_field_after(
                    TableField(name=column_name, field_type=column_type_mysql),
                    column_after,
                )

                ch_table_structure.add_field_after(
                    TableField(name=column_name, field_type=column_type_ch),
                    column_after,
                )

        query = f'ALTER TABLE {db_name}.{table_name} ADD COLUMN {column_name} {column_type_ch}'
        if column_first:
            query += ' FIRST'
        else:
            query += f' AFTER {column_after}'

        if self.db_replicator:
            self.db_replicator.clickhouse_api.execute_command(query)

    def __convert_alter_table_drop_column(self, db_name, table_name, tokens):
        if ',' in ' '.join(tokens):
            raise Exception('add multiple columns not implemented', tokens)

        if len(tokens) != 1:
            raise Exception('wrong tokens count', tokens)

        column_name = strip_sql_name(tokens[0])

        # update table structure
        if self.db_replicator:
            table_structure = self.db_replicator.state.tables_structure[table_name]
            mysql_table_structure: TableStructure = table_structure[0]
            ch_table_structure: TableStructure = table_structure[1]

            mysql_table_structure.remove_field(field_name=column_name)
            ch_table_structure.remove_field(field_name=column_name)

        query = f'ALTER TABLE {db_name}.{table_name} DROP COLUMN {column_name}'
        if self.db_replicator:
            self.db_replicator.clickhouse_api.execute_command(query)

    def __convert_alter_table_modify_column(self, db_name, table_name, tokens):
        if len(tokens) < 2:
            raise Exception('wrong tokens count', tokens)

        if ',' in ' '.join(tokens):
            raise Exception('add multiple columns not implemented', tokens)

        column_name = strip_sql_name(tokens[0])
        column_type_mysql = tokens[1]
        column_type_mysql_parameters = ' '.join(tokens[2:])

        column_type_ch = self.convert_field_type(column_type_mysql, column_type_mysql_parameters)

        # update table structure
        if self.db_replicator:
            table_structure = self.db_replicator.state.tables_structure[table_name]
            mysql_table_structure: TableStructure = table_structure[0]
            ch_table_structure: TableStructure = table_structure[1]

            mysql_table_structure.update_field(
                TableField(name=column_name, field_type=column_type_mysql),
            )

            ch_table_structure.update_field(
                TableField(name=column_name, field_type=column_type_ch),
            )

        query = f'ALTER TABLE {db_name}.{table_name} MODIFY COLUMN {column_name} {column_type_ch}'
        if self.db_replicator:
            self.db_replicator.clickhouse_api.execute_command(query)

    def __convert_alter_table_change_column(self, db_name, table_name, tokens):
        if len(tokens) < 3:
            raise Exception('wrong tokens count', tokens)

        if ',' in ' '.join(tokens):
            raise Exception('add multiple columns not implemented', tokens)

        column_name = strip_sql_name(tokens[0])
        new_column_name = strip_sql_name(tokens[1])
        column_type_mysql = tokens[2]
        column_type_mysql_parameters = ' '.join(tokens[3:])

        column_type_ch = self.convert_field_type(column_type_mysql, column_type_mysql_parameters)

        # update table structure
        if self.db_replicator:
            table_structure = self.db_replicator.state.tables_structure[table_name]
            mysql_table_structure: TableStructure = table_structure[0]
            ch_table_structure: TableStructure = table_structure[1]

            current_column_type_ch = ch_table_structure.get_field(column_name).field_type

            if current_column_type_ch != column_type_ch:

                mysql_table_structure.update_field(
                    TableField(name=column_name, field_type=column_type_mysql),
                )

                ch_table_structure.update_field(
                    TableField(name=column_name, field_type=column_type_ch),
                )

                query = f'ALTER TABLE {db_name}.{table_name} MODIFY COLUMN {column_name} {column_type_ch}'
                self.db_replicator.clickhouse_api.execute_command(query)

            if column_name != new_column_name:
                curr_field_mysql = mysql_table_structure.get_field(column_name)
                curr_field_clickhouse = ch_table_structure.get_field(column_name)

                curr_field_mysql.name = new_column_name
                curr_field_clickhouse.name = new_column_name

                query = f'ALTER TABLE {db_name}.{table_name} RENAME COLUMN {column_name} TO {new_column_name}'
                self.db_replicator.clickhouse_api.execute_command(query)

    def parse_create_table_query(self, mysql_query) -> tuple[TableStructure, TableStructure]:
        mysql_table_structure = self.parse_mysql_table_structure(mysql_query)
        ch_table_structure = self.convert_table_structure(mysql_table_structure)
        return mysql_table_structure, ch_table_structure

    def convert_drop_table_query(self, mysql_query):
        raise Exception('not implement')

    def _strip_comments(self, create_statement):
        pattern = r'\bCOMMENT(?:\s*=\s*|\s+)([\'"])(?:\\.|[^\\])*?\1'
        return re.sub(pattern, '', create_statement, flags=re.IGNORECASE)

    def parse_mysql_table_structure(self, create_statement, required_table_name=None):
        create_statement = self._strip_comments(create_statement)

        structure = TableStructure()

        tokens = sqlparse.parse(create_statement.replace('\n', ' ').strip())[0].tokens
        tokens = [t for t in tokens if not t.is_whitespace and not t.is_newline]

        # remove "IF NOT EXISTS"
        if (len(tokens) > 5 and
                tokens[0].normalized.upper() == 'CREATE' and
                tokens[1].normalized.upper() == 'TABLE' and
                tokens[2].normalized.upper() == 'IF' and
                tokens[3].normalized.upper() == 'NOT' and
                tokens[4].normalized.upper() == 'EXISTS'):
            del tokens[2:5]  # Remove the 'IF', 'NOT', 'EXISTS' tokens
            structure.if_not_exists = True

        if tokens[0].ttype != sqlparse.tokens.DDL:
            raise Exception('wrong create statement', create_statement)
        if tokens[0].normalized.lower() != 'create':
            raise Exception('wrong create statement', create_statement)
        if tokens[1].ttype != sqlparse.tokens.Keyword:
            raise Exception('wrong create statement', create_statement)

        if not isinstance(tokens[2], sqlparse.sql.Identifier):
            raise Exception('wrong create statement', create_statement)

        # get_real_name() returns the table name if the token is in the
        # style `<dbname>.<tablename>`
        structure.table_name = strip_sql_name(tokens[2].get_real_name())

        if not isinstance(tokens[3], sqlparse.sql.Parenthesis):
            raise Exception('wrong create statement', create_statement)

        #print(' --- processing statement:\n', create_statement, '\n')

        inner_tokens = tokens[3].tokens
        inner_tokens = ''.join([str(t) for t in inner_tokens[1:-1]]).strip()
        inner_tokens = split_high_level(inner_tokens, ',')

        prev_token = ''
        prev_prev_token = ''
        for line in tokens[4:]:
            curr_token = line.value
            if prev_token == '=' and prev_prev_token.lower() == 'charset':
                structure.charset = curr_token
            prev_prev_token = prev_token
            prev_token = curr_token

        structure.charset_python = 'utf-8'

        if structure.charset:
            structure.charset_python = CHARSET_MYSQL_TO_PYTHON[structure.charset]

        prev_line = ''
        for line in inner_tokens:
            line = prev_line + line
            q_count = line.count('`')
            if q_count % 2 == 1:
                prev_line = line
                continue
            prev_line = ''

            if line.lower().startswith('unique key'):
                continue
            if line.lower().startswith('key'):
                continue
            if line.lower().startswith('constraint'):
                continue
            if line.lower().startswith('fulltext'):
                continue
            if line.lower().startswith('spatial'):
                continue
            if line.lower().startswith('primary key'):
                # Define identifier to match column names, handling backticks and unquoted names
                identifier = (Suppress('`') + Word(alphas + alphanums + '_') + Suppress('`')) | Word(
                    alphas + alphanums + '_')

                # Build the parsing pattern
                pattern = CaselessKeyword('PRIMARY') + CaselessKeyword('KEY') + Suppress('(') + delimitedList(
                    identifier)('column_names') + Suppress(')')

                # Parse the line
                result = pattern.parseString(line)

                # Extract and process the primary key column names
                primary_keys = [strip_sql_name(name) for name in result['column_names']]

                structure.primary_keys = primary_keys

                continue

            #print(" === processing line", line)

            definition = line.split(' ')
            field_name = strip_sql_name(definition[0])
            field_type = definition[1]
            field_parameters = ''
            if len(definition) > 2:
                field_parameters = ' '.join(definition[2:])

            additional_data = None
            if 'set(' in field_type.lower():
                vals = field_type[len('set('):]
                close_pos = vals.find(')')
                vals = vals[:close_pos]
                vals = vals.split(',')
                def vstrip(e):
                    if not e:
                        return e
                    if e[0] in '"\'':
                        return e[1:-1]
                    return e
                vals = [vstrip(v) for v in vals]
                additional_data = vals

            structure.fields.append(TableField(
                name=field_name,
                field_type=field_type,
                parameters=field_parameters,
                additional_data=additional_data,
            ))
            #print(' ---- params:', field_parameters)


        if not structure.primary_keys:
            for field in structure.fields:
                if 'primary key' in field.parameters.lower():
                    structure.primary_keys.append(field.name)

        if not structure.primary_keys:
            if structure.has_field('id'):
                structure.primary_keys = ['id']

        if not structure.primary_keys:
            raise Exception(f'No primary key for table {structure.table_name}, {create_statement}')

        structure.preprocess()
        return structure
