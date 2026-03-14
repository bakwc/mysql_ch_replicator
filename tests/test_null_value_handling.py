import pytest
from mysql_ch_replicator.converter import MysqlToClickhouseConverter
from mysql_ch_replicator.table_structure import TableStructure, TableField
from mysql_ch_replicator.clickhouse_api import ClickhouseApi
from mysql_ch_replicator.config import Settings


def test_null_value_in_non_nullable_int_column():
    """
    Test that NULL values in MySQL are properly converted to default values
    when inserting into non-nullable ClickHouse columns.
    
    This test reproduces the bug where the replicator crashes with:
    TypeError: int() argument must be a string, a bytes-like object or a real number, not 'NoneType'
    """
    converter = MysqlToClickhouseConverter()
    
    # MySQL structure with NOT NULL int column
    mysql_structure = TableStructure()
    mysql_structure.table_name = 'test_table'
    mysql_structure.primary_keys = ['id']
    mysql_structure.fields = [
        TableField(name='id', field_type='int', parameters='NOT NULL'),
        TableField(name='value', field_type='int', parameters='NOT NULL'),
    ]
    mysql_structure.preprocess()
    
    # ClickHouse structure - non-nullable Int32 (because MySQL has NOT NULL)
    clickhouse_structure = TableStructure()
    clickhouse_structure.table_name = 'test_table'
    clickhouse_structure.primary_keys = ['id']
    clickhouse_structure.fields = [
        TableField(name='id', field_type='Int32'),
        TableField(name='value', field_type='Int32'),
    ]
    clickhouse_structure.preprocess()
    
    # MySQL record with NULL value in NOT NULL column
    mysql_record = (1, None)
    
    # This should NOT crash with TypeError
    # It should convert NULL to a default value (0 for Int32)
    result = converter.convert_record(
        mysql_record,
        [field.field_type for field in mysql_structure.fields],
        [field.field_type for field in clickhouse_structure.fields],
        mysql_structure,
        only_primary=False
    )
    
    # The result should be a tuple with default value for NULL
    assert result is not None
    assert len(result) == 2
    assert result[0] == 1
    # NULL should be converted to 0 for non-nullable Int32
    assert result[1] == 0


def test_null_value_in_nullable_int_column():
    """
    Test that NULL values are preserved when ClickHouse column is nullable.
    """
    converter = MysqlToClickhouseConverter()
    
    # MySQL structure with nullable int column
    mysql_structure = TableStructure()
    mysql_structure.table_name = 'test_table'
    mysql_structure.primary_keys = ['id']
    mysql_structure.fields = [
        TableField(name='id', field_type='int', parameters=''),
        TableField(name='value', field_type='int', parameters=''),
    ]
    mysql_structure.preprocess()
    
    # ClickHouse structure - nullable Int32
    clickhouse_structure = TableStructure()
    clickhouse_structure.table_name = 'test_table'
    clickhouse_structure.primary_keys = ['id']
    clickhouse_structure.fields = [
        TableField(name='id', field_type='Nullable(Int32)'),
        TableField(name='value', field_type='Nullable(Int32)'),
    ]
    clickhouse_structure.preprocess()
    
    # MySQL record with NULL value
    mysql_record = (1, None)
    
    result = converter.convert_record(
        mysql_record,
        [field.field_type for field in mysql_structure.fields],
        [field.field_type for field in clickhouse_structure.fields],
        mysql_structure,
        only_primary=False
    )
    
    # NULL should be preserved for nullable columns
    assert result is not None
    assert len(result) == 2
    assert result[0] == 1
    assert result[1] is None


def test_null_value_in_non_nullable_string_column():
    """
    Test that NULL values in non-nullable String columns are converted to empty string.
    """
    converter = MysqlToClickhouseConverter()
    
    mysql_structure = TableStructure()
    mysql_structure.table_name = 'test_table'
    mysql_structure.primary_keys = ['id']
    mysql_structure.fields = [
        TableField(name='id', field_type='int', parameters='NOT NULL'),
        TableField(name='name', field_type='varchar', parameters='NOT NULL'),
    ]
    mysql_structure.preprocess()
    
    clickhouse_structure = TableStructure()
    clickhouse_structure.table_name = 'test_table'
    clickhouse_structure.primary_keys = ['id']
    clickhouse_structure.fields = [
        TableField(name='id', field_type='Int32'),
        TableField(name='name', field_type='String'),
    ]
    clickhouse_structure.preprocess()
    
    mysql_record = (1, None)
    
    result = converter.convert_record(
        mysql_record,
        [field.field_type for field in mysql_structure.fields],
        [field.field_type for field in clickhouse_structure.fields],
        mysql_structure,
        only_primary=False
    )
    
    assert result is not None
    assert len(result) == 2
    assert result[0] == 1
    # NULL should be converted to empty string for non-nullable String
    assert result[1] == ''


def test_clickhouse_api_get_table_structure():
    """
    Test that ClickhouseApi.get_table_structure returns the actual table structure
    from ClickHouse, including non-nullable String columns and primary keys.
    """
    cfg = Settings()
    cfg.load('tests/tests_config.yaml')
    
    ch = ClickhouseApi(
        database='test_db',
        clickhouse_settings=cfg.clickhouse,
    )
    
    ch.create_database('test_db')
    ch.execute_command('USE `test_db`')
    ch.execute_command('''
CREATE TABLE `test_db`.`test_structure_table` (
    id Int32,
    email String,
    name Nullable(String),
    `_version` UInt64
) ENGINE = ReplacingMergeTree(_version)
ORDER BY id
    ''')
    
    structure = ch.get_table_structure('test_structure_table')
    
    assert structure.table_name == 'test_structure_table'
    assert len(structure.fields) == 3
    
    assert structure.fields[0].name == 'id'
    assert structure.fields[0].field_type == 'Int32'
    
    assert structure.fields[1].name == 'email'
    assert structure.fields[1].field_type == 'String'
    assert 'Nullable' not in structure.fields[1].field_type
    
    assert structure.fields[2].name == 'name'
    assert 'Nullable' in structure.fields[2].field_type
    
    assert structure.primary_keys == ['id']
    
    ch.drop_table('test_structure_table')
    ch.drop_database('test_db')
