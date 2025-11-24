import pytest
from mysql_ch_replicator.converter import split_high_level


@pytest.mark.parametrize("data,delimiter,expected", [
    # Basic column definitions without quotes or parentheses
    (
        "id int NOT NULL, name varchar(255), age int",
        ",",
        ['id int NOT NULL', 'name varchar(255)', 'age int']
    ),

    # Column with DEFAULT value containing comma inside single quotes
    (
        "status varchar(50) DEFAULT 'active,pending', id int",
        ",",
        ["status varchar(50) DEFAULT 'active,pending'", 'id int']
    ),

    # Multiple columns with quoted DEFAULT values containing commas
    (
        "col1 varchar(50) DEFAULT 'value,with,commas', col2 int, col3 varchar(100) DEFAULT 'another,comma'",
        ",",
        ["col1 varchar(50) DEFAULT 'value,with,commas'", 'col2 int', "col3 varchar(100) DEFAULT 'another,comma'"]
    ),

    # ENUM definition with multiple values (commas inside parentheses)
    (
        "status enum('active','inactive','pending'), id int",
        ",",
        ["status enum('active','inactive','pending')", 'id int']
    ),

    # SET type with multiple values
    (
        "permissions set('read','write','execute'), user_id int",
        ",",
        ["permissions set('read','write','execute')", 'user_id int']
    ),

    # Column with DEFAULT containing single quote with comma
    (
        "description text DEFAULT 'User, Admin', created_at datetime",
        ",",
        ["description text DEFAULT 'User, Admin'", 'created_at datetime']
    ),

    # DECIMAL with precision and scale (comma inside parentheses)
    (
        "price decimal(10,2), quantity int",
        ",",
        ['price decimal(10,2)', 'quantity int']
    ),

    # Complex: ENUM + DEFAULT with commas in both
    (
        "type enum('type1','type2') DEFAULT 'type1', description varchar(255) DEFAULT 'desc,with,comma'",
        ",",
        ["type enum('type1','type2') DEFAULT 'type1'", "description varchar(255) DEFAULT 'desc,with,comma'"]
    ),

    # VARCHAR with length and DEFAULT containing comma
    (
        "name varchar(100) DEFAULT 'Last, First', id int NOT NULL",
        ",",
        ["name varchar(100) DEFAULT 'Last, First'", 'id int NOT NULL']
    ),

    # Empty string should return empty list
    (
        "",
        ",",
        []
    ),

    # Single column definition
    (
        "id int PRIMARY KEY",
        ",",
        ['id int PRIMARY KEY']
    ),

    # Multiple nested parentheses
    (
        "data1 varchar(100), func(arg1, arg2), data2 int",
        ",",
        ['data1 varchar(100)', 'func(arg1, arg2)', 'data2 int']
    ),

    # ALTER TABLE multi-statement with commas in DEFAULT values
    (
        "ADD COLUMN status varchar(50) DEFAULT 'new,value', DROP COLUMN old_col",
        ",",
        ["ADD COLUMN status varchar(50) DEFAULT 'new,value'", 'DROP COLUMN old_col']
    ),

    # Real-world example from MySQL CREATE TABLE
    (
        "`id` int NOT NULL AUTO_INCREMENT, `email` varchar(255) DEFAULT 'user@example.com', `status` enum('active','inactive') DEFAULT 'active'",
        ",",
        ["`id` int NOT NULL AUTO_INCREMENT", "`email` varchar(255) DEFAULT 'user@example.com'", "`status` enum('active','inactive') DEFAULT 'active'"]
    ),
])
def test_split_high_level(data, delimiter, expected):
    """
    Test the split_high_level function with SQL column definitions.

    This test verifies that the function correctly splits SQL statements by the delimiter
    while ignoring delimiters that appear inside:
    - Parentheses (e.g., enum values, function arguments, type precision)
    - Single quotes (e.g., DEFAULT values, string literals)
    """
    result = split_high_level(data, delimiter)
    assert result == expected, f"Failed for input: {data} with delimiter: {delimiter}"
