import pytest
from mysql_ch_replicator.converter import MysqlToClickhouseConverter

 
@pytest.mark.parametrize("input_sql,expected_output", [
    # Basic single quote comment
    (
        "CREATE TABLE test (id int NOT NULL COMMENT 'Simple comment', name varchar(255))",
        "CREATE TABLE test (id int NOT NULL , name varchar(255))"
    ),
    # Basic double quote comment
    (
        "CREATE TABLE test (id int NOT NULL COMMENT \"Simple comment\", name varchar(255))",
        "CREATE TABLE test (id int NOT NULL , name varchar(255))"
    ),
    # Comment with escaped single quotes (the original bug case)
    (
        "CREATE TABLE test (id int NOT NULL COMMENT '事件类型，可选值: ''SYSTEM'', ''BUSINESS''', name varchar(255))",
        "CREATE TABLE test (id int NOT NULL , name varchar(255))"
    ),
    # Comment with escaped double quotes
    (
        "CREATE TABLE test (id int NOT NULL COMMENT \"Value can be: \"\"ACTIVE\"\" or \"\"INACTIVE\"\"\", name varchar(255))",
        "CREATE TABLE test (id int NOT NULL , name varchar(255))"
    ),
    # Multiple comments in same table
    (
        """CREATE TABLE test (
            id int NOT NULL COMMENT 'Primary key',
            name varchar(255) COMMENT 'User name',
            status enum('active','inactive') COMMENT 'Status with ''quotes'''
        )""",
        """CREATE TABLE test (
            id int NOT NULL ,
            name varchar(255) ,
            status enum('active','inactive') 
        )"""
    ),
    # Comment with COMMENT = syntax
    (
        "CREATE TABLE test (id int NOT NULL COMMENT = 'Primary key', name varchar(255))",
        "CREATE TABLE test (id int NOT NULL , name varchar(255))"
    ),
    # Comment with mixed quotes and special characters
    (
        "CREATE TABLE test (id int COMMENT 'Mixed: ''single'', \"double\", and `backtick`', name text)",
        "CREATE TABLE test (id int , name text)"
    ),
    # Multiline comment
    (
        """CREATE TABLE test (
            id int NOT NULL COMMENT 'This is a
            multiline comment
            with newlines',
            name varchar(255)
        )""",
        """CREATE TABLE test (
            id int NOT NULL ,
            name varchar(255)
        )"""
    ),
    # Comment with Unicode characters
    (
        "CREATE TABLE test (id int COMMENT '用户ID - 主键', name varchar(255) COMMENT 'Имя пользователя')",
        "CREATE TABLE test (id int , name varchar(255) )"
    ),
    # No comments (should remain unchanged)
    (
        "CREATE TABLE test (id int NOT NULL, name varchar(255))",
        "CREATE TABLE test (id int NOT NULL, name varchar(255))"
    ),
    # Comment at table level
    (
        "CREATE TABLE test (id int NOT NULL, name varchar(255)) COMMENT 'Table comment'",
        "CREATE TABLE test (id int NOT NULL, name varchar(255)) "
    ),
    # Complex case with multiple escaped quotes and special characters
    (
        """CREATE TABLE test (
            departments int(11) NOT NULL COMMENT '事件类型，可选值: ''SYSTEM'', ''BUSINESS''',
            termine int(11) NOT NULL COMMENT '事件类型，可选值: ''SYSTEM'', ''BUSINESS''',
            PRIMARY KEY (departments,termine)
        )""",
        """CREATE TABLE test (
            departments int(11) NOT NULL ,
            termine int(11) NOT NULL ,
            PRIMARY KEY (departments,termine)
        )"""
    ),
    # Comment with JSON-like content
    (
        "CREATE TABLE test (config json COMMENT '{\"type\": \"config\", \"values\": [\"a\", \"b\"]}', id int)",
        "CREATE TABLE test (config json , id int)"
    ),
    # Comment with SQL injection-like content (should be safely handled)
    (
        "CREATE TABLE test (id int COMMENT 'DROP TABLE users; --', name varchar(255))",
        "CREATE TABLE test (id int , name varchar(255))"
    ),
    # Empty comment
    (
        "CREATE TABLE test (id int COMMENT '', name varchar(255))",
        "CREATE TABLE test (id int , name varchar(255))"
    ),
    # Comment with only spaces
    (
        "CREATE TABLE test (id int COMMENT '   ', name varchar(255))",
        "CREATE TABLE test (id int , name varchar(255))"
    ),
    # Case insensitive COMMENT keyword
    (
        "CREATE TABLE test (id int comment 'lowercase', name varchar(255) Comment 'Mixed case')",
        "CREATE TABLE test (id int , name varchar(255) )"
    ),
    # Field named comment
    (
        """CREATE TABLE test (
            `departments int(11) NOT NULL,
            `termine int(11) NOT NULL,
            `comment` varchar(120) DEFAULT NULL,
            PRIMARY KEY (departments,termine)
        )""",
        """CREATE TABLE test (
            `departments int(11) NOT NULL,
            `termine int(11) NOT NULL,
            `comment` varchar(120) DEFAULT NULL,
            PRIMARY KEY (departments,termine)
        )"""
    ),
    # COMMENT keyword inside string literals (critical edge case)
    (
        "CREATE TABLE test (id int DEFAULT 'COMMENT test', name varchar(255))",
        "CREATE TABLE test (id int DEFAULT 'COMMENT test', name varchar(255))"
    ),
    # Unquoted column name 'comment' (critical edge case)
    (
        "CREATE TABLE test (comment varchar(255), id int)",
        "CREATE TABLE test (comment varchar(255), id int)"
    ),
    # COMMENT in DEFAULT with complex content (critical edge case)
    (
        "CREATE TABLE test (status varchar(50) DEFAULT 'COMMENT: active', id int)",
        "CREATE TABLE test (status varchar(50) DEFAULT 'COMMENT: active', id int)"
    ),
    # Multiple string literals containing COMMENT (critical edge case)
    (
        "CREATE TABLE test (col1 varchar(50) DEFAULT 'COMMENT 1', col2 varchar(50) DEFAULT 'COMMENT 2')",
        "CREATE TABLE test (col1 varchar(50) DEFAULT 'COMMENT 1', col2 varchar(50) DEFAULT 'COMMENT 2')"
    ),
])
def test_strip_comments_function(input_sql, expected_output):
    """
    Test the _strip_comments function with various realistic scenarios.
    
    This test covers:
    - Basic single and double quoted comments
    - Escaped quotes within comments (MySQL style with doubled quotes)
    - Multiple comments in the same table
    - COMMENT = syntax
    - Multiline comments with newlines
    - Unicode characters in comments
    - Table-level comments
    - Complex real-world scenarios
    - Edge cases like empty comments and case variations
    """
    from mysql_ch_replicator.converter import MysqlToClickhouseConverter
    
    converter = MysqlToClickhouseConverter()
    result = converter._strip_comments(input_sql)
    
    # Normalize whitespace for comparison (remove extra spaces that might be left behind)
    def normalize_whitespace(text):
        import re
        # Replace multiple spaces with single space, but preserve newlines
        return re.sub(r'[ \t]+', ' ', text).strip()
    
    assert normalize_whitespace(result) == normalize_whitespace(expected_output), f"Failed for input: {input_sql}"

