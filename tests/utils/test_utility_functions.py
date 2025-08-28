"""Unit tests for utility and parser functions"""

import pytest

from mysql_ch_replicator.binlog_replicator import BinlogReplicator
from mysql_ch_replicator.converter import MysqlToClickhouseConverter


@pytest.mark.unit
def test_parse_mysql_table_structure():
    """Test parsing MySQL table structure from CREATE TABLE statement"""
    query = "CREATE TABLE IF NOT EXISTS user_preferences_portal (\n\t\t\tid char(36) NOT NULL,\n\t\t\tcategory varchar(50) DEFAULT NULL,\n\t\t\tdeleted tinyint(1) DEFAULT 0,\n\t\t\tdate_entered datetime DEFAULT NULL,\n\t\t\tdate_modified datetime DEFAULT NULL,\n\t\t\tassigned_user_id char(36) DEFAULT NULL,\n\t\t\tcontents longtext DEFAULT NULL\n\t\t ) ENGINE=InnoDB DEFAULT CHARSET=utf8"

    converter = MysqlToClickhouseConverter()

    structure = converter.parse_mysql_table_structure(query)

    assert structure.table_name == "user_preferences_portal"


@pytest.mark.unit
@pytest.mark.parametrize(
    "query,expected",
    [
        ("CREATE TABLE `mydb`.`mytable` (id INT)", "mydb"),
        ("CREATE TABLE mydb.mytable (id INT)", "mydb"),
        ("ALTER TABLE `mydb`.mytable ADD COLUMN name VARCHAR(50)", "mydb"),
        ("CREATE TABLE IF NOT EXISTS mydb.mytable (id INT)", "mydb"),
        ("CREATE TABLE mytable (id INT)", ""),
        ("  CREATE   TABLE    `mydb`   .   `mytable` \n ( id INT )", "mydb"),
        ('ALTER TABLE "testdb"."tablename" ADD COLUMN flag BOOLEAN', "testdb"),
        ("create table mydb.mytable (id int)", "mydb"),
        ("DROP DATABASE mydb", ""),
        ("CREATE TABLE mydbmytable (id int)", ""),  # missing dot between DB and table
        (
            """
        CREATE TABLE IF NOT EXISTS
        `multidb`
        .
        `multitable`
        (
          id INT,
          name VARCHAR(100)
        )
    """,
            "multidb",
        ),
        (
            """
        ALTER TABLE
        `justtable`
        ADD COLUMN age INT;
    """,
            "",
        ),
        (
            """
    CREATE TABLE `replication-test_db`.`test_table_2` (
        `id` int unsigned NOT NULL AUTO_INCREMENT,
        name varchar(255),
        PRIMARY KEY (id)
    )
    """,
            "replication-test_db",
        ),
        ("BEGIN", ""),
    ],
)
def test_parse_db_name_from_query(query, expected):
    """Test parsing database name from SQL queries"""
    assert BinlogReplicator._try_parse_db_name_from_query(query) == expected


@pytest.mark.unit
def test_alter_tokens_split():
    """Test ALTER TABLE token splitting functionality"""
    examples = [
        # basic examples from the prompt:
        ("test_name VARCHAR(254) NULL", ["test_name", "VARCHAR(254)", "NULL"]),
        (
            "factor NUMERIC(5, 2) DEFAULT NULL",
            ["factor", "NUMERIC(5, 2)", "DEFAULT", "NULL"],
        ),
        # backquoted column name:
        ("`test_name` VARCHAR(254) NULL", ["`test_name`", "VARCHAR(254)", "NULL"]),
        ("`order` INT NOT NULL", ["`order`", "INT", "NOT", "NULL"]),
        # type that contains a parenthesized list with quoted values:
        (
            "status ENUM('active','inactive') DEFAULT 'active'",
            ["status", "ENUM('active','inactive')", "DEFAULT", "'active'"],
        ),
        # multi‐word type definitions:
        ("col DOUBLE PRECISION DEFAULT 0", ["col", "DOUBLE PRECISION", "DEFAULT", "0"]),
        ("col INT UNSIGNED DEFAULT 0", ["col", "INT UNSIGNED", "DEFAULT", "0"]),
        # a case with a quoted string containing spaces and punctuation:
        (
            "message VARCHAR(100) DEFAULT 'Hello, world!'",
            ["message", "VARCHAR(100)", "DEFAULT", "'Hello, world!'"],
        ),
        # longer definition with more options:
        (
            "col DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
            [
                "col",
                "DATETIME",
                "DEFAULT",
                "CURRENT_TIMESTAMP",
                "ON",
                "UPDATE",
                "CURRENT_TIMESTAMP",
            ],
        ),
        # type with a COMMENT clause (here the type is given, then a parameter keyword)
        (
            "col VARCHAR(100) COMMENT 'This is a test comment'",
            ["col", "VARCHAR(100)", "COMMENT", "'This is a test comment'"],
        ),
        ("c1 INT FIRST", ["c1", "INT", "FIRST"]),
    ]

    for sql, expected in examples:
        result = MysqlToClickhouseConverter._tokenize_alter_query(sql)
        print("SQL Input:  ", sql)
        print("Expected:   ", expected)
        print("Tokenized:  ", result)
        print("Match?     ", result == expected)
        print("-" * 60)
        assert result == expected


@pytest.mark.integration
def test_issue_160_unknown_mysql_type_bug():
    """
    Test to reproduce the bug from issue #160.

    Bug Description: Replication fails when adding a new table during realtime replication
    with Exception: unknown mysql type ""

    This test should FAIL until the bug is fixed.
    When the bug is present: parsing will fail with unknown mysql type and the test will FAIL
    When the bug is fixed: parsing will succeed and the test will PASS
    """
    # The exact CREATE TABLE statement from the bug report
    create_table_query = """create table test_table
(
    id    bigint          not null,
    col_a datetime(6)     not null,
    col_b datetime(6)     null,
    col_c varchar(255)    not null,
    col_d varchar(255)    not null,
    col_e int             not null,
    col_f decimal(20, 10) not null,
    col_g decimal(20, 10) not null,
    col_h datetime(6)     not null,
    col_i date            not null,
    col_j varchar(255)    not null,
    col_k varchar(255)    not null,
    col_l bigint          not null,
    col_m varchar(50)     not null,
    col_n bigint          null,
    col_o decimal(20, 1)  null,
    col_p date            null,
    primary key (id, col_e)
);"""

    # Create a converter instance
    converter = MysqlToClickhouseConverter()

    # This should succeed when the bug is fixed
    # When the bug is present, this will raise "unknown mysql type """ and the test will FAIL
    mysql_structure, ch_structure = converter.parse_create_table_query(
        create_table_query
    )

    # Verify the parsing worked correctly
    assert mysql_structure.table_name == "test_table"
    assert len(mysql_structure.fields) == 17  # All columns should be parsed
    assert mysql_structure.primary_keys == ["id", "col_e"]
