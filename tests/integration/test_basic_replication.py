"""Integration tests for basic replication functionality"""

import pytest

from tests.conftest import (
    CONFIG_FILE,
    CONFIG_FILE_MARIADB,
    TEST_DB_NAME,
    TEST_TABLE_NAME,
    TEST_TABLE_NAME_2,
    TEST_TABLE_NAME_3,
    BinlogReplicatorRunner,
    DbReplicatorRunner,
    assert_wait,
)


@pytest.mark.integration
@pytest.mark.parametrize(
    "dynamic_config", [CONFIG_FILE, CONFIG_FILE_MARIADB], indirect=True
)
def test_e2e_regular(dynamic_clean_environment, dynamic_config):
    """Test end-to-end replication with regular operations"""
    cfg, mysql, ch = dynamic_clean_environment
    config_file = getattr(cfg, "config_file", CONFIG_FILE)

    mysql.execute(f"""
CREATE TABLE `{TEST_TABLE_NAME}` (
    id int NOT NULL AUTO_INCREMENT,
    name varchar(255) COMMENT 'Dân tộc, ví dụ: Kinh',
    age int COMMENT 'CMND Cũ',
    field1 text,
    field2 blob,
    PRIMARY KEY (id)
); 
    """)

    mysql.execute(
        f"INSERT INTO `{TEST_TABLE_NAME}` (name, age, field1, field2) VALUES ('Ivan', 42, 'test1', 'test2');",
        commit=True,
    )
    mysql.execute(
        f"INSERT INTO `{TEST_TABLE_NAME}` (name, age) VALUES ('Peter', 33);",
        commit=True,
    )

    binlog_replicator_runner = BinlogReplicatorRunner(cfg_file=config_file)
    binlog_replicator_runner.run()
    db_replicator_runner = DbReplicatorRunner(TEST_DB_NAME, cfg_file=config_file)
    db_replicator_runner.run()

    assert_wait(lambda: TEST_DB_NAME in ch.get_databases())

    ch.execute_command(f"USE `{TEST_DB_NAME}`")

    assert_wait(lambda: TEST_TABLE_NAME in ch.get_tables())
    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 2)

    # Check for custom partition_by configuration when using CONFIG_FILE_MARIADB (tests_config_mariadb.yaml)
    if config_file == CONFIG_FILE_MARIADB:
        create_query = ch.show_create_table(TEST_TABLE_NAME)
        assert "PARTITION BY intDiv(id, 1000000)" in create_query, (
            f"Custom partition_by not found in CREATE TABLE query: {create_query}"
        )

    mysql.execute(
        f"INSERT INTO `{TEST_TABLE_NAME}` (name, age) VALUES ('Filipp', 50);",
        commit=True,
    )
    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 3)
    assert_wait(
        lambda: ch.select(TEST_TABLE_NAME, where="name='Filipp'")[0]["age"] == 50
    )

    mysql.execute(f"ALTER TABLE `{TEST_TABLE_NAME}` ADD `last_name` varchar(255); ")
    mysql.execute(
        f"ALTER TABLE `{TEST_TABLE_NAME}` ADD `price` decimal(10,2) DEFAULT NULL; "
    )

    mysql.execute(f"ALTER TABLE `{TEST_TABLE_NAME}` ADD UNIQUE INDEX prise_idx (price)")
    mysql.execute(
        f"ALTER TABLE `{TEST_TABLE_NAME}` DROP INDEX prise_idx, ADD UNIQUE INDEX age_idx (age)"
    )

    mysql.execute(
        f"INSERT INTO `{TEST_TABLE_NAME}` (name, age, last_name, price) VALUES ('Mary', 24, 'Smith', 3.2);",
        commit=True,
    )

    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 4)
    assert_wait(
        lambda: ch.select(TEST_TABLE_NAME, where="name='Mary'")[0]["last_name"]
        == "Smith"
    )

    assert_wait(
        lambda: ch.select(TEST_TABLE_NAME, where="field1='test1'")[0]["name"] == "Ivan"
    )
    assert_wait(
        lambda: ch.select(TEST_TABLE_NAME, where="field2='test2'")[0]["name"] == "Ivan"
    )

    mysql.execute(
        f"ALTER TABLE `{TEST_DB_NAME}`.`{TEST_TABLE_NAME}` "
        f"ADD COLUMN country VARCHAR(25) DEFAULT '' NOT NULL AFTER name;"
    )

    mysql.execute(
        f"INSERT INTO `{TEST_TABLE_NAME}` (name, age, last_name, country) "
        f"VALUES ('John', 12, 'Doe', 'USA');",
        commit=True,
    )

    mysql.execute(
        f"ALTER TABLE `{TEST_DB_NAME}`.`{TEST_TABLE_NAME}` "
        f"CHANGE COLUMN country origin VARCHAR(24) DEFAULT '' NOT NULL",
    )

    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 5)
    assert_wait(
        lambda: ch.select(TEST_TABLE_NAME, where="name='John'")[0].get("origin")
        == "USA"
    )

    mysql.execute(
        f"ALTER TABLE `{TEST_DB_NAME}`.`{TEST_TABLE_NAME}` "
        f"CHANGE COLUMN origin country VARCHAR(24) DEFAULT '' NOT NULL",
    )
    assert_wait(
        lambda: ch.select(TEST_TABLE_NAME, where="name='John'")[0].get("origin") is None
    )
    assert_wait(
        lambda: ch.select(TEST_TABLE_NAME, where="name='John'")[0].get("country")
        == "USA"
    )

    mysql.execute(
        f"ALTER TABLE `{TEST_DB_NAME}`.`{TEST_TABLE_NAME}` DROP COLUMN country"
    )
    assert_wait(
        lambda: ch.select(TEST_TABLE_NAME, where="name='John'")[0].get("country")
        is None
    )

    assert_wait(
        lambda: ch.select(TEST_TABLE_NAME, where="name='Filipp'")[0].get("last_name")
        is None
    )

    mysql.execute(
        f"UPDATE `{TEST_TABLE_NAME}` SET last_name = '' WHERE last_name IS NULL;"
    )
    mysql.execute(
        f"ALTER TABLE `{TEST_TABLE_NAME}` MODIFY `last_name` varchar(1024) NOT NULL"
    )

    assert_wait(
        lambda: ch.select(TEST_TABLE_NAME, where="name='Filipp'")[0].get("last_name")
        == ""
    )

    mysql.execute(f"""
    CREATE TABLE {TEST_TABLE_NAME_2} (
        id int NOT NULL AUTO_INCREMENT,
        name varchar(255),
        age int,
        PRIMARY KEY (id)
    ); 
        """)

    assert_wait(lambda: TEST_TABLE_NAME_2 in ch.get_tables())

    mysql.execute(
        f"INSERT INTO `{TEST_TABLE_NAME_2}` (name, age) VALUES ('Ivan', 42);",
        commit=True,
    )
    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME_2)) == 1)

    mysql.execute(f"""
    CREATE TABLE `{TEST_TABLE_NAME_3}` (
        id int NOT NULL AUTO_INCREMENT,
        `name` varchar(255),
        age int,
        PRIMARY KEY (`id`)
    ); 
        """)

    assert_wait(lambda: TEST_TABLE_NAME_3 in ch.get_tables())

    mysql.execute(
        f"INSERT INTO `{TEST_TABLE_NAME_3}` (name, `age`) VALUES ('Ivan', 42);",
        commit=True,
    )
    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME_3)) == 1)

    mysql.execute(f"DROP TABLE `{TEST_TABLE_NAME_3}`")
    assert_wait(lambda: TEST_TABLE_NAME_3 not in ch.get_tables())

    db_replicator_runner.stop()


@pytest.mark.integration
def test_e2e_multistatement(clean_environment):
    """Test end-to-end replication with multi-statement operations"""
    cfg, mysql, ch = clean_environment

    mysql.execute(f"""
CREATE TABLE `{TEST_TABLE_NAME}` (
    id int NOT NULL AUTO_INCREMENT,
    name varchar(255),
    age int,
    PRIMARY KEY (id, `name`)
); 
    """)

    mysql.execute(
        f"INSERT INTO `{TEST_TABLE_NAME}` (name, age) VALUES ('Ivan', 42);", commit=True
    )

    binlog_replicator_runner = BinlogReplicatorRunner()
    binlog_replicator_runner.run()
    db_replicator_runner = DbReplicatorRunner(TEST_DB_NAME)
    db_replicator_runner.run()

    assert_wait(lambda: TEST_DB_NAME in ch.get_databases())

    ch.execute_command(f"USE `{TEST_DB_NAME}`")

    assert_wait(lambda: TEST_TABLE_NAME in ch.get_tables())
    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 1)

    mysql.execute(
        f"ALTER TABLE `{TEST_TABLE_NAME}` ADD `last_name` varchar(255), ADD COLUMN city varchar(255); "
    )
    mysql.execute(
        f"INSERT INTO `{TEST_TABLE_NAME}` (name, age, last_name, city) "
        f"VALUES ('Mary', 24, 'Smith', 'London');",
        commit=True,
    )

    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 2)
    assert_wait(
        lambda: ch.select(TEST_TABLE_NAME, where="name='Mary'")[0].get("last_name")
        == "Smith"
    )
    assert_wait(
        lambda: ch.select(TEST_TABLE_NAME, where="name='Mary'")[0].get("city")
        == "London"
    )

    mysql.execute(
        f"ALTER TABLE `{TEST_TABLE_NAME}` DROP COLUMN last_name, DROP COLUMN city"
    )
    assert_wait(
        lambda: ch.select(TEST_TABLE_NAME, where="name='Mary'")[0].get("last_name")
        is None
    )
    assert_wait(
        lambda: ch.select(TEST_TABLE_NAME, where="name='Mary'")[0].get("city") is None
    )

    mysql.execute(f"DELETE FROM `{TEST_TABLE_NAME}` WHERE name='Ivan';", commit=True)
    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 1)

    mysql.execute(
        f"ALTER TABLE `{TEST_TABLE_NAME}` ADD factor NUMERIC(5, 2) DEFAULT NULL;"
    )
    mysql.execute(
        f"INSERT INTO `{TEST_TABLE_NAME}` (name, age, factor) VALUES ('Snow', 31, 13.29);",
        commit=True,
    )

    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 2)
    import decimal

    assert_wait(
        lambda: ch.select(TEST_TABLE_NAME, where="name='Snow'")[0].get("factor")
        == decimal.Decimal("13.29")
    )

    mysql.execute(
        f"CREATE TABLE {TEST_TABLE_NAME_2} "
        f"(id int NOT NULL AUTO_INCREMENT, name varchar(255), age int, "
        f"PRIMARY KEY (id));"
    )

    assert_wait(lambda: TEST_TABLE_NAME_2 in ch.get_tables())

    db_replicator_runner.stop()
    binlog_replicator_runner.stop()


@pytest.mark.integration
def test_initial_only(clean_environment):
    """Test initial-only replication mode"""
    cfg, mysql, ch = clean_environment

    mysql.execute(f"""
CREATE TABLE `{TEST_TABLE_NAME}` (
    id int NOT NULL AUTO_INCREMENT,
    name varchar(255),
    age int,
    PRIMARY KEY (id)
); 
    """)

    mysql.execute(
        f"INSERT INTO `{TEST_TABLE_NAME}` (name, age) VALUES ('Ivan', 42);", commit=True
    )
    mysql.execute(
        f"INSERT INTO `{TEST_TABLE_NAME}` (name, age) VALUES ('Peter', 33);",
        commit=True,
    )

    db_replicator_runner = DbReplicatorRunner(
        TEST_DB_NAME, additional_arguments="--initial_only=True"
    )
    db_replicator_runner.run()
    db_replicator_runner.wait_complete()

    assert TEST_DB_NAME in ch.get_databases()

    ch.execute_command(f"USE `{TEST_DB_NAME}`")

    assert TEST_TABLE_NAME in ch.get_tables()
    assert len(ch.select(TEST_TABLE_NAME)) == 2

    ch.execute_command(f"DROP DATABASE `{TEST_DB_NAME}`")

    db_replicator_runner.stop()

    db_replicator_runner = DbReplicatorRunner(
        TEST_DB_NAME, additional_arguments="--initial_only=True"
    )
    db_replicator_runner.run()
    db_replicator_runner.wait_complete()
    assert TEST_DB_NAME in ch.get_databases()

    db_replicator_runner.stop()
