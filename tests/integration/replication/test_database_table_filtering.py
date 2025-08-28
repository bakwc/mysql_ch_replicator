"""Integration test for database/table filtering include/exclude patterns"""

import pytest

from tests.conftest import (
    RunAllRunner,
    assert_wait,
    prepare_env,
)


@pytest.mark.integration
def test_database_tables_filtering(clean_environment):
    cfg, mysql, ch = clean_environment
    cfg_file = "tests/configs/replicator/tests_config_databases_tables.yaml"
    cfg.load(cfg_file)

    # Prepare MySQL and ClickHouse state
    mysql.drop_database("test_db_3")
    mysql.drop_database("test_db_12")
    mysql.create_database("test_db_3")
    mysql.create_database("test_db_12")
    ch.drop_database("test_db_3")
    ch.drop_database("test_db_12")

    # Prepare env for test_db_2 (target DB for inclusion)
    prepare_env(cfg, mysql, ch, db_name="test_db_2")

    # Create multiple tables in test_db_2
    mysql.execute(
        """
        CREATE TABLE test_table_15 (
            id int NOT NULL AUTO_INCREMENT,
            name varchar(255),
            age int,
            PRIMARY KEY (id)
        );
        """
    )
    mysql.execute(
        """
        CREATE TABLE test_table_142 (
            id int NOT NULL AUTO_INCREMENT,
            name varchar(255),
            age int,
            PRIMARY KEY (id)
        );
        """
    )
    mysql.execute(
        """
        CREATE TABLE test_table_143 (
            id int NOT NULL AUTO_INCREMENT,
            name varchar(255),
            age int,
            PRIMARY KEY (id)
        );
        """
    )
    mysql.execute(
        """
        CREATE TABLE test_table_3 (
            id int NOT NULL AUTO_INCREMENT,
            name varchar(255),
            age int,
            PRIMARY KEY (id)
        );
        """
    )
    mysql.execute(
        """
        CREATE TABLE test_table_2 (
            id int NOT NULL AUTO_INCREMENT,
            name varchar(255),
            age int,
            PRIMARY KEY (id)
        );
        """
    )

    # Seed a bit of data
    mysql.execute(
        "INSERT INTO test_table_3 (name, age) VALUES ('Ivan', 42);",
        commit=True,
    )
    mysql.execute(
        "INSERT INTO test_table_2 (name, age) VALUES ('Ivan', 42);",
        commit=True,
    )

    # Run replication with filter config
    runner = RunAllRunner(cfg_file=cfg_file)
    runner.run()

    # Verify databases
    assert_wait(lambda: "test_db_2" in ch.get_databases())
    assert "test_db_3" not in ch.get_databases()
    assert "test_db_12" not in ch.get_databases()

    ch.execute_command("USE test_db_2")

    # Included tables
    assert_wait(lambda: "test_table_2" in ch.get_tables())
    assert_wait(lambda: len(ch.select("test_table_2")) == 1)
    assert_wait(lambda: "test_table_143" in ch.get_tables())

    # Excluded tables
    assert "test_table_3" not in ch.get_tables()
    assert "test_table_15" not in ch.get_tables()
    assert "test_table_142" not in ch.get_tables()

    runner.stop()
