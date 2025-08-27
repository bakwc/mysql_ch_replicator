"""Integration tests for schema evolution and DDL operations"""

import pytest

from tests.conftest import (
    CONFIG_FILE,
    TEST_DB_NAME,
    TEST_TABLE_NAME,
    TEST_TABLE_NAME_2,
    BinlogReplicatorRunner,
    DbReplicatorRunner,
    assert_wait,
)


@pytest.mark.integration
def test_add_column_first_after_and_drop_column(clean_environment):
    """Test adding columns with FIRST/AFTER and dropping columns"""
    cfg, mysql, ch = clean_environment

    mysql.execute(f"""
CREATE TABLE `{TEST_TABLE_NAME}` (
  `id` int NOT NULL,
  PRIMARY KEY (`id`)); 
    """)

    mysql.execute(
        f"INSERT INTO `{TEST_TABLE_NAME}` (id) VALUES (42)",
        commit=True,
    )

    binlog_replicator_runner = BinlogReplicatorRunner(
        cfg_file="tests/configs/replicator/tests_config_string_primary_key.yaml"
    )
    binlog_replicator_runner.run()
    db_replicator_runner = DbReplicatorRunner(
        TEST_DB_NAME,
        cfg_file="tests/configs/replicator/tests_config_string_primary_key.yaml",
    )
    db_replicator_runner.run()

    assert_wait(lambda: TEST_DB_NAME in ch.get_databases())

    ch.execute_command(f"USE `{TEST_DB_NAME}`")

    assert_wait(lambda: TEST_TABLE_NAME in ch.get_tables())
    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 1)

    # Test add column first
    mysql.execute(f"ALTER TABLE `{TEST_TABLE_NAME}` ADD COLUMN c1 INT FIRST")
    mysql.execute(
        f"INSERT INTO `{TEST_TABLE_NAME}` (id, c1) VALUES (43, 11)",
        commit=True,
    )
    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME, where="id=43")) == 1)
    assert_wait(lambda: ch.select(TEST_TABLE_NAME, where="id=43")[0]["c1"] == 11)

    # Test add column after
    mysql.execute(f"ALTER TABLE `{TEST_TABLE_NAME}` ADD COLUMN c2 INT AFTER c1")
    mysql.execute(
        f"INSERT INTO `{TEST_TABLE_NAME}` (id, c1, c2) VALUES (44, 111, 222)",
        commit=True,
    )
    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME, where="id=44")) == 1)
    assert_wait(lambda: ch.select(TEST_TABLE_NAME, where="id=44")[0]["c1"] == 111)
    assert_wait(lambda: ch.select(TEST_TABLE_NAME, where="id=44")[0]["c2"] == 222)

    # Test add KEY
    mysql.execute(f"ALTER TABLE `{TEST_TABLE_NAME}` ADD KEY `idx_c1_c2` (`c1`,`c2`)")
    mysql.execute(
        f"INSERT INTO `{TEST_TABLE_NAME}` (id, c1, c2) VALUES (46, 333, 444)",
        commit=True,
    )
    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME, where="id=46")) == 1)
    assert_wait(lambda: ch.select(TEST_TABLE_NAME, where="id=46")[0]["c1"] == 333)
    assert_wait(lambda: ch.select(TEST_TABLE_NAME, where="id=46")[0]["c2"] == 444)

    # Test drop column
    mysql.execute(f"ALTER TABLE `{TEST_TABLE_NAME}` DROP COLUMN c2")
    mysql.execute(
        f"INSERT INTO `{TEST_TABLE_NAME}` (id, c1) VALUES (45, 1111)",
        commit=True,
    )
    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME, where="id=45")) == 1)
    assert_wait(lambda: ch.select(TEST_TABLE_NAME, where="id=45")[0]["c1"] == 1111)
    assert_wait(lambda: ch.select(TEST_TABLE_NAME, where="id=45")[0].get("c2") is None)

    db_replicator_runner.stop()
    binlog_replicator_runner.stop()


@pytest.mark.integration
def test_create_table_like(clean_environment):
    """Test CREATE TABLE ... LIKE statements"""
    cfg, mysql, ch = clean_environment
    mysql.set_database(TEST_DB_NAME)

    # Create the source table with a complex structure
    mysql.execute("""
    CREATE TABLE `source_table` (
        id INT NOT NULL AUTO_INCREMENT,
        name VARCHAR(255) NOT NULL,
        age INT UNSIGNED,
        email VARCHAR(100) UNIQUE,
        status ENUM('active','inactive','pending') DEFAULT 'active',
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        data JSON,
        PRIMARY KEY (id)
    );
    """)

    # Create a table using LIKE statement
    mysql.execute("""
    CREATE TABLE `derived_table` LIKE `source_table`;
    """)

    # Set up replication
    binlog_replicator_runner = BinlogReplicatorRunner(cfg_file=CONFIG_FILE)
    binlog_replicator_runner.run()
    db_replicator_runner = DbReplicatorRunner(TEST_DB_NAME, cfg_file=CONFIG_FILE)
    db_replicator_runner.run()

    # Wait for database to be created and renamed from tmp to final
    assert_wait(lambda: TEST_DB_NAME in ch.get_databases(), max_wait_time=10.0)

    # Use the correct database explicitly
    ch.execute_command(f"USE `{TEST_DB_NAME}`")

    # Wait for tables to be created in ClickHouse with a longer timeout
    assert_wait(lambda: "source_table" in ch.get_tables(), max_wait_time=10.0)
    assert_wait(lambda: "derived_table" in ch.get_tables(), max_wait_time=10.0)

    # Insert data into both tables to verify they work
    mysql.execute(
        "INSERT INTO `source_table` (name, age, email, status) VALUES ('Alice', 30, 'alice@example.com', 'active');",
        commit=True,
    )
    mysql.execute(
        "INSERT INTO `derived_table` (name, age, email, status) VALUES ('Bob', 25, 'bob@example.com', 'pending');",
        commit=True,
    )

    # Wait for data to be replicated
    assert_wait(lambda: len(ch.select("source_table")) == 1, max_wait_time=10.0)
    assert_wait(lambda: len(ch.select("derived_table")) == 1, max_wait_time=10.0)

    # Compare structures by reading descriptions in ClickHouse
    source_desc = ch.execute_command("DESCRIBE TABLE source_table")
    derived_desc = ch.execute_command("DESCRIBE TABLE derived_table")

    # The structures should be identical
    assert source_desc == derived_desc

    # Verify the data in both tables
    source_data = ch.select("source_table")[0]
    derived_data = ch.select("derived_table")[0]

    assert source_data["name"] == "Alice"
    assert derived_data["name"] == "Bob"

    # Both tables should have same column types
    assert type(source_data["id"]) == type(derived_data["id"])
    assert type(source_data["name"]) == type(derived_data["name"])
    assert type(source_data["age"]) == type(derived_data["age"])

    # Clean up
    db_replicator_runner.stop()
    binlog_replicator_runner.stop()


@pytest.mark.integration
def test_if_exists_if_not_exists(clean_environment):
    """Test IF EXISTS and IF NOT EXISTS clauses in DDL"""
    cfg, mysql, ch = clean_environment

    binlog_replicator_runner = BinlogReplicatorRunner(
        cfg_file="tests/configs/replicator/tests_config_string_primary_key.yaml"
    )
    binlog_replicator_runner.run()
    db_replicator_runner = DbReplicatorRunner(
        TEST_DB_NAME,
        cfg_file="tests/configs/replicator/tests_config_string_primary_key.yaml",
    )
    db_replicator_runner.run()

    assert_wait(lambda: TEST_DB_NAME in ch.get_databases())

    mysql.execute(
        f"CREATE TABLE IF NOT EXISTS `{TEST_DB_NAME}`.`{TEST_TABLE_NAME}` (id int NOT NULL, PRIMARY KEY(id));"
    )
    mysql.execute(
        f"CREATE TABLE IF NOT EXISTS `{TEST_TABLE_NAME}` (id int NOT NULL, PRIMARY KEY(id));"
    )
    mysql.execute(
        f"CREATE TABLE IF NOT EXISTS `{TEST_DB_NAME}`.{TEST_TABLE_NAME_2} (id int NOT NULL, PRIMARY KEY(id));"
    )
    mysql.execute(
        f"CREATE TABLE IF NOT EXISTS {TEST_TABLE_NAME_2} (id int NOT NULL, PRIMARY KEY(id));"
    )
    mysql.execute(f"DROP TABLE IF EXISTS `{TEST_DB_NAME}`.{TEST_TABLE_NAME};")
    mysql.execute(f"DROP TABLE IF EXISTS {TEST_TABLE_NAME};")

    ch.execute_command(f"USE `{TEST_DB_NAME}`")

    assert_wait(lambda: TEST_TABLE_NAME_2 in ch.get_tables())
    assert_wait(lambda: TEST_TABLE_NAME not in ch.get_tables())

    db_replicator_runner.stop()
    binlog_replicator_runner.stop()


@pytest.mark.integration
def test_percona_migration(clean_environment):
    """Test Percona pt-online-schema-change style migration"""
    cfg, mysql, ch = clean_environment

    mysql.execute(f"""
CREATE TABLE `{TEST_TABLE_NAME}` (
  `id` int NOT NULL,
  PRIMARY KEY (`id`)); 
    """)

    mysql.execute(
        f"INSERT INTO `{TEST_TABLE_NAME}` (id) VALUES (42)",
        commit=True,
    )

    binlog_replicator_runner = BinlogReplicatorRunner(
        cfg_file="tests/configs/replicator/tests_config_string_primary_key.yaml"
    )
    binlog_replicator_runner.run()
    db_replicator_runner = DbReplicatorRunner(
        TEST_DB_NAME,
        cfg_file="tests/configs/replicator/tests_config_string_primary_key.yaml",
    )
    db_replicator_runner.run()

    assert_wait(lambda: TEST_DB_NAME in ch.get_databases())

    ch.execute_command(f"USE `{TEST_DB_NAME}`")

    assert_wait(lambda: TEST_TABLE_NAME in ch.get_tables())
    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 1)

    # Perform 'pt-online-schema-change' style migration to add a column
    mysql.execute(f"""
CREATE TABLE `{TEST_DB_NAME}`.`_{TEST_TABLE_NAME}_new` (
  `id` int NOT NULL,
  PRIMARY KEY (`id`)
)""")

    mysql.execute(
        f"ALTER TABLE `{TEST_DB_NAME}`.`_{TEST_TABLE_NAME}_new` ADD COLUMN c1 INT;"
    )

    mysql.execute(
        f"INSERT LOW_PRIORITY IGNORE INTO `{TEST_DB_NAME}`.`_{TEST_TABLE_NAME}_new` (`id`) SELECT `id` FROM `{TEST_DB_NAME}`.`{TEST_TABLE_NAME}` LOCK IN SHARE MODE;",
        commit=True,
    )

    mysql.execute(
        f"RENAME TABLE `{TEST_DB_NAME}`.`{TEST_TABLE_NAME}` TO `{TEST_DB_NAME}`.`_{TEST_TABLE_NAME}_old`, `{TEST_DB_NAME}`.`_{TEST_TABLE_NAME}_new` TO `{TEST_DB_NAME}`.`{TEST_TABLE_NAME}`;"
    )

    mysql.execute(f"DROP TABLE IF EXISTS `{TEST_DB_NAME}`.`_{TEST_TABLE_NAME}_old`;")

    # Wait for table to be recreated in ClickHouse after rename
    assert_wait(lambda: TEST_TABLE_NAME in ch.get_tables())

    mysql.execute(
        f"INSERT INTO `{TEST_TABLE_NAME}` (id, c1) VALUES (43, 1)",
        commit=True,
    )

    assert_wait(lambda: len(ch.select(TEST_TABLE_NAME)) == 2)

    db_replicator_runner.stop()
    binlog_replicator_runner.stop()
