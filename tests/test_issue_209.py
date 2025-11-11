import time

from mysql_ch_replicator import config
from mysql_ch_replicator import mysql_api
from mysql_ch_replicator import clickhouse_api

from common import *


def test_issue_209_bulk_insert():
    """
    Test for issue #209: Insertion cuts off after certain amount of records
    https://github.com/bakwc/mysql_ch_replicator/issues/209
    
    This test verifies that all records are properly replicated when inserting
    1000 records at once.
    """
    config_file = CONFIG_FILE_MARIADB

    cfg = config.Settings()
    cfg.load(config_file)

    mysql = mysql_api.MySQLApi(
        database=None,
        mysql_settings=cfg.mysql,
    )

    ch = clickhouse_api.ClickhouseApi(
        database=TEST_DB_NAME,
        clickhouse_settings=cfg.clickhouse,
    )

    prepare_env(cfg, mysql, ch)

    mysql.execute("RESET MASTER", commit=True)
    time.sleep(1)

    mysql.execute(f"""
CREATE TABLE `sites` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `user_id` int(11) DEFAULT NULL,
  `email` varchar(100) NOT NULL,
  `project_id` int(11) DEFAULT NULL,
  `kw` varchar(191) NOT NULL,
  `timestamp` timestamp NOT NULL DEFAULT current_timestamp(),
  `active` tinyint(1) NOT NULL DEFAULT 1,
  `deleted_at` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `active_idx` (`active`),
  KEY `sites_project_id_foreign` (`project_id`),
  KEY `sites_user_id_index` (`user_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_unicode_ci
""")

    run_all_runner = RunAllRunner(cfg_file=config_file)
    run_all_runner.run()

    assert_wait(lambda: TEST_DB_NAME in ch.get_databases())
    ch.execute_command(f'USE `{TEST_DB_NAME}`')
    assert_wait(lambda: 'sites' in ch.get_tables())

    mysql.execute("""
INSERT INTO sites (user_id, email, project_id, kw, timestamp, active, deleted_at)
WITH RECURSIVE seq (n) AS (
  SELECT 1
  UNION ALL
  SELECT n + 1 FROM seq WHERE n < 1000
)
SELECT
  12345 AS user_id,
  'test@example.com' AS email,
  12345 AS project_id,
  concat('Keyword ', n) AS kw,
  now() AS timestamp,
  1 AS active,
  NULL AS deleted_at
FROM seq;
""", commit=True)

    mysql.execute("SELECT COUNT(*) FROM sites")
    mysql_count = mysql.cursor.fetchall()[0][0]
    assert mysql_count == 1000, f"Expected 1000 records in MySQL, got {mysql_count}"

    assert_wait(lambda: len(ch.select('sites')) == 1000, max_wait_time=10.0)
    
    ch_count = len(ch.select('sites'))
    assert ch_count == 1000, f"Expected 1000 records in ClickHouse, got {ch_count}"

    run_all_runner.stop()

    assert_wait(lambda: 'stopping db_replicator' in read_logs(TEST_DB_NAME))
    assert 'Traceback' not in read_logs(TEST_DB_NAME)


def test_issue_209_bulk_update():
    """
    Test for issue #209: Bulk UPDATE operations not replicating properly
    
    This test reproduces the specific issue reported where UPDATE operations
    on MariaDB split into multiple UpdateRowsEvent objects with same position,
    causing events to be processed repeatedly.
    """
    config_file = CONFIG_FILE_MARIADB

    cfg = config.Settings()
    cfg.load(config_file)

    mysql = mysql_api.MySQLApi(
        database=None,
        mysql_settings=cfg.mysql,
    )

    ch = clickhouse_api.ClickhouseApi(
        database=TEST_DB_NAME,
        clickhouse_settings=cfg.clickhouse,
    )

    prepare_env(cfg, mysql, ch)

    mysql.execute("RESET MASTER", commit=True)
    time.sleep(1)

    mysql.execute(f"""
CREATE TABLE `sites` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `user_id` int(11) DEFAULT NULL,
  `email` varchar(100) NOT NULL,
  `project_id` int(11) DEFAULT NULL,
  `kw` varchar(191) NOT NULL,
  `timestamp` timestamp NOT NULL DEFAULT current_timestamp(),
  `active` tinyint(1) NOT NULL DEFAULT 1,
  `deleted_at` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `active_idx` (`active`),
  KEY `sites_project_id_foreign` (`project_id`),
  KEY `sites_user_id_index` (`user_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_unicode_ci
""")

    run_all_runner = RunAllRunner(cfg_file=config_file)
    run_all_runner.run()

    assert_wait(lambda: TEST_DB_NAME in ch.get_databases())
    ch.execute_command(f'USE `{TEST_DB_NAME}`')
    assert_wait(lambda: 'sites' in ch.get_tables())

    for batch in range(5):
        offset = batch * 1000
        mysql.execute(f"""
INSERT INTO sites (user_id, email, project_id, kw, timestamp, active, deleted_at)
WITH RECURSIVE seq (n) AS (
  SELECT 1
  UNION ALL
  SELECT n + 1 FROM seq WHERE n < 1000
)
SELECT
  12345 AS user_id,
  'test@example.com' AS email,
  12345 AS project_id,
  concat('Keyword ', n + {offset}) AS kw,
  now() AS timestamp,
  0 AS active,
  NULL AS deleted_at
FROM seq;
""", commit=True)

    mysql.execute("SELECT COUNT(*) FROM sites WHERE active = 0")
    mysql_count = mysql.cursor.fetchall()[0][0]
    assert mysql_count == 5000, f"Expected 5000 inactive records in MySQL, got {mysql_count}"

    assert_wait(lambda: len(ch.select('sites')) == 5000, max_wait_time=15.0)

    mysql.execute("UPDATE sites SET active = 1 WHERE active = 0", commit=True)

    mysql.execute("SELECT COUNT(*) FROM sites WHERE active = 1")
    mysql_count_active = mysql.cursor.fetchall()[0][0]
    assert mysql_count_active == 5000, f"Expected 5000 active records in MySQL, got {mysql_count_active}"

    def check_all_active():
        result = ch.select('sites')
        active_count = sum(1 for row in result if row['active'] == 1)
        return active_count == 5000

    assert_wait(check_all_active, max_wait_time=30.0)

    result = ch.select('sites')
    active_count = sum(1 for row in result if row['active'] == 1)
    assert active_count == 5000, f"Expected 5000 active records in ClickHouse, got {active_count}"

    run_all_runner.stop()

    assert_wait(lambda: 'stopping db_replicator' in read_logs(TEST_DB_NAME))
    assert 'Traceback' not in read_logs(TEST_DB_NAME)

