import pickle
import os
import time
from logging import getLogger

from .config import Settings
from .mysql_api import MySQLApi
from .clickhouse_api import ClickhouseApi
from .utils import RegularKiller


logger = getLogger(__name__)


class State:

    def __init__(self, file_name):
        self.file_name = file_name
        self.last_process_time = {}
        self.load()

    def load(self):
        file_name = self.file_name
        if not os.path.exists(file_name):
            return
        data = open(file_name, 'rb').read()
        data = pickle.loads(data)
        self.last_process_time = data['last_process_time']

    def save(self):
        file_name = self.file_name
        data = pickle.dumps({
            'last_process_time': self.last_process_time,
        })
        with open(file_name + '.tmp', 'wb') as f:
            f.write(data)
        os.rename(file_name + '.tmp', file_name)


class DbOptimizer:
    def __init__(self, config: Settings):
        self.state = State(os.path.join(
            config.binlog_replicator.data_dir,
            'db_optimizer.bin',
        ))
        self.config = config
        self.mysql_api = MySQLApi(
            database=None,
            mysql_settings=config.mysql,
            mysql_timezone=config.mysql_timezone,
        )
        self.clickhouse_api = ClickhouseApi(
            database=None,
            clickhouse_settings=config.clickhouse,
        )

    def get_clickhouse_database(self, mysql_db_name: str) -> str:
        return self.config.target_databases.get(mysql_db_name, mysql_db_name)

    def select_db_to_optimize(self):
        databases = self.mysql_api.get_databases()
        databases = [db for db in databases if self.config.is_database_matches(db)]
        ch_databases = set(self.clickhouse_api.get_databases())

        for mysql_db in databases:
            ch_db = self.get_clickhouse_database(mysql_db)
            if ch_db not in ch_databases:
                continue
            last_process_time = self.state.last_process_time.get(mysql_db, 0.0)
            if time.time() - last_process_time < self.config.optimize_interval:
                continue
            return mysql_db
        return None

    def optimize_table(self, ch_db_name, table_name):
        logger.info(f'Optimizing table {ch_db_name}.{table_name}')
        t1 = time.time()
        on_cluster = self.clickhouse_api.get_on_cluster_clause()
        optimize_final = 'FINAL' if self.config.enable_optimize_final else ''
        self.clickhouse_api.execute_command(
            f'OPTIMIZE TABLE `{ch_db_name}`.`{table_name}` {on_cluster} {optimize_final}  SETTINGS mutations_sync = 2, alter_sync = 2'
        )
        t2 = time.time()
        logger.info(f'Optimize finished in {int(t2-t1)} seconds')

    def optimize_database(self, mysql_db_name):
        ch_db_name = self.get_clickhouse_database(mysql_db_name)
        self.mysql_api.set_database(mysql_db_name)
        tables = self.mysql_api.get_tables()
        self.mysql_api.close()
        tables = [table for table in tables if self.config.is_table_matches(table)]

        self.clickhouse_api.database = ch_db_name
        self.clickhouse_api.execute_command(f'USE `{ch_db_name}`')
        ch_tables = self.clickhouse_api.get_optimizable_tables(ch_db_name)

        for table in tables:
            ch_table_name = self.config.get_target_table_name(mysql_db_name, table)
            if ch_table_name not in ch_tables:
                logger.debug(
                    f'skip optimize for {ch_db_name}.{ch_table_name}: '
                    f'not an optimizable table in ClickHouse (view or missing)',
                )
                continue
            try:
                self.optimize_table(ch_db_name, ch_table_name)
            except Exception as e:
                logger.warning(
                    f'failed to optimize {ch_db_name}.{ch_table_name}, skipping: {e}',
                    exc_info=True,
                )

        self.state.last_process_time[mysql_db_name] = time.time()
        self.state.save()

    def run(self):
        logger.info('running optimizer')
        RegularKiller('optimizer')
        try:
            while True:
                db_to_optimize = self.select_db_to_optimize()
                self.mysql_api.close()
                if db_to_optimize is None:
                    time.sleep(min(120, self.config.optimize_interval))
                    continue
                self.optimize_database(mysql_db_name=db_to_optimize)
        except Exception as e:
            logger.error(f'error {e}', exc_info=True)
