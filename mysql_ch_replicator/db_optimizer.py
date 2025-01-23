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
        )
        self.clickhouse_api = ClickhouseApi(
            database=None,
            clickhouse_settings=config.clickhouse,
        )

    def select_db_to_optimize(self):
        databases = self.mysql_api.get_databases()
        databases = [db for db in databases if self.config.is_database_matches(db)]
        ch_databases = set(self.clickhouse_api.get_databases())

        for db in databases:
            if db not in ch_databases:
                continue
            last_process_time = self.state.last_process_time.get(db, 0.0)
            if time.time() - last_process_time < self.config.optimize_interval:
                continue
            return db
        return None

    def optimize_table(self, db_name, table_name):
        logger.info(f'Optimizing table {db_name}.{table_name}')
        t1 = time.time()
        self.clickhouse_api.execute_command(
            f'OPTIMIZE TABLE `{db_name}`.`{table_name}` FINAL SETTINGS mutations_sync = 2'
        )
        t2 = time.time()
        logger.info(f'Optimize finished in {int(t2-t1)} seconds')

    def optimize_database(self, db_name):
        self.mysql_api.set_database(db_name)
        tables = self.mysql_api.get_tables()
        self.mysql_api.close()
        tables = [table for table in tables if self.config.is_table_matches(table)]

        self.clickhouse_api.execute_command(f'USE `{db_name}`')
        ch_tables = set(self.clickhouse_api.get_tables())

        for table in tables:
            if table not in ch_tables:
                continue
            self.optimize_table(db_name, table)
        self.state.last_process_time[db_name] = time.time()
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
                self.optimize_database(db_name=db_to_optimize)
        except Exception as e:
            logger.error(f'error {e}', exc_info=True)
