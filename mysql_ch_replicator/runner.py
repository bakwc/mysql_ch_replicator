import os
import time
import sys

from logging import getLogger

from .config import Settings
from .mysql_api import MySQLApi
from .utils import ProcessRunner, GracefulKiller

from . import db_replicator


logger = getLogger(__name__)



class BinlogReplicatorRunner(ProcessRunner):
    def __init__(self, config_file):
        super().__init__(f'{sys.argv[0]} --config {config_file} binlog_replicator')


class DbReplicatorRunner(ProcessRunner):
    def __init__(self, db_name, config_file):
        super().__init__(f'{sys.argv[0]} --config {config_file} --db {db_name} db_replicator')


class DbOptimizerRunner(ProcessRunner):
    def __init__(self, config_file):
        super().__init__(f'{sys.argv[0]} --config {config_file} db_optimizer')


class RunAllRunner(ProcessRunner):
    def __init__(self, db_name, config_file):
        super().__init__(f'{sys.argv[0]} --config {config_file} run_all --db {db_name}')


class Runner:
    def __init__(self, config: Settings, wait_initial_replication: bool, databases: str):
        self.config = config
        self.databases = databases or config.databases
        self.wait_initial_replication = wait_initial_replication
        self.runners: dict[str: DbReplicatorRunner] = {}
        self.binlog_runner = None
        self.db_optimizer = None

    def is_initial_replication_finished(self, db_name):
        state_path = os.path.join(
            self.config.binlog_replicator.data_dir,
            db_name,
            'state.pckl',
        )
        state = db_replicator.State(state_path)
        return state.status == db_replicator.Status.RUNNING_REALTIME_REPLICATION

    def restart_dead_processes(self):
        for runner in self.runners.values():
            runner.restart_dead_process_if_required()
        if self.binlog_runner is not None:
            self.binlog_runner.restart_dead_process_if_required()
        if self.db_optimizer is not None:
            self.db_optimizer.restart_dead_process_if_required()

    def check_databases_updated(self, mysql_api: MySQLApi):
        logger.debug('check if databases were created / removed in mysql')
        databases = mysql_api.get_databases()
        logger.info(f'mysql databases: {databases}')
        databases = [db for db in databases if self.config.is_database_matches(db)]
        logger.info(f'mysql databases filtered: {databases}')
        for db in databases:
            if db in self.runners:
                continue
            logger.info(f'running replication for {db} (database created in mysql)')
            runner = self.runners[db] = DbReplicatorRunner(db_name=db, config_file=self.config.settings_file)
            runner.run()

        for db in self.runners.keys():
            if db in databases:
                continue
            logger.info(f'stop replication for {db} (database removed from mysql)')
            self.runners[db].stop()
            self.runners.pop(db)

    def run(self):
        mysql_api = MySQLApi(
            database=None, mysql_settings=self.config.mysql,
        )
        databases = mysql_api.get_databases()
        databases = [db for db in databases if self.config.is_database_matches(db)]

        killer = GracefulKiller()

        self.binlog_runner = BinlogReplicatorRunner(self.config.settings_file)
        self.binlog_runner.run()

        self.db_optimizer = DbOptimizerRunner(self.config.settings_file)
        self.db_optimizer.run()

        # First - continue replication for DBs that already finished initial replication
        for db in databases:
            if not self.is_initial_replication_finished(db_name=db):
                continue
            logger.info(f'running replication for {db} (initial replication finished)')
            runner = self.runners[db] = DbReplicatorRunner(db_name=db, config_file=self.config.settings_file)
            runner.run()

        # Second - run replication for other DBs one by one and wait until initial replication finished
        for db in databases:
            if db in self.runners:
                continue

            logger.info(f'running replication for {db} (initial replication not finished - waiting)')
            runner = self.runners[db] = DbReplicatorRunner(db_name=db, config_file=self.config.settings_file)
            runner.run()
            if not self.wait_initial_replication:
                continue

            while not self.is_initial_replication_finished(db_name=db) and not killer.kill_now:
                time.sleep(1)
                self.restart_dead_processes()

        logger.info('all replicators launched')

        last_check_db_updated = time.time()
        while not killer.kill_now:
            time.sleep(1)
            self.restart_dead_processes()
            if time.time() - last_check_db_updated > self.config.check_db_updated_interval:
                self.check_databases_updated(mysql_api=mysql_api)
                last_check_db_updated = time.time()

        logger.info('stopping runner')

        if self.binlog_runner is not None:
            logger.info('stopping binlog replication')
            self.binlog_runner.stop()

        if self.db_optimizer is not None:
            logger.info('stopping db_optimizer')
            self.db_optimizer.stop()

        for db_name, db_replication_runner in self.runners.items():
            logger.info(f'stopping replication for {db_name}')
            db_replication_runner.stop()

        logger.info('stopped')
