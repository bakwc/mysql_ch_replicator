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


class RunAllRunner(ProcessRunner):
    def __init__(self, db_name, config_file):
        super().__init__(f'{sys.argv[0]} --config {config_file} run_all --db {db_name}')


class Runner:
    def __init__(self, config: Settings, wait_initial_replication: bool, databases: str):
        self.config = config
        self.databases = databases or config.databases
        self.wait_initial_replication = wait_initial_replication
        self.runners: dict = {}
        self.binlog_runner = None

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

    def run(self):
        mysql_api = MySQLApi(
            database=None, mysql_settings=self.config.mysql,
        )
        databases = mysql_api.get_databases()
        databases = [db for db in databases if self.config.is_database_matches(db)]

        killer = GracefulKiller()

        self.binlog_runner = BinlogReplicatorRunner(self.config.settings_file)
        self.binlog_runner.run()

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

        while not killer.kill_now:
            time.sleep(1)
            self.restart_dead_processes()

        logger.info('stopping runner')

        if self.binlog_runner is not None:
            logger.info('stopping binlog replication')
            self.binlog_runner.stop()

        for db_name, db_replication_runner in self.runners.items():
            logger.info(f'stopping replication for {db_name}')
            db_replication_runner.stop()

        logger.info('stopped')
