import os.path
import time
import pickle
import hashlib
from logging import getLogger
from dataclasses import dataclass

from .config import Settings
from .mysql_api import MySQLApi
from .clickhouse_api import ClickhouseApi
from .converter import MysqlToClickhouseConverter
from .binlog_replicator import DataReader
from .db_replicator_initial import DbReplicatorInitial
from .db_replicator_realtime import DbReplicatorRealtime
from .common import Status


logger = getLogger(__name__)


@dataclass
class Statistics:
    last_transaction: tuple = None
    events_count: int = 0
    insert_events_count: int = 0
    insert_records_count: int = 0
    erase_events_count: int = 0
    erase_records_count: int = 0
    no_events_count: int = 0
    cpu_load: float = 0.0


class State:

    def __init__(self, file_name):
        self.file_name = file_name
        self.last_processed_transaction = None
        self.last_processed_transaction_non_uploaded = None
        self.status = Status.NONE
        self.tables_last_record_version = {}
        self.initial_replication_table = None
        self.initial_replication_max_primary_key = None
        self.tables_structure: dict = {}
        self.tables = []
        self.pid = None
        self.load()

    def load(self):
        file_name = self.file_name
        if not os.path.exists(file_name):
            return
        data = open(file_name, 'rb').read()
        data = pickle.loads(data)
        self.last_processed_transaction = data['last_processed_transaction']
        self.last_processed_transaction_non_uploaded = data['last_processed_transaction']
        self.status = Status(data['status'])
        self.tables_last_record_version = data['tables_last_record_version']
        self.initial_replication_table = data['initial_replication_table']
        self.initial_replication_max_primary_key = data['initial_replication_max_primary_key']
        self.tables_structure = data['tables_structure']
        self.tables = data['tables']
        self.pid = data.get('pid', None)

    def save(self):
        file_name = self.file_name
        data = pickle.dumps({
            'last_processed_transaction': self.last_processed_transaction,
            'status': self.status.value,
            'tables_last_record_version': self.tables_last_record_version,
            'initial_replication_table': self.initial_replication_table,
            'initial_replication_max_primary_key': self.initial_replication_max_primary_key,
            'tables_structure': self.tables_structure,
            'tables': self.tables,
            'pid': os.getpid(),
            'save_time': time.time(),
        })
        with open(file_name + '.tmp', 'wb') as f:
            f.write(data)
        os.rename(file_name + '.tmp', file_name)

    def remove(self):
        file_name = self.file_name
        if os.path.exists(file_name):
            os.remove(file_name)
        if os.path.exists(file_name + '.tmp'):
            os.remove(file_name + '.tmp')


class DbReplicator:
    def __init__(self, config: Settings, database: str, target_database: str = None, initial_only: bool = False, 
                 worker_id: int = None, total_workers: int = None, table: str = None):
        self.config = config
        self.database = database
        self.worker_id = worker_id
        self.total_workers = total_workers
        self.settings_file = config.settings_file
        self.single_table = table  # Store the single table to process
        
        # use same as source database by default
        self.target_database = database

        # use target database from config file if exists
        target_database_from_config = config.target_databases.get(database)
        if target_database_from_config:
            self.target_database = target_database_from_config

        # use command line argument if exists
        if target_database:
            self.target_database = target_database

        self.initial_only = initial_only

        # Handle state file differently for parallel workers
        if self.worker_id is not None and self.total_workers is not None:
            # For worker processes in parallel mode, use a different state file with a deterministic name
            self.is_parallel_worker = True
            
            # Determine table name for the state file
            table_identifier = self.single_table if self.single_table else "all_tables"
            
            # Create a hash of the table name to ensure it's filesystem-safe
            if self.single_table:
                # Use a hex digest of the table name to ensure it's filesystem-safe
                table_identifier = hashlib.sha256(self.single_table.encode('utf-8')).hexdigest()[:16]
            else:
                table_identifier = "all_tables"
            
            # Create a deterministic state file path that includes worker_id, total_workers, and table hash
            self.state_path = os.path.join(
                self.config.binlog_replicator.data_dir, 
                self.database, 
                f'state_worker_{self.worker_id}_of_{self.total_workers}_{table_identifier}.pckl'
            )
            
            logger.info(f"Worker {self.worker_id}/{self.total_workers} using state file: {self.state_path}")
            
            if self.single_table:
                logger.info(f"Worker {self.worker_id} focusing only on table: {self.single_table}")
        else:
            self.state_path = os.path.join(self.config.binlog_replicator.data_dir, self.database, 'state.pckl')
            self.is_parallel_worker = False

        self.target_database_tmp = self.target_database + '_tmp'
        if self.is_parallel_worker:
            self.target_database_tmp = self.target_database

        self.mysql_api = MySQLApi(
            database=self.database,
            mysql_settings=config.mysql,
        )
        self.clickhouse_api = ClickhouseApi(
            database=self.target_database,
            clickhouse_settings=config.clickhouse,
        )
        self.converter = MysqlToClickhouseConverter(self)
        self.data_reader = DataReader(config.binlog_replicator, database)
        self.state = self.create_state()
        self.clickhouse_api.tables_last_record_version = self.state.tables_last_record_version
        self.stats = Statistics()
        self.start_time = time.time()
        
        # Create the initial replicator instance
        self.initial_replicator = DbReplicatorInitial(self)
        
        # Create the realtime replicator instance
        self.realtime_replicator = DbReplicatorRealtime(self)

    def create_state(self):
        return State(self.state_path)

    def validate_database_settings(self):
        if not self.initial_only:
            final_setting = self.clickhouse_api.get_system_setting('final')
            if final_setting != '1':
                logger.warning('settings validation failed')
                logger.warning(
                    '\n\n\n    !!!  WARNING - MISSING REQUIRED CLICKHOUSE SETTING  (final)  !!!\n\n'
                    'You need to set <final>1</final> in clickhouse config file\n'
                    'Otherwise you will get DUPLICATES in your SELECT queries\n\n\n'
                )

    def run(self):
        try:
            logger.info('launched db_replicator')
            self.validate_database_settings()

            if self.state.status != Status.NONE:
                # ensure target database still exists
                if self.target_database not in self.clickhouse_api.get_databases() and f"{self.target_database}_tmp" not in self.clickhouse_api.get_databases():
                    logger.warning(f'database {self.target_database} missing in CH')
                    logger.warning('will run replication from scratch')
                    self.state.remove()
                    self.state = self.create_state()

            if self.state.status == Status.RUNNING_REALTIME_REPLICATION:
                self.run_realtime_replication()
                return
            if self.state.status == Status.PERFORMING_INITIAL_REPLICATION:
                self.initial_replicator.perform_initial_replication()
                self.run_realtime_replication()
                return

            # If ignore_deletes is enabled, we don't create a temporary DB and don't swap DBs
            # We replicate directly into the target DB
            if self.config.ignore_deletes:
                logger.info(f'using existing database (ignore_deletes=True)')
                self.clickhouse_api.database = self.target_database
                self.target_database_tmp = self.target_database
                
                # Create database if it doesn't exist
                if self.target_database not in self.clickhouse_api.get_databases():
                    logger.info(f'creating database {self.target_database}')
                    self.clickhouse_api.create_database(db_name=self.target_database)
            else:
                logger.info('recreating database')
                self.clickhouse_api.database = self.target_database_tmp
                if not self.is_parallel_worker:
                    self.clickhouse_api.recreate_database()

            self.state.tables = self.mysql_api.get_tables()
            self.state.tables = [
                table for table in self.state.tables if self.config.is_table_matches(table)
            ]
            self.state.last_processed_transaction = self.data_reader.get_last_transaction_id()
            self.state.save()
            logger.info(f'last known transaction {self.state.last_processed_transaction}')
            self.initial_replicator.create_initial_structure()
            self.initial_replicator.perform_initial_replication()
            self.run_realtime_replication()
        except Exception:
            logger.error(f'unhandled exception', exc_info=True)
            raise

    def run_realtime_replication(self):
        # Delegate to the realtime replicator
        self.realtime_replicator.run_realtime_replication()
