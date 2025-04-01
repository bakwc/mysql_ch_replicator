import json
import os.path
import random
import time
import pickle
import hashlib
from logging import getLogger
from enum import Enum
from dataclasses import dataclass
from collections import defaultdict
import sys
import subprocess
import select

from .config import Settings, MysqlSettings, ClickhouseSettings
from .mysql_api import MySQLApi
from .clickhouse_api import ClickhouseApi
from .converter import MysqlToClickhouseConverter, strip_sql_name, strip_sql_comments
from .table_structure import TableStructure, TableField
from .binlog_replicator import DataReader, LogEvent, EventType
from .utils import GracefulKiller, touch_all_files, format_floats
from .db_replicator_initial import DbReplicatorInitial
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

    SAVE_STATE_INTERVAL = 10
    STATS_DUMP_INTERVAL = 60
    BINLOG_TOUCH_INTERVAL = 120

    DATA_DUMP_INTERVAL = 1
    DATA_DUMP_BATCH_SIZE = 100000

    READ_LOG_INTERVAL = 0.3

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
        self.last_save_state_time = 0
        self.stats = Statistics()
        self.last_dump_stats_time = 0
        self.last_dump_stats_process_time = 0
        self.records_to_insert = defaultdict(dict)  # table_name => {record_id=>record, ...}
        self.records_to_delete = defaultdict(set)  # table_name => {record_id, ...}
        self.last_records_upload_time = 0
        self.last_touch_time = 0
        self.start_time = time.time()
        
        # Create the initial replicator instance
        self.initial_replicator = DbReplicatorInitial(self)

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

    def prevent_binlog_removal(self):
        if time.time() - self.last_touch_time < self.BINLOG_TOUCH_INTERVAL:
            return
        binlog_directory = os.path.join(self.config.binlog_replicator.data_dir, self.database)
        logger.info(f'touch binlog {binlog_directory}')
        if not os.path.exists(binlog_directory):
            return
        self.last_touch_time = time.time()
        touch_all_files(binlog_directory)

    def run_realtime_replication(self):
        if self.initial_only:
            logger.info('skip running realtime replication, only initial replication was requested')
            self.state.remove()
            return

        self.mysql_api.close()
        self.mysql_api = None
        logger.info(f'running realtime replication from the position: {self.state.last_processed_transaction}')
        self.state.status = Status.RUNNING_REALTIME_REPLICATION
        self.state.save()
        self.data_reader.set_position(self.state.last_processed_transaction)

        killer = GracefulKiller()

        while not killer.kill_now:
            if self.config.auto_restart_interval:
                curr_time = time.time()
                if curr_time - self.start_time >= self.config.auto_restart_interval:
                    logger.info('process restart (check auto_restart_interval config option)')
                    break

            event = self.data_reader.read_next_event()
            if event is None:
                time.sleep(DbReplicator.READ_LOG_INTERVAL)
                self.upload_records_if_required(table_name=None)
                self.stats.no_events_count += 1
                self.log_stats_if_required()
                continue
            assert event.db_name == self.database
            if self.database != self.target_database:
                event.db_name = self.target_database
            self.handle_event(event)

        logger.info('stopping db_replicator')
        self.upload_records()
        self.save_state_if_required(force=True)
        logger.info('stopped')


    def handle_event(self, event: LogEvent):
        if self.state.last_processed_transaction_non_uploaded is not None:
            if event.transaction_id <= self.state.last_processed_transaction_non_uploaded:
                return

        logger.debug(f'processing event {event.transaction_id}, {event.event_type}, {event.table_name}')

        event_handlers = {
            EventType.ADD_EVENT.value: self.handle_insert_event,
            EventType.REMOVE_EVENT.value: self.handle_erase_event,
            EventType.QUERY.value: self.handle_query_event,
        }

        if not event.table_name or self.config.is_table_matches(event.table_name):
            event_handlers[event.event_type](event)

        self.stats.events_count += 1
        self.stats.last_transaction = event.transaction_id
        self.state.last_processed_transaction_non_uploaded = event.transaction_id

        self.upload_records_if_required(table_name=event.table_name)

        self.save_state_if_required()
        self.log_stats_if_required()

    def save_state_if_required(self, force=False):
        curr_time = time.time()
        if curr_time - self.last_save_state_time < DbReplicator.SAVE_STATE_INTERVAL and not force:
            return
        self.last_save_state_time = curr_time
        self.state.tables_last_record_version = self.clickhouse_api.tables_last_record_version
        self.state.save()

    def _get_record_id(self, ch_table_structure, record: list):
        result = []
        for idx in ch_table_structure.primary_key_ids:
            field_type = ch_table_structure.fields[idx].field_type
            if field_type == 'String':
                result.append(f"'{record[idx]}'")
            else:
                result.append(record[idx])
        return ','.join(map(str, result))

    def handle_insert_event(self, event: LogEvent):
        if self.config.debug_log_level:
            logger.debug(
                f'processing insert event: {event.transaction_id}, '
                f'table: {event.table_name}, '
                f'records: {event.records}',
            )
        self.stats.insert_events_count += 1
        self.stats.insert_records_count += len(event.records)

        mysql_table_structure = self.state.tables_structure[event.table_name][0]
        clickhouse_table_structure = self.state.tables_structure[event.table_name][1]
        records = self.converter.convert_records(event.records, mysql_table_structure, clickhouse_table_structure)

        current_table_records_to_insert = self.records_to_insert[event.table_name]
        current_table_records_to_delete = self.records_to_delete[event.table_name]
        for record in records:
            record_id = self._get_record_id(clickhouse_table_structure, record)
            current_table_records_to_insert[record_id] = record
            current_table_records_to_delete.discard(record_id)

    def handle_erase_event(self, event: LogEvent):
        if self.config.debug_log_level:
            logger.debug(
                f'processing erase event: {event.transaction_id}, '
                f'table: {event.table_name}, '
                f'records: {event.records}',
            )
        self.stats.erase_events_count += 1
        self.stats.erase_records_count += len(event.records)

        table_structure_ch: TableStructure = self.state.tables_structure[event.table_name][1]
        table_structure_mysql: TableStructure = self.state.tables_structure[event.table_name][0]

        records = self.converter.convert_records(
            event.records, table_structure_mysql, table_structure_ch, only_primary=True,
        )
        keys_to_remove = [self._get_record_id(table_structure_ch, record) for record in records]

        current_table_records_to_insert = self.records_to_insert[event.table_name]
        current_table_records_to_delete = self.records_to_delete[event.table_name]
        for record_id in keys_to_remove:
            current_table_records_to_delete.add(record_id)
            current_table_records_to_insert.pop(record_id, None)

    def handle_query_event(self, event: LogEvent):
        if self.config.debug_log_level:
            logger.debug(f'processing query event: {event.transaction_id}, query: {event.records}')
        query = strip_sql_comments(event.records)
        if query.lower().startswith('alter'):
            self.upload_records()
            self.handle_alter_query(query, event.db_name)
        if query.lower().startswith('create table'):
            self.handle_create_table_query(query, event.db_name)
        if query.lower().startswith('drop table'):
            self.upload_records()
            self.handle_drop_table_query(query, event.db_name)
        if query.lower().startswith('rename table'):
            self.upload_records()
            self.handle_rename_table_query(query, event.db_name)

    def handle_alter_query(self, query, db_name):
        self.converter.convert_alter_query(query, db_name)

    def handle_create_table_query(self, query, db_name):
        mysql_structure, ch_structure = self.converter.parse_create_table_query(query)
        if not self.config.is_table_matches(mysql_structure.table_name):
            return
        self.state.tables_structure[mysql_structure.table_name] = (mysql_structure, ch_structure)
        indexes = self.config.get_indexes(self.database, ch_structure.table_name)
        self.clickhouse_api.create_table(ch_structure, additional_indexes=indexes)

    def handle_drop_table_query(self, query, db_name):
        tokens = query.split()
        if tokens[0].lower() != 'drop' or tokens[1].lower() != 'table':
            raise Exception('wrong drop table query', query)

        if_exists = (len(tokens) > 4 and
                tokens[2].lower() == 'if' and
                tokens[3].lower() == 'exists')
        if if_exists:
            del tokens[2:4]  # Remove the 'IF', 'EXISTS' tokens

        if len(tokens) != 3:
            raise Exception('wrong token count', query)

        db_name, table_name, matches_config = self.converter.get_db_and_table_name(tokens[2], db_name)
        if not matches_config:
            return

        if table_name in self.state.tables_structure:
            self.state.tables_structure.pop(table_name)
        self.clickhouse_api.execute_command(f'DROP TABLE {"IF EXISTS" if if_exists else ""} `{db_name}`.`{table_name}`')

    def handle_rename_table_query(self, query, db_name):
        tokens = query.split()
        if tokens[0].lower() != 'rename' or tokens[1].lower() != 'table':
            raise Exception('wrong rename table query', query)

        ch_clauses = []
        for rename_clause in ' '.join(tokens[2:]).split(','):
            tokens = rename_clause.split()

            if len(tokens) != 3:
                raise Exception('wrong token count', query)
            if tokens[1].lower() != 'to':
                raise Exception('"to" keyword expected', query)

            src_db_name, src_table_name, matches_config = self.converter.get_db_and_table_name(tokens[0], db_name)
            dest_db_name, dest_table_name, _ = self.converter.get_db_and_table_name(tokens[2], db_name)
            if not matches_config:
                return

            if src_db_name != self.target_database or dest_db_name != self.target_database:
                raise Exception('cross databases table renames not implemented', tokens)
            if src_table_name in self.state.tables_structure:
                self.state.tables_structure[dest_table_name] = self.state.tables_structure.pop(src_table_name)

            ch_clauses.append(f"`{src_db_name}`.`{src_table_name}` TO `{dest_db_name}`.`{dest_table_name}`")
        self.clickhouse_api.execute_command(f'RENAME TABLE {", ".join(ch_clauses)}')

    def log_stats_if_required(self):
        curr_time = time.time()
        if curr_time - self.last_dump_stats_time < DbReplicator.STATS_DUMP_INTERVAL:
            return

        curr_process_time = time.process_time()

        time_spent = curr_time - self.last_dump_stats_time
        process_time_spent = curr_process_time - self.last_dump_stats_process_time

        if time_spent > 0.0:
            self.stats.cpu_load = process_time_spent / time_spent

        self.last_dump_stats_time = curr_time
        self.last_dump_stats_process_time = curr_process_time
        logger.info(f'stats: {json.dumps(format_floats(self.stats.__dict__))}')
        logger.info(f'ch_stats: {json.dumps(format_floats(self.clickhouse_api.get_stats()))}')
        self.stats = Statistics()

    def upload_records_if_required(self, table_name):
        need_dump = False
        if table_name is not None:
            if len(self.records_to_insert[table_name]) >= DbReplicator.DATA_DUMP_BATCH_SIZE:
                need_dump = True
            if len(self.records_to_delete[table_name]) >= DbReplicator.DATA_DUMP_BATCH_SIZE:
                need_dump = True

        curr_time = time.time()
        if curr_time - self.last_records_upload_time >= DbReplicator.DATA_DUMP_INTERVAL:
            need_dump = True

        if not need_dump:
            return

        self.upload_records()

    def upload_records(self):
        logger.debug(
            f'upload records, to insert: {len(self.records_to_insert)}, to delete: {len(self.records_to_delete)}',
        )
        self.last_records_upload_time = time.time()

        for table_name, id_to_records in self.records_to_insert.items():
            records = id_to_records.values()
            if not records:
                continue
            _, ch_table_structure = self.state.tables_structure[table_name]
            if self.config.debug_log_level:
                logger.debug(f'inserting into {table_name}, records: {records}')
            self.clickhouse_api.insert(table_name, records, table_structure=ch_table_structure)

        for table_name, keys_to_remove in self.records_to_delete.items():
            if not keys_to_remove:
                continue
            table_structure: TableStructure = self.state.tables_structure[table_name][0]
            primary_key_names = table_structure.primary_keys
            if self.config.debug_log_level:
                logger.debug(f'erasing from {table_name}, primary key: {primary_key_names}, values: {keys_to_remove}')
            self.clickhouse_api.erase(
                table_name=table_name,
                field_name=primary_key_names,
                field_values=keys_to_remove,
            )

        self.records_to_insert = defaultdict(dict)  # table_name => {record_id=>record, ...}
        self.records_to_delete = defaultdict(set)  # table_name => {record_id, ...}
        self.state.last_processed_transaction = self.state.last_processed_transaction_non_uploaded
        self.save_state_if_required()
