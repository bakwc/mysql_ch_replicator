import json
import os.path
import time
from logging import getLogger
from collections import defaultdict

from .binlog_replicator import LogEvent, EventType
from .table_structure import TableStructure
from .utils import GracefulKiller, touch_all_files, format_floats
from .converter import strip_sql_comments
from .common import Status


logger = getLogger(__name__)


class DbReplicatorRealtime:
    # Constants for realtime replication
    SAVE_STATE_INTERVAL = 10
    STATS_DUMP_INTERVAL = 60
    BINLOG_TOUCH_INTERVAL = 120
    DATA_DUMP_INTERVAL = 1
    DATA_DUMP_BATCH_SIZE = 100000
    READ_LOG_INTERVAL = 0.3

    def __init__(self, replicator):
        self.replicator = replicator
        
        # Initialize internal state
        self.records_to_insert = defaultdict(dict)  # table_name => {record_id=>record, ...}
        self.records_to_delete = defaultdict(set)  # table_name => {record_id, ...}
        self.last_save_state_time = 0
        self.last_dump_stats_time = 0
        self.last_dump_stats_process_time = 0
        self.last_records_upload_time = 0
        self.start_time = time.time()

    def run_realtime_replication(self):
        if self.replicator.initial_only:
            logger.info('skip running realtime replication, only initial replication was requested')
            self.replicator.state.remove()
            return

        # Close MySQL connection as it's not needed for realtime replication
        if self.replicator.mysql_api:
            self.replicator.mysql_api.close()
            self.replicator.mysql_api = None
            
        logger.info(f'running realtime replication from the position: {self.replicator.state.last_processed_transaction}')
        self.replicator.state.status = Status.RUNNING_REALTIME_REPLICATION
        self.replicator.state.save()
        self.replicator.data_reader.set_position(self.replicator.state.last_processed_transaction)

        killer = GracefulKiller()

        while not killer.kill_now:
            if self.replicator.config.auto_restart_interval:
                curr_time = time.time()
                if curr_time - self.start_time >= self.replicator.config.auto_restart_interval:
                    logger.info('process restart (check auto_restart_interval config option)')
                    break

            event = self.replicator.data_reader.read_next_event()
            if event is None:
                time.sleep(self.READ_LOG_INTERVAL)
                self.upload_records_if_required(table_name=None)
                self.replicator.stats.no_events_count += 1
                self.log_stats_if_required()
                continue
            assert event.db_name == self.replicator.database
            if self.replicator.database != self.replicator.target_database:
                event.db_name = self.replicator.target_database
            self.handle_event(event)

        logger.info('stopping db_replicator')
        self.upload_records()
        self.save_state_if_required(force=True)
        logger.info('stopped')

    def handle_event(self, event: LogEvent):
        if self.replicator.state.last_processed_transaction_non_uploaded is not None:
            if event.transaction_id <= self.replicator.state.last_processed_transaction_non_uploaded:
                return

        logger.debug(f'processing event {event.transaction_id}, {event.event_type}, {event.table_name}')

        event_handlers = {
            EventType.ADD_EVENT.value: self.handle_insert_event,
            EventType.REMOVE_EVENT.value: self.handle_erase_event,
            EventType.QUERY.value: self.handle_query_event,
        }

        if not event.table_name or self.replicator.config.is_table_matches(event.table_name):
            event_handlers[event.event_type](event)

        self.replicator.stats.events_count += 1
        self.replicator.stats.last_transaction = event.transaction_id
        self.replicator.state.last_processed_transaction_non_uploaded = event.transaction_id

        self.upload_records_if_required(table_name=event.table_name)

        self.save_state_if_required()
        self.log_stats_if_required()

    def save_state_if_required(self, force=False):
        curr_time = time.time()
        if curr_time - self.last_save_state_time < self.SAVE_STATE_INTERVAL and not force:
            return
        self.last_save_state_time = curr_time
        self.replicator.state.tables_last_record_version = self.replicator.clickhouse_api.tables_last_record_version
        self.replicator.state.save()

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
        if self.replicator.config.debug_log_level:
            logger.debug(
                f'processing insert event: {event.transaction_id}, '
                f'table: {event.table_name}, '
                f'records: {event.records}',
            )
        self.replicator.stats.insert_events_count += 1
        self.replicator.stats.insert_records_count += len(event.records)

        mysql_table_structure = self.replicator.state.tables_structure[event.table_name][0]
        clickhouse_table_structure = self.replicator.state.tables_structure[event.table_name][1]
        records = self.replicator.converter.convert_records(event.records, mysql_table_structure, clickhouse_table_structure)

        current_table_records_to_insert = self.records_to_insert[event.table_name]
        current_table_records_to_delete = self.records_to_delete[event.table_name]
        for record in records:
            record_id = self._get_record_id(clickhouse_table_structure, record)
            current_table_records_to_insert[record_id] = record
            current_table_records_to_delete.discard(record_id)

    def handle_erase_event(self, event: LogEvent):
        if self.replicator.config.debug_log_level:
            logger.debug(
                f'processing erase event: {event.transaction_id}, '
                f'table: {event.table_name}, '
                f'records: {event.records}',
            )
        
        # If ignore_deletes is enabled, skip processing delete events
        if self.replicator.config.ignore_deletes:
            if self.replicator.config.debug_log_level:
                logger.debug(
                    f'ignoring erase event (ignore_deletes=True): {event.transaction_id}, '
                    f'table: {event.table_name}, '
                    f'records: {len(event.records)}',
                )
            return
            
        self.replicator.stats.erase_events_count += 1
        self.replicator.stats.erase_records_count += len(event.records)

        table_structure_ch: TableStructure = self.replicator.state.tables_structure[event.table_name][1]
        table_structure_mysql: TableStructure = self.replicator.state.tables_structure[event.table_name][0]

        records = self.replicator.converter.convert_records(
            event.records, table_structure_mysql, table_structure_ch, only_primary=True,
        )
        keys_to_remove = [self._get_record_id(table_structure_ch, record) for record in records]

        current_table_records_to_insert = self.records_to_insert[event.table_name]
        current_table_records_to_delete = self.records_to_delete[event.table_name]
        for record_id in keys_to_remove:
            current_table_records_to_delete.add(record_id)
            current_table_records_to_insert.pop(record_id, None)

    def handle_query_event(self, event: LogEvent):
        if self.replicator.config.debug_log_level:
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
        if query.lower().startswith('truncate'):
            self.upload_records()
            self.handle_truncate_query(query, event.db_name)

    def handle_alter_query(self, query, db_name):
        self.replicator.converter.convert_alter_query(query, db_name)

    def handle_create_table_query(self, query, db_name):
        mysql_structure, ch_structure = self.replicator.converter.parse_create_table_query(query)
        if not self.replicator.config.is_table_matches(mysql_structure.table_name):
            return
        self.replicator.state.tables_structure[mysql_structure.table_name] = (mysql_structure, ch_structure)
        indexes = self.replicator.config.get_indexes(self.replicator.database, ch_structure.table_name)
        partition_bys = self.replicator.config.get_partition_bys(self.replicator.database, ch_structure.table_name)
        self.replicator.clickhouse_api.create_table(ch_structure, additional_indexes=indexes, additional_partition_bys=partition_bys)

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

        db_name, table_name, matches_config = self.replicator.converter.get_db_and_table_name(tokens[2], db_name)
        if not matches_config:
            return

        if table_name in self.replicator.state.tables_structure:
            self.replicator.state.tables_structure.pop(table_name)
        self.replicator.clickhouse_api.execute_command(f'DROP TABLE {"IF EXISTS" if if_exists else ""} `{db_name}`.`{table_name}`')

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

            src_db_name, src_table_name, matches_config = self.replicator.converter.get_db_and_table_name(tokens[0], db_name)
            dest_db_name, dest_table_name, _ = self.replicator.converter.get_db_and_table_name(tokens[2], db_name)
            if not matches_config:
                return

            if src_db_name != self.replicator.target_database or dest_db_name != self.replicator.target_database:
                raise Exception('cross databases table renames not implemented', tokens)
            if src_table_name in self.replicator.state.tables_structure:
                self.replicator.state.tables_structure[dest_table_name] = self.replicator.state.tables_structure.pop(src_table_name)

            ch_clauses.append(f"`{src_db_name}`.`{src_table_name}` TO `{dest_db_name}`.`{dest_table_name}`")
        self.replicator.clickhouse_api.execute_command(f'RENAME TABLE {", ".join(ch_clauses)}')

    def handle_truncate_query(self, query, db_name):
        """Handle TRUNCATE TABLE operations by clearing data in ClickHouse"""
        tokens = query.strip().split()
        if len(tokens) < 3 or tokens[0].lower() != 'truncate' or tokens[1].lower() != 'table':
            raise Exception('Invalid TRUNCATE query format', query)

        # Get table name from the third token (after TRUNCATE TABLE)
        table_token = tokens[2]
        
        # Parse database and table name from the token
        db_name, table_name, matches_config = self.replicator.converter.get_db_and_table_name(table_token, db_name)
        if not matches_config:
            return

        # Check if table exists in our tracking
        if table_name not in self.replicator.state.tables_structure:
            logger.warning(f'TRUNCATE: Table {table_name} not found in tracked tables, skipping')
            return

        # Clear any pending records for this table
        if table_name in self.records_to_insert:
            self.records_to_insert[table_name].clear()
        if table_name in self.records_to_delete:
            self.records_to_delete[table_name].clear()

        # Execute TRUNCATE on ClickHouse
        logger.info(f'Executing TRUNCATE on ClickHouse table: {db_name}.{table_name}')
        self.replicator.clickhouse_api.execute_command(f'TRUNCATE TABLE `{db_name}`.`{table_name}`')

    def log_stats_if_required(self):
        curr_time = time.time()
        if curr_time - self.last_dump_stats_time < self.STATS_DUMP_INTERVAL:
            return

        curr_process_time = time.process_time()

        time_spent = curr_time - self.last_dump_stats_time
        process_time_spent = curr_process_time - self.last_dump_stats_process_time

        if time_spent > 0.0:
            self.replicator.stats.cpu_load = process_time_spent / time_spent

        self.last_dump_stats_time = curr_time
        self.last_dump_stats_process_time = curr_process_time
        logger.info(f'stats: {json.dumps(format_floats(self.replicator.stats.__dict__))}')
        logger.info(f'ch_stats: {json.dumps(format_floats(self.replicator.clickhouse_api.get_stats()))}')
        # Reset stats for next period - reuse parent's stats object
        self.replicator.stats = type(self.replicator.stats)()

    def upload_records_if_required(self, table_name):
        need_dump = False
        if table_name is not None:
            if len(self.records_to_insert[table_name]) >= self.DATA_DUMP_BATCH_SIZE:
                need_dump = True
            if len(self.records_to_delete[table_name]) >= self.DATA_DUMP_BATCH_SIZE:
                need_dump = True

        curr_time = time.time()
        if curr_time - self.last_records_upload_time >= self.DATA_DUMP_INTERVAL:
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
            _, ch_table_structure = self.replicator.state.tables_structure[table_name]
            if self.replicator.config.debug_log_level:
                logger.debug(f'inserting into {table_name}, records: {records}')
            self.replicator.clickhouse_api.insert(table_name, records, table_structure=ch_table_structure)

        for table_name, keys_to_remove in self.records_to_delete.items():
            if not keys_to_remove:
                continue
            table_structure: TableStructure = self.replicator.state.tables_structure[table_name][0]
            primary_key_names = table_structure.primary_keys
            if self.replicator.config.debug_log_level:
                logger.debug(f'erasing from {table_name}, primary key: {primary_key_names}, values: {keys_to_remove}')
            self.replicator.clickhouse_api.erase(
                table_name=table_name,
                field_name=primary_key_names,
                field_values=keys_to_remove,
            )

        self.records_to_insert = defaultdict(dict)  # table_name => {record_id=>record, ...}
        self.records_to_delete = defaultdict(set)  # table_name => {record_id, ...}
        self.replicator.state.last_processed_transaction = self.replicator.state.last_processed_transaction_non_uploaded
        self.save_state_if_required()
