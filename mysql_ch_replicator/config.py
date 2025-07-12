import yaml
import fnmatch
import zoneinfo

from dataclasses import dataclass


def stype(obj):
    return type(obj).__name__


@dataclass
class MysqlSettings:
    host: str = 'localhost'
    port: int = 3306
    user: str = 'root'
    password: str = ''

    def validate(self):
        if not isinstance(self.host, str):
            raise ValueError(f'mysql host should be string and not {stype(self.host)}')

        if not isinstance(self.port, int):
            raise ValueError(f'mysql port should be int and not {stype(self.port)}')

        if not isinstance(self.user, str):
            raise ValueError(f'mysql user should be string and not {stype(self.user)}')

        if not isinstance(self.password, str):
            raise ValueError(f'mysql password should be string and not {stype(self.password)}')


@dataclass
class Index:
    databases: str | list = '*'
    tables: str | list = '*'
    index: str = ''


@dataclass
class PartitionBy:
    databases: str | list = '*'
    tables: str | list = '*'
    partition_by: str = ''


@dataclass
class ClickhouseSettings:
    host: str = 'localhost'
    port: int = 3306
    user: str = 'root'
    password: str = ''
    connection_timeout: int = 30
    send_receive_timeout: int = 120

    def validate(self):
        if not isinstance(self.host, str):
            raise ValueError(f'clickhouse host should be string and not {stype(self.host)}')

        if not isinstance(self.port, int):
            raise ValueError(f'clickhouse port should be int and not {stype(self.port)}')

        if not isinstance(self.user, str):
            raise ValueError(f'clickhouse user should be string and not {stype(self.user)}')

        if not isinstance(self.password, str):
            raise ValueError(f'clickhouse password should be string and not {stype(self.password)}')

        if not isinstance(self.connection_timeout, int):
            raise ValueError(f'clickhouse connection_timeout should be int and not {stype(self.connection_timeout)}')

        if not isinstance(self.send_receive_timeout, int):
            raise ValueError(f'clickhouse send_receive_timeout should be int and not {stype(self.send_receive_timeout)}')

        if self.connection_timeout <= 0:
            raise ValueError(f'connection timeout should be at least 1 second')

        if self.send_receive_timeout <= 0:
            raise ValueError(f'send_receive_timeout timeout should be at least 1 second')


@dataclass
class BinlogReplicatorSettings:
    data_dir: str = 'binlog'
    records_per_file: int = 100000
    binlog_retention_period: int = 43200  # 12 hours in seconds

    def validate(self):
        if not isinstance(self.data_dir, str):
            raise ValueError(f'binlog_replicator data_dir should be string and not {stype(self.data_dir)}')

        if not isinstance(self.records_per_file, int):
            raise ValueError(f'binlog_replicator records_per_file should be int and not {stype(self.data_dir)}')

        if self.records_per_file <= 0:
            raise ValueError('binlog_replicator records_per_file should be positive')

        if not isinstance(self.binlog_retention_period, int):
            raise ValueError(f'binlog_replicator binlog_retention_period should be int and not {stype(self.binlog_retention_period)}')

        if self.binlog_retention_period <= 0:
            raise ValueError('binlog_replicator binlog_retention_period should be positive')


class Settings:
    DEFAULT_LOG_LEVEL = 'info'
    DEFAULT_OPTIMIZE_INTERVAL = 86400
    DEFAULT_CHECK_DB_UPDATED_INTERVAL = 120
    DEFAULT_AUTO_RESTART_INTERVAL = 3600

    def __init__(self):
        self.mysql = MysqlSettings()
        self.clickhouse = ClickhouseSettings()
        self.binlog_replicator = BinlogReplicatorSettings()
        self.databases = ''
        self.tables = '*'
        self.exclude_databases = ''
        self.exclude_tables = ''
        self.settings_file = ''
        self.log_level = 'info'
        self.debug_log_level = False
        self.optimize_interval = 0
        self.check_db_updated_interval = 0
        self.indexes: list[Index] = []
        self.partition_bys: list[PartitionBy] = []
        self.auto_restart_interval = 0
        self.http_host = ''
        self.http_port = 0
        self.types_mapping = {}
        self.target_databases = {}
        self.initial_replication_threads = 0
        self.ignore_deletes = False
        self.mysql_timezone = 'UTC'

    def load(self, settings_file):
        data = open(settings_file, 'r').read()
        data = yaml.safe_load(data)

        self.settings_file = settings_file
        self.mysql = MysqlSettings(**data.pop('mysql'))
        self.clickhouse = ClickhouseSettings(**data.pop('clickhouse'))
        self.databases = data.pop('databases')
        self.tables = data.pop('tables', '*')
        self.exclude_databases = data.pop('exclude_databases', '')
        self.exclude_tables = data.pop('exclude_tables', '')
        self.log_level = data.pop('log_level', Settings.DEFAULT_LOG_LEVEL)
        self.optimize_interval = data.pop('optimize_interval', Settings.DEFAULT_OPTIMIZE_INTERVAL)
        self.check_db_updated_interval = data.pop(
            'check_db_updated_interval', Settings.DEFAULT_CHECK_DB_UPDATED_INTERVAL,
        )
        self.auto_restart_interval = data.pop(
            'auto_restart_interval', Settings.DEFAULT_AUTO_RESTART_INTERVAL,
        )
        self.types_mapping = data.pop('types_mapping', {})
        self.http_host = data.pop('http_host', '')
        self.http_port = data.pop('http_port', 0)
        self.target_databases = data.pop('target_databases', {})
        self.initial_replication_threads = data.pop('initial_replication_threads', 0)
        self.ignore_deletes = data.pop('ignore_deletes', False)
        self.mysql_timezone = data.pop('mysql_timezone', 'UTC')

        indexes = data.pop('indexes', [])
        for index in indexes:
            self.indexes.append(
                Index(**index)
            )
        
        partition_bys = data.pop('partition_bys', [])
        for partition_by in partition_bys:
            self.partition_bys.append(
                PartitionBy(**partition_by)
            )
        
        assert isinstance(self.databases, str) or isinstance(self.databases, list)
        assert isinstance(self.tables, str) or isinstance(self.tables, list)
        self.binlog_replicator = BinlogReplicatorSettings(**data.pop('binlog_replicator'))
        if data:
            raise Exception(f'Unsupported config options: {list(data.keys())}')
        self.validate()

    @classmethod
    def is_pattern_matches(cls, substr, pattern):
        if not pattern or pattern == '*':
            return True
        if isinstance(pattern, str):
            return fnmatch.fnmatch(substr, pattern)
        if isinstance(pattern, list):
            for allowed_pattern in pattern:
                if fnmatch.fnmatch(substr, allowed_pattern):
                    return True
            return False
        raise ValueError()

    def is_database_matches(self, db_name):
        if self.exclude_databases and self.is_pattern_matches(db_name, self.exclude_databases):
            return False
        return self.is_pattern_matches(db_name, self.databases)

    def is_table_matches(self, table_name):
        if self.exclude_tables and self.is_pattern_matches(table_name, self.exclude_tables):
            return False
        return self.is_pattern_matches(table_name, self.tables)

    def validate_log_level(self):
        if self.log_level not in ['critical', 'error', 'warning', 'info', 'debug']:
            raise ValueError(f'wrong log level {self.log_level}')
        if self.log_level == 'debug':
            self.debug_log_level = True

    def validate_mysql_timezone(self):
        if not isinstance(self.mysql_timezone, str):
            raise ValueError(f'mysql_timezone should be string and not {stype(self.mysql_timezone)}')
        
        # Validate timezone by attempting to import and check if it's valid
        try:
            zoneinfo.ZoneInfo(self.mysql_timezone)
        except zoneinfo.ZoneInfoNotFoundError:
            raise ValueError(f'invalid timezone: {self.mysql_timezone}. Use IANA timezone names like "UTC", "Europe/London", "America/New_York", etc.')

    def get_indexes(self, db_name, table_name):
        results = []
        for index in self.indexes:
            if not self.is_pattern_matches(db_name, index.databases):
                continue
            if not self.is_pattern_matches(table_name, index.tables):
                continue
            results.append(index.index)
        return results

    def get_partition_bys(self, db_name, table_name):
        results = []
        for partition_by in self.partition_bys:
            if not self.is_pattern_matches(db_name, partition_by.databases):
                continue
            if not self.is_pattern_matches(table_name, partition_by.tables):
                continue
            results.append(partition_by.partition_by)
        return results

    def validate(self):
        self.mysql.validate()
        self.clickhouse.validate()
        self.binlog_replicator.validate()
        self.validate_log_level()
        if not isinstance(self.target_databases, dict):
            raise ValueError(f'wrong target databases {self.target_databases}')
        if not isinstance(self.initial_replication_threads, int):
            raise ValueError(f'initial_replication_threads should be an integer, not {type(self.initial_replication_threads)}')
        if self.initial_replication_threads < 0:
            raise ValueError(f'initial_replication_threads should be non-negative')
        self.validate_mysql_timezone()
