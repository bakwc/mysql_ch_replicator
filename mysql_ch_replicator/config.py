import yaml
import fnmatch

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
class ClickhouseSettings:
    host: str = 'localhost'
    port: int = 3306
    user: str = 'root'
    password: str = ''

    def validate(self):
        if not isinstance(self.host, str):
            raise ValueError(f'clickhouse host should be string and not {stype(self.host)}')

        if not isinstance(self.port, int):
            raise ValueError(f'clickhouse port should be int and not {stype(self.port)}')

        if not isinstance(self.user, str):
            raise ValueError(f'clickhouse user should be string and not {stype(self.user)}')

        if not isinstance(self.password, str):
            raise ValueError(f'clickhouse password should be string and not {stype(self.password)}')


@dataclass
class BinlogReplicatorSettings:
    data_dir: str = 'binlog'
    records_per_file: int = 100000

    def validate(self):
        if not isinstance(self.data_dir, str):
            raise ValueError(f'binlog_replicator data_dir should be string and not {stype(self.data_dir)}')

        if not isinstance(self.records_per_file, int):
            raise ValueError(f'binlog_replicator records_per_file should be int and not {stype(self.data_dir)}')

        if self.records_per_file <= 0:
            raise ValueError('binlog_replicator records_per_file should be positive')


class Settings:

    def __init__(self):
        self.mysql = MysqlSettings()
        self.clickhouse = ClickhouseSettings()
        self.binlog_replicator = BinlogReplicatorSettings()
        self.databases = ''
        self.tables = '*'
        self.settings_file = ''

    def load(self, settings_file):
        data = open(settings_file, 'r').read()
        data = yaml.safe_load(data)

        self.settings_file = settings_file
        self.mysql = MysqlSettings(**data['mysql'])
        self.clickhouse = ClickhouseSettings(**data['clickhouse'])
        self.databases = data['databases']
        self.tables = data.get('tables', '*')
        assert isinstance(self.databases, str) or isinstance(self.databases, list)
        assert isinstance(self.tables, str) or isinstance(self.tables, list)
        self.binlog_replicator = BinlogReplicatorSettings(**data['binlog_replicator'])
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
        return self.is_pattern_matches(db_name, self.databases)

    def is_table_matches(self, table_name):
        return self.is_pattern_matches(table_name, self.tables)

    def validate(self):
        self.mysql.validate()
        self.clickhouse.validate()
        self.binlog_replicator.validate()
