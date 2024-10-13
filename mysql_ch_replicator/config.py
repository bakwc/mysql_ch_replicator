import yaml
import fnmatch

from dataclasses import dataclass


@dataclass
class MysqlSettings:
    host: str = 'localhost'
    port: int = 3306
    user: str = 'root'
    password: str = ''


@dataclass
class ClickhouseSettings:
    host: str = 'localhost'
    port: int = 3306
    user: str = 'root'
    password: str = ''


@dataclass
class BinlogReplicatorSettings:
    data_dir: str = 'binlog'
    records_per_file: int = 100000


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
