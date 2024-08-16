import yaml

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
        self.settings_file = ''

    def load(self, settings_file):
        data = open(settings_file, 'r').read()
        data = yaml.safe_load(data)

        self.settings_file = settings_file
        self.mysql = MysqlSettings(**data['mysql'])
        self.clickhouse = ClickhouseSettings(**data['clickhouse'])
        self.databases = data['databases']
        assert isinstance(self.databases, str)
        self.binlog_replicator = BinlogReplicatorSettings(**data['binlog_replicator'])
