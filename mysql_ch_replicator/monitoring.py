import os
import json
import time
import pickle

from .config import Settings
from .mysql_api import MySQLApi


def bnum(fname):
    return int(fname.split('.')[-1])


class Monitoring:

    CHECK_INTERVAL = 10

    def __init__(self, databases: str, config: Settings):
        self.config = config
        self.databases = [db.strip() for db in databases.split(',') if db.strip()]
        self.mysql_api = MySQLApi(database=None, mysql_settings=config.mysql)

    def run(self):
        stats = []
        stats.append('timestamp')
        stats.append('mysql')
        stats.append('binlog')
        stats.append('binlog_diff')

        for database in self.databases:
            stats.append(database)
            stats.append(database + '_diff')

        print('|'.join(map(str, stats)), flush=True)

        while True:
            binlog_file_binlog = self.get_last_binlog_binlog()
            binlog_file_mysql = self.get_last_binlog_mysql()

            stats = []
            stats.append(int(time.time()))
            stats.append(binlog_file_mysql)
            stats.append(binlog_file_binlog)
            stats.append(bnum(binlog_file_mysql) - bnum(binlog_file_binlog))

            for database in self.databases:
                database_binlog = self.get_last_binlog_database(database)
                stats.append(database_binlog)
                stats.append(bnum(binlog_file_mysql) - bnum(database_binlog))

            print('|'.join(map(str, stats)), flush=True)
            time.sleep(Monitoring.CHECK_INTERVAL)

    def get_last_binlog_binlog(self):
        return self.get_binlog_state()['last_seen_transaction'][0]

    def get_binlog_state(self):
        file_path = os.path.join(self.config.binlog_replicator.data_dir, 'state.json')
        data = open(file_path, 'rt').read()
        return json.loads(data)

    def get_last_binlog_mysql(self):
        files = self.mysql_api.get_binlog_files()
        files = sorted(files)
        return files[-1]

    def get_last_binlog_database(self, database):
        state = self.load_database_state(database)
        return state['last_processed_transaction'][0]

    def load_database_state(self, database):
        file_path = os.path.join(self.config.binlog_replicator.data_dir, database, 'state.pckl')
        data = open(file_path, 'rb').read()
        return pickle.loads(data)
