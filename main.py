#!/usr/bin/env python3

import argparse
import logging

from config import Settings
from db_replicator import DbReplicator
from binlog_replicator import BinlogReplicator
from monitoring import Monitoring


logging.basicConfig(level=logging.INFO, format='[ %(asctime)s %(levelname)8s ] %(message)s')


def run_binlog_replicator(args, config: Settings):
    binlog_replicator = BinlogReplicator(
        mysql_settings=config.mysql,
        replicator_settings=config.binlog_replicator,
    )
    binlog_replicator.run()

def run_db_replicator(args, config: Settings):
    if not args.db:
        raise Exception("need to pass --db argument")

    db_replicator = DbReplicator(
        config=config,
        database=args.db,
    )
    db_replicator.run()


def run_monitoring(args, config: Settings):
    monitoring = Monitoring(args.db or '', config)
    monitoring.run()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", help="run mode", type=str, choices=["binlog_replicator", "db_replicator", "monitoring"])
    parser.add_argument("--config", help="config file path", default='config.yaml', type=str)
    parser.add_argument("--db", help="database(s) name", type=str)
    args = parser.parse_args()

    config = Settings()
    config.load(args.config)
    if args.mode == 'binlog_replicator':
        run_binlog_replicator(args, config)
    if args.mode == 'db_replicator':
        run_db_replicator(args, config)
    if args.mode == 'monitoring':
        run_monitoring(args, config)


if __name__ == '__main__':
    main()
