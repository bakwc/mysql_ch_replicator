#!/usr/bin/env python3

import argparse
import logging

from .config import Settings
from .db_replicator import DbReplicator
from .binlog_replicator import BinlogReplicator
from .monitoring import Monitoring
from .runner import Runner


def set_logging_config(tags):
    logging.basicConfig(
        level=logging.INFO,
        format=f'[{tags} %(asctime)s %(levelname)8s] %(message)s',
    )


def run_binlog_replicator(args, config: Settings):
    set_logging_config('binlogrepl')
    binlog_replicator = BinlogReplicator(
        mysql_settings=config.mysql,
        replicator_settings=config.binlog_replicator,
    )
    binlog_replicator.run()


def run_db_replicator(args, config: Settings):
    if not args.db:
        raise Exception("need to pass --db argument")

    set_logging_config(f'dbrepl {args.db}')

    db_replicator = DbReplicator(
        config=config,
        database=args.db,
        target_database=getattr(args, 'target_db', None),
        initial_only=args.initial_only,
    )
    db_replicator.run()


def run_monitoring(args, config: Settings):
    set_logging_config('monitor')
    monitoring = Monitoring(args.db or '', config)
    monitoring.run()


def run_all(args, config: Settings):
    set_logging_config('runner')
    runner = Runner(config, args.wait_initial_replication, args.db)
    runner.run()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "mode", help="run mode",
        type=str,
        choices=["run_all", "binlog_replicator", "db_replicator", "monitoring"])
    parser.add_argument("--config", help="config file path", default='config.yaml', type=str)
    parser.add_argument("--db", help="source database(s) name", type=str)
    parser.add_argument("--target_db", help="target database(s) name, if not set will be same as source", type=str)
    parser.add_argument("--wait_initial_replication", type=bool, default=True)
    parser.add_argument(
        "--initial_only", type=bool, default=False,
        help="don't run realtime replication, run initial replication only",
    )
    args = parser.parse_args()

    config = Settings()
    config.load(args.config)
    if args.mode == 'binlog_replicator':
        run_binlog_replicator(args, config)
    if args.mode == 'db_replicator':
        run_db_replicator(args, config)
    if args.mode == 'monitoring':
        run_monitoring(args, config)
    if args.mode == 'run_all':
        run_all(args, config)


if __name__ == '__main__':
    main()
