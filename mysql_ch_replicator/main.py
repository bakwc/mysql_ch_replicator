#!/usr/bin/env python3

import argparse
import logging
from logging.handlers import RotatingFileHandler
import sys
import os

from .config import Settings
from .db_replicator import DbReplicator
from .binlog_replicator import BinlogReplicator
from .db_optimizer import DbOptimizer
from .monitoring import Monitoring
from .runner import Runner


def set_logging_config(tags, log_file=None, log_level_str=None):

    handlers = []
    handlers.append(logging.StreamHandler(sys.stderr))
    if log_file is not None:
        handlers.append(
            RotatingFileHandler(
                filename=log_file,
                maxBytes=50*1024*1024,  # 50 Mb
                backupCount=3,
                encoding='utf-8',
                delay=False,
            )
        )

    log_levels = {
        'critical': logging.CRITICAL,
        'error': logging.ERROR,
        'warning': logging.WARNING,
        'info': logging.INFO,
        'debug': logging.DEBUG,
    }

    log_level = log_levels.get(log_level_str)
    if log_level is None:
        print(f'[warning] unknown log level {log_level_str}, setting info')
        log_level = 'info'

    logging.basicConfig(
        level=log_level,
        format=f'[{tags} %(asctime)s %(levelname)8s] %(message)s',
        handlers=handlers,
    )


def run_binlog_replicator(args, config: Settings):
    if not os.path.exists(config.binlog_replicator.data_dir):
        os.mkdir(config.binlog_replicator.data_dir)

    log_file = os.path.join(
        config.binlog_replicator.data_dir,
        'binlog_replicator.log',
    )

    set_logging_config('binlogrepl', log_file=log_file, log_level_str=config.log_level)
    binlog_replicator = BinlogReplicator(
        settings=config,
    )
    binlog_replicator.run()


def run_db_replicator(args, config: Settings):
    if not args.db:
        raise Exception("need to pass --db argument")

    db_name = args.db

    if not os.path.exists(config.binlog_replicator.data_dir):
        os.mkdir(config.binlog_replicator.data_dir)

    db_dir = os.path.join(
        config.binlog_replicator.data_dir,
        db_name,
    )

    if not os.path.exists(db_dir):
        os.mkdir(db_dir)

    log_file = os.path.join(
        db_dir,
        'db_replicator.log',
    )

    set_logging_config(f'dbrepl {args.db}', log_file=log_file, log_level_str=config.log_level)

    db_replicator = DbReplicator(
        config=config,
        database=db_name,
        target_database=getattr(args, 'target_db', None),
        initial_only=args.initial_only,
    )
    db_replicator.run()


def run_db_optimizer(args, config: Settings):
    data_dir = config.binlog_replicator.data_dir
    if not os.path.exists(data_dir):
        os.mkdir(data_dir)

    log_file = os.path.join(
        data_dir,
        'db_optimizer.log',
    )

    set_logging_config(f'dbopt {args.db}', log_file=log_file, log_level_str=config.log_level)

    db_optimizer = DbOptimizer(
        config=config,
    )
    db_optimizer.run()


def run_monitoring(args, config: Settings):
    set_logging_config('monitor', log_level_str=config.log_level)
    monitoring = Monitoring(args.db or '', config)
    monitoring.run()


def run_all(args, config: Settings):
    set_logging_config('runner', log_level_str=config.log_level)
    runner = Runner(config, args.wait_initial_replication, args.db)
    runner.run()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "mode", help="run mode",
        type=str,
        choices=["run_all", "binlog_replicator", "db_replicator", "monitoring", "db_optimizer"])
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
    if args.mode == 'db_optimizer':
        run_db_optimizer(args, config)
    if args.mode == 'monitoring':
        run_monitoring(args, config)
    if args.mode == 'run_all':
        run_all(args, config)


if __name__ == '__main__':
    main()
