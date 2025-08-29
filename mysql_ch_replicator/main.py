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
        # Ensure log file directory exists before creating handler
        log_dir = os.path.dirname(log_file)
        if log_dir:
            try:
                os.makedirs(log_dir, exist_ok=True)
            except FileNotFoundError:
                # Handle nested directory creation for isolated test paths
                try:
                    # Create all parent directories recursively
                    os.makedirs(os.path.dirname(log_dir), exist_ok=True)
                    os.makedirs(log_dir, exist_ok=True)
                except Exception as e:
                    print(f"Warning: Could not create log directory {log_dir}: {e}")
                    # Skip file logging if directory creation fails
                    log_file = None
        
        # Only add file handler if log directory was created successfully
        if log_file is not None:
            handlers.append(
                RotatingFileHandler(
                    filename=log_file,
                    maxBytes=50*1024*1024,  # 50 Mb
                    backupCount=3,
                    encoding='utf-8',
                    delay=True,  # Defer file creation until first log
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
    # Ensure the binlog data directory exists with robust error handling
    try:
        os.makedirs(config.binlog_replicator.data_dir, exist_ok=True)
    except FileNotFoundError as e:
        # If parent directory doesn't exist, create it recursively
        parent_dir = os.path.dirname(config.binlog_replicator.data_dir)
        os.makedirs(parent_dir, exist_ok=True)
        os.makedirs(config.binlog_replicator.data_dir, exist_ok=True)

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

    # Ensure the binlog data directory exists with robust error handling  
    # CRITICAL: Support parallel test isolation patterns like /app/binlog_{worker_id}_{test_id}/
    try:
        os.makedirs(config.binlog_replicator.data_dir, exist_ok=True)
    except FileNotFoundError as e:
        # If parent directory doesn't exist, create it recursively
        # This handles deep paths like /app/binlog_gw1_test123/
        parent_dir = os.path.dirname(config.binlog_replicator.data_dir)
        if parent_dir and parent_dir != config.binlog_replicator.data_dir:
            os.makedirs(parent_dir, exist_ok=True)
        os.makedirs(config.binlog_replicator.data_dir, exist_ok=True)
    except Exception as e:
        # Handle any other filesystem issues (permissions, disk space)
        logging.warning(f"Could not create binlog directory {config.binlog_replicator.data_dir}: {e}")
        # Continue execution - logging will use parent directory or fail gracefully

    db_dir = os.path.join(
        config.binlog_replicator.data_dir,
        db_name,
    )

    # Create database-specific directory with robust error handling
    # CRITICAL: This prevents FileNotFoundError in isolated test scenarios
    try:
        os.makedirs(db_dir, exist_ok=True)
    except FileNotFoundError as e:
        # Ensure parent directories exist recursively - handle isolated test paths
        try:
            # Create full directory hierarchy recursively
            os.makedirs(os.path.dirname(config.binlog_replicator.data_dir), exist_ok=True)
            os.makedirs(config.binlog_replicator.data_dir, exist_ok=True)
            os.makedirs(db_dir, exist_ok=True)
        except Exception as e2:
            logging.warning(f"Could not create database directory hierarchy {db_dir}: {e2}")
    except Exception as e:
        # Handle filesystem issues gracefully
        logging.warning(f"Could not create database directory {db_dir}: {e}")
        # Continue execution - logging will attempt to create directory when needed

    log_file = os.path.join(
        db_dir,
        'db_replicator.log',
    )

    # Set log tag according to whether this is a worker or main process
    if args.worker_id is not None:
        if args.table:
            log_tag = f'dbrepl {db_name} worker_{args.worker_id} table_{args.table}'
        else:
            log_tag = f'dbrepl {db_name} worker_{args.worker_id}'
    else:
        log_tag = f'dbrepl {db_name}'

    set_logging_config(log_tag, log_file=log_file, log_level_str=config.log_level)

    if args.table:
        logging.info(f"Processing specific table: {args.table}")

    db_replicator = DbReplicator(
        config=config,
        database=db_name,
        target_database=getattr(args, 'target_db', None),
        initial_only=args.initial_only,
        worker_id=args.worker_id,
        total_workers=args.total_workers,
        table=args.table,
        initial_replication_test_fail_records=getattr(args, 'initial_replication_test_fail_records', None),
    )
    db_replicator.run()


def run_db_optimizer(args, config: Settings):
    data_dir = config.binlog_replicator.data_dir
    if not os.path.exists(data_dir):
        os.makedirs(data_dir, exist_ok=True)

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
    parser.add_argument(
        "--worker_id", type=int, default=None,
        help="Worker ID for parallel initial replication (0-based)",
    )
    parser.add_argument(
        "--total_workers", type=int, default=None,
        help="Total number of workers for parallel initial replication",
    )
    parser.add_argument(
        "--table", type=str, default=None,
        help="Specific table to process (used with --worker_id for parallel processing of a single table)",
    )
    parser.add_argument(
        "--initial-replication-test-fail-records", type=int, default=None,
        help="FOR TESTING ONLY: Exit initial replication after processing this many records",
    )
    args = parser.parse_args()

    config = Settings()
    config.load(args.config)
    
    # CRITICAL SAFETY: Force directory creation again immediately after config loading
    # This is essential for Docker volume mount scenarios where the host directory 
    # may override container directories or be empty
    try:
        os.makedirs(config.binlog_replicator.data_dir, exist_ok=True)
    except Exception as e:
        print(f"Warning: Could not ensure binlog directory exists: {e}")
        # Try to create with full path
        try:
            parent_dir = os.path.dirname(config.binlog_replicator.data_dir)
            if parent_dir:
                os.makedirs(parent_dir, exist_ok=True)
            os.makedirs(config.binlog_replicator.data_dir, exist_ok=True)
        except Exception as e2:
            print(f"CRITICAL: Failed to create binlog directory: {e2}")
            # This will likely cause failures but let's continue to see the specific error
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
