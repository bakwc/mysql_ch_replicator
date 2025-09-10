"""
MySQL to ClickHouse Replicator Configuration Management

This module provides configuration classes and utilities for managing the replication
system settings including database connections, replication behavior, and data handling.

Classes:
    MysqlSettings: MySQL database connection configuration with connection pooling
    ClickhouseSettings: ClickHouse database connection configuration  
    BinlogReplicatorSettings: Binary log replication behavior configuration
    Index: Database/table-specific index configuration
    PartitionBy: Database/table-specific partitioning configuration
    Settings: Main configuration class that orchestrates all settings

Key Features:
    - YAML-based configuration loading
    - Connection pool management for MySQL
    - Database/table filtering with pattern matching
    - Type validation and error handling
    - Timezone handling for MySQL connections
    - Directory management for binlog data
"""

import fnmatch
import zoneinfo
from dataclasses import dataclass

import yaml


def stype(obj):
    """Get the simple type name of an object.
    
    Args:
        obj: Any object to get type name for
        
    Returns:
        str: Simple class name of the object's type
        
    Example:
        >>> stype([1, 2, 3])
        'list'
        >>> stype("hello")
        'str'
    """
    return type(obj).__name__


@dataclass
class MysqlSettings:
    """MySQL database connection configuration with connection pool support.
    
    Supports MySQL 5.7+, MySQL 8.0+, MariaDB 10.x, and Percona Server.
    Includes connection pooling configuration for high-performance replication.
    
    Attributes:
        host: MySQL server hostname or IP address
        port: MySQL server port (default: 3306)
        user: MySQL username for authentication
        password: MySQL password for authentication
        pool_size: Base number of connections in pool (default: 5)
        max_overflow: Maximum additional connections beyond pool_size (default: 10)
        pool_name: Identifier for connection pool (default: "default")
        charset: Character set for connection (MariaDB compatibility, optional)
        collation: Collation for connection (MariaDB compatibility, optional)
        
    Example:
        mysql_config = MysqlSettings(
            host="mysql.example.com",
            port=3306,
            user="replicator",
            password="secure_password",
            pool_size=10,
            charset="utf8mb4"
        )
    """
    host: str = "localhost"
    port: int = 3306
    user: str = "root"
    password: str = ""
    # Connection pool settings for high-performance replication
    pool_size: int = 5
    max_overflow: int = 10
    pool_name: str = "default"
    # Optional charset specification (critical for MariaDB compatibility)
    charset: str = None
    # Optional collation specification (critical for MariaDB compatibility)  
    collation: str = None

    def validate(self):
        if not isinstance(self.host, str):
            raise ValueError(f"mysql host should be string and not {stype(self.host)}")

        if not isinstance(self.port, int):
            raise ValueError(f"mysql port should be int and not {stype(self.port)}")

        if not isinstance(self.user, str):
            raise ValueError(f"mysql user should be string and not {stype(self.user)}")

        if not isinstance(self.password, str):
            raise ValueError(
                f"mysql password should be string and not {stype(self.password)}"
            )

        if not isinstance(self.pool_size, int) or self.pool_size < 1:
            raise ValueError(
                f"mysql pool_size should be positive integer and not {stype(self.pool_size)}"
            )

        if not isinstance(self.max_overflow, int) or self.max_overflow < 0:
            raise ValueError(
                f"mysql max_overflow should be non-negative integer and not {stype(self.max_overflow)}"
            )

        if not isinstance(self.pool_name, str):
            raise ValueError(
                f"mysql pool_name should be string and not {stype(self.pool_name)}"
            )

        if self.charset is not None and not isinstance(self.charset, str):
            raise ValueError(
                f"mysql charset should be string or None and not {stype(self.charset)}"
            )

        if self.collation is not None and not isinstance(self.collation, str):
            raise ValueError(
                f"mysql collation should be string or None and not {stype(self.collation)}"
            )

    def get_connection_config(self, database=None, autocommit=True):
        """Build standardized MySQL connection configuration"""
        config = {
            "host": self.host,
            "port": self.port,
            "user": self.user,
            "password": self.password,
            "autocommit": autocommit,
        }

        # Add database if specified
        if database is not None:
            config["database"] = database

        # Add charset if specified (important for MariaDB compatibility)
        if self.charset is not None:
            config["charset"] = self.charset

        # Add collation if specified (important for MariaDB compatibility)
        if self.collation is not None:
            config["collation"] = self.collation

        return config


@dataclass
class Index:
    databases: str | list = "*"
    tables: str | list = "*"
    index: str = ""


@dataclass
class PartitionBy:
    databases: str | list = "*"
    tables: str | list = "*"
    partition_by: str = ""


@dataclass
class ClickhouseSettings:
    host: str = "localhost"
    port: int = 3306
    user: str = "root"
    password: str = ""
    connection_timeout: int = 30
    send_receive_timeout: int = 120

    def validate(self):
        if not isinstance(self.host, str):
            raise ValueError(
                f"clickhouse host should be string and not {stype(self.host)}"
            )

        if not isinstance(self.port, int):
            raise ValueError(
                f"clickhouse port should be int and not {stype(self.port)}"
            )

        if not isinstance(self.user, str):
            raise ValueError(
                f"clickhouse user should be string and not {stype(self.user)}"
            )

        if not isinstance(self.password, str):
            raise ValueError(
                f"clickhouse password should be string and not {stype(self.password)}"
            )

        if not isinstance(self.connection_timeout, int):
            raise ValueError(
                f"clickhouse connection_timeout should be int and not {stype(self.connection_timeout)}"
            )

        if not isinstance(self.send_receive_timeout, int):
            raise ValueError(
                f"clickhouse send_receive_timeout should be int and not {stype(self.send_receive_timeout)}"
            )

        if self.connection_timeout <= 0:
            raise ValueError("connection timeout should be at least 1 second")

        if self.send_receive_timeout <= 0:
            raise ValueError("send_receive_timeout timeout should be at least 1 second")


@dataclass
class BinlogReplicatorSettings:
    data_dir: str = "binlog"
    records_per_file: int = 100000
    binlog_retention_period: int = 43200  # 12 hours in seconds

    def validate(self):
        if not isinstance(self.data_dir, str):
            raise ValueError(
                f"binlog_replicator data_dir should be string and not {stype(self.data_dir)}"
            )

        if not isinstance(self.records_per_file, int):
            raise ValueError(
                f"binlog_replicator records_per_file should be int and not {stype(self.data_dir)}"
            )

        if self.records_per_file <= 0:
            raise ValueError("binlog_replicator records_per_file should be positive")

        if not isinstance(self.binlog_retention_period, int):
            raise ValueError(
                f"binlog_replicator binlog_retention_period should be int and not {stype(self.binlog_retention_period)}"
            )

        if self.binlog_retention_period <= 0:
            raise ValueError(
                "binlog_replicator binlog_retention_period should be positive"
            )


class Settings:
    DEFAULT_LOG_LEVEL = "info"
    DEFAULT_OPTIMIZE_INTERVAL = 86400
    DEFAULT_CHECK_DB_UPDATED_INTERVAL = 120
    DEFAULT_AUTO_RESTART_INTERVAL = 3600
    DEFAULT_INITIAL_REPLICATION_BATCH_SIZE = 50000

    def __init__(self):
        self.mysql = MysqlSettings()
        self.clickhouse = ClickhouseSettings()
        self.binlog_replicator = BinlogReplicatorSettings()
        self.databases = ""
        self.tables = "*"
        self.exclude_databases = ""
        self.exclude_tables = ""
        self.settings_file = ""
        self.log_level = "info"
        self.debug_log_level = False
        self.optimize_interval = 0
        self.check_db_updated_interval = 0
        self.indexes: list[Index] = []
        self.partition_bys: list[PartitionBy] = []
        self.auto_restart_interval = 0
        self.http_host = ""
        self.http_port = 0
        self.types_mapping = {}
        self.target_databases = {}
        self.initial_replication_threads = 0
        self.ignore_deletes = False
        self.mysql_timezone = "UTC"
        self.initial_replication_batch_size = 50000

    def load(self, settings_file):
        data = open(settings_file, "r").read()
        data = yaml.safe_load(data)

        self.settings_file = settings_file
        self.mysql = MysqlSettings(**data.pop("mysql"))
        self.clickhouse = ClickhouseSettings(**data.pop("clickhouse"))
        self.databases = data.pop("databases")
        self.tables = data.pop("tables", "*")
        self.exclude_databases = data.pop("exclude_databases", "")
        self.exclude_tables = data.pop("exclude_tables", "")
        self.log_level = data.pop("log_level", Settings.DEFAULT_LOG_LEVEL)
        self.optimize_interval = data.pop(
            "optimize_interval", Settings.DEFAULT_OPTIMIZE_INTERVAL
        )
        self.check_db_updated_interval = data.pop(
            "check_db_updated_interval",
            Settings.DEFAULT_CHECK_DB_UPDATED_INTERVAL,
        )
        self.auto_restart_interval = data.pop(
            "auto_restart_interval",
            Settings.DEFAULT_AUTO_RESTART_INTERVAL,
        )
        self.types_mapping = data.pop("types_mapping", {})
        self.http_host = data.pop("http_host", "")
        self.http_port = data.pop("http_port", 0)
        self.target_databases = data.pop("target_databases", {})
        self.initial_replication_threads = data.pop("initial_replication_threads", 0)
        self.ignore_deletes = data.pop("ignore_deletes", False)
        self.mysql_timezone = data.pop("mysql_timezone", "UTC")
        self.initial_replication_batch_size = data.pop(
            "initial_replication_batch_size",
            Settings.DEFAULT_INITIAL_REPLICATION_BATCH_SIZE,
        )

        indexes = data.pop("indexes", [])
        for index in indexes:
            self.indexes.append(Index(**index))

        partition_bys = data.pop("partition_bys", [])
        for partition_by in partition_bys:
            self.partition_bys.append(PartitionBy(**partition_by))

        assert isinstance(self.databases, str) or isinstance(self.databases, list)
        assert isinstance(self.tables, str) or isinstance(self.tables, list)
        self.binlog_replicator = BinlogReplicatorSettings(
            **data.pop("binlog_replicator")
        )
        
        # CRITICAL: Ensure binlog directory exists immediately after configuration loading
        # This prevents race conditions in parallel test execution and container startup
        import os
        import shutil
        
        # Special handling for Docker volume mount issues where directory exists but can't be written to
        try:
            # CRITICAL: Ensure parent directories exist first
            # This fixes the issue where isolated test paths like /app/binlog/w3_75f29622 
            # don't have their parent directories created yet
            parent_dir = os.path.dirname(self.binlog_replicator.data_dir)
            if parent_dir and not os.path.exists(parent_dir):
                os.makedirs(parent_dir, exist_ok=True)
                print(f"DEBUG: Created parent directory: {parent_dir}")
            
            # Now ensure the target directory exists
            if not os.path.exists(self.binlog_replicator.data_dir):
                os.makedirs(self.binlog_replicator.data_dir, exist_ok=True)
                print(f"DEBUG: Created binlog directory: {self.binlog_replicator.data_dir}")
            
            # Test if we can actually create files in the directory
            test_file = os.path.join(self.binlog_replicator.data_dir, ".test_write")
            try:
                with open(test_file, "w") as f:
                    f.write("test")
                os.remove(test_file)
                # Directory works, we're good
                print(f"DEBUG: Binlog directory writability confirmed: {self.binlog_replicator.data_dir}")
            except (OSError, IOError) as e:
                print(f"DEBUG: Directory exists but not writable, recreating: {e}")
                # Directory exists but is not writable, recreate it
                shutil.rmtree(self.binlog_replicator.data_dir, ignore_errors=True)
                os.makedirs(self.binlog_replicator.data_dir, exist_ok=True)
                # Test write again after recreation
                try:
                    with open(test_file, "w") as f:
                        f.write("test")
                    os.remove(test_file)
                    print(f"DEBUG: Binlog directory successfully recreated and writable: {self.binlog_replicator.data_dir}")
                except (OSError, IOError) as e2:
                    print(f"WARNING: Binlog directory still not writable after recreation: {e2}")
                
        except Exception as e:
            print(f"WARNING: Could not ensure binlog directory is writable: {e}")
            # Fallback - try creating anyway
            try:
                os.makedirs(self.binlog_replicator.data_dir, exist_ok=True)
                print(f"DEBUG: Fallback directory creation successful: {self.binlog_replicator.data_dir}")
            except Exception as e2:
                print(f"CRITICAL: Final binlog directory creation failed: {e2}")
        
        if data:
            raise Exception(f"Unsupported config options: {list(data.keys())}")
        self.validate()

    @classmethod
    def is_pattern_matches(cls, substr, pattern):
        if not pattern or pattern == "*":
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
        if self.exclude_databases and self.is_pattern_matches(
            db_name, self.exclude_databases
        ):
            return False
        return self.is_pattern_matches(db_name, self.databases)

    def is_table_matches(self, table_name):
        if self.exclude_tables and self.is_pattern_matches(
            table_name, self.exclude_tables
        ):
            return False
        return self.is_pattern_matches(table_name, self.tables)

    def validate_log_level(self):
        if self.log_level not in ["critical", "error", "warning", "info", "debug"]:
            raise ValueError(f"wrong log level {self.log_level}")
        if self.log_level == "debug":
            self.debug_log_level = True

    def validate_mysql_timezone(self):
        if not isinstance(self.mysql_timezone, str):
            raise ValueError(
                f"mysql_timezone should be string and not {stype(self.mysql_timezone)}"
            )

        # Validate timezone by attempting to import and check if it's valid
        try:
            zoneinfo.ZoneInfo(self.mysql_timezone)
        except zoneinfo.ZoneInfoNotFoundError:
            raise ValueError(
                f'invalid timezone: {self.mysql_timezone}. Use IANA timezone names like "UTC", "Europe/London", "America/New_York", etc.'
            )

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
            raise ValueError(f"wrong target databases {self.target_databases}")
        if not isinstance(self.initial_replication_threads, int):
            raise ValueError(
                f"initial_replication_threads should be an integer, not {type(self.initial_replication_threads)}"
            )
        if self.initial_replication_threads < 0:
            raise ValueError("initial_replication_threads should be non-negative")
        self.validate_mysql_timezone()
