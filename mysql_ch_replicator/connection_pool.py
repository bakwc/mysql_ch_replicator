"""MySQL Connection Pool Manager for mysql-ch-replicator"""

import hashlib
import threading
from logging import getLogger

from mysql.connector import Error as MySQLError
from mysql.connector.pooling import MySQLConnectionPool

from .config import MysqlSettings

logger = getLogger(__name__)


class ConnectionPoolManager:
    """Singleton connection pool manager for MySQL connections"""

    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if not self._initialized:
            self._pools = {}
            self._initialized = True

    def _generate_short_pool_name(self, pool_key: str, user: str) -> str:
        """
        Generate shortened pool name for MySQLConnectionPool.

        MySQL connector limits pool names to ~64 characters. This method creates
        a deterministic short name while keeping the full pool_key for internal
        dictionary lookups.

        Args:
            pool_key: Full composite key (host:port:user:pool_name)
            user: MySQL username for human-readable prefix

        Returns:
            Shortened pool name in format: pool_{user}_{hash8}
        """
        hash_digest = hashlib.sha256(pool_key.encode("utf-8")).hexdigest()[:8]
        safe_user = user[:16] if len(user) > 16 else user
        short_name = f"pool_{safe_user}_{hash_digest}"
        logger.debug(
            f"Generated short pool name '{short_name}' for pool key '{pool_key}'"
        )
        return short_name

    def get_or_create_pool(
        self,
        mysql_settings: MysqlSettings,
        pool_name: str = "default",
        pool_size: int = 5,
        max_overflow: int = 10,
    ) -> MySQLConnectionPool:
        """
        Get or create a connection pool for the given MySQL settings

        Args:
            mysql_settings: MySQL connection configuration
            pool_name: Name of the connection pool
            pool_size: Number of connections to maintain in pool
            max_overflow: Maximum number of additional connections beyond pool_size

        Returns:
            MySQLConnectionPool instance
        """
        pool_key = f"{mysql_settings.host}:{mysql_settings.port}:{mysql_settings.user}:{pool_name}"

        if pool_key not in self._pools:
            with self._lock:
                if pool_key not in self._pools:
                    try:
                        # Use standardized connection configuration
                        config = mysql_settings.get_connection_config(autocommit=True)

                        # Calculate actual pool size (base + overflow)
                        actual_pool_size = min(
                            pool_size + max_overflow, 32
                        )  # MySQL max connections per user

                        # Generate shortened pool name for MySQL connector
                        # (MySQL limits pool names to ~64 characters)
                        short_pool_name = self._generate_short_pool_name(
                            pool_key, mysql_settings.user
                        )

                        self._pools[pool_key] = MySQLConnectionPool(
                            pool_name=short_pool_name,
                            pool_size=actual_pool_size,
                            pool_reset_session=True,
                            **config,
                        )

                        logger.info(
                            f"Created MySQL connection pool '{short_pool_name}' "
                            f"(key: '{pool_key}') with {actual_pool_size} connections"
                        )

                    except MySQLError as e:
                        logger.error(
                            f"Failed to create connection pool '{pool_key}': {e}"
                        )
                        raise

        return self._pools[pool_key]

    def close_all_pools(self):
        """Close all connection pools"""
        with self._lock:
            for pool_name, pool in self._pools.items():
                try:
                    # MySQL connector doesn't have explicit pool close, connections auto-close
                    logger.info(f"Connection pool '{pool_name}' will be cleaned up")
                except Exception as e:
                    logger.warning(f"Error closing pool '{pool_name}': {e}")
            self._pools.clear()


class PooledConnection:
    """Context manager for pooled MySQL connections"""

    def __init__(self, pool: MySQLConnectionPool):
        self.pool = pool
        self.connection = None
        self.cursor = None

    def __enter__(self):
        try:
            self.connection = self.pool.get_connection()
            self.cursor = self.connection.cursor()
            return self.connection, self.cursor
        except MySQLError as e:
            logger.error(f"Failed to get connection from pool: {e}")
            raise

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()  # Returns connection to pool

        # Log any exceptions that occurred
        if exc_type is not None:
            logger.error(f"Error in pooled connection: {exc_val}")


def get_pool_manager() -> ConnectionPoolManager:
    """Get the singleton connection pool manager"""
    return ConnectionPoolManager()
