"""Unit tests for MySQL connection pooling functionality"""

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import pytest

from mysql_ch_replicator.config import MysqlSettings
from mysql_ch_replicator.connection_pool import get_pool_manager
from mysql_ch_replicator.mysql_api import MySQLApi

# Set up logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


@pytest.mark.unit
def test_basic_pooling():
    """Test basic connection pooling functionality"""
    logger.info("Testing basic connection pooling...")

    mysql_settings = MysqlSettings(
        host="localhost",
        port=3306,
        user="root",
        password="",
        pool_size=3,
        max_overflow=2,
        pool_name="test_pool",
    )

    # Create multiple MySQLApi instances - they should share the same pool
    api1 = MySQLApi(database=None, mysql_settings=mysql_settings)
    api2 = MySQLApi(database=None, mysql_settings=mysql_settings)

    # Verify they use the same pool
    assert api1.connection_pool is api2.connection_pool, (
        "APIs should share the same connection pool"
    )
    logger.info("✓ Multiple MySQLApi instances share the same connection pool")

    try:
        # Test basic operations
        databases = api1.get_databases()
        logger.info(f"✓ Successfully retrieved {len(databases)} databases")

        # Test with different API instance
        databases2 = api2.get_databases()
        assert databases == databases2, "Both APIs should return the same results"
        logger.info("✓ Both API instances return consistent results")

    except Exception as e:
        logger.error(f"Basic pooling test failed: {e}")
        raise


@pytest.mark.unit
def test_concurrent_access():
    """Test concurrent access to the connection pool"""
    logger.info("Testing concurrent access to connection pool...")

    mysql_settings = MysqlSettings(
        host="localhost",
        port=3306,
        user="root",
        password="",
        pool_size=2,
        max_overflow=3,
        pool_name="concurrent_test_pool",
    )

    def worker(worker_id):
        """Worker function for concurrent testing"""
        api = MySQLApi(database=None, mysql_settings=mysql_settings)
        start_time = time.time()

        try:
            databases = api.get_databases()
            elapsed = time.time() - start_time
            logger.info(
                f"Worker {worker_id}: Retrieved {len(databases)} databases in {elapsed:.3f}s"
            )
            return worker_id, len(databases), elapsed
        except Exception as e:
            logger.error(f"Worker {worker_id} failed: {e}")
            raise

    # Run multiple workers concurrently
    num_workers = 5
    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = [executor.submit(worker, i) for i in range(num_workers)]

        results = []
        for future in as_completed(futures):
            results.append(future.result())

    logger.info(f"✓ {len(results)} concurrent workers completed successfully")

    # Verify all workers got the same number of databases
    db_counts = [result[1] for result in results]
    assert all(count == db_counts[0] for count in db_counts), (
        "All workers should get same database count"
    )
    logger.info("✓ All concurrent workers returned consistent results")


@pytest.mark.unit
def test_pool_reuse():
    """Test that connection pools are properly reused"""
    logger.info("Testing connection pool reuse...")

    pool_manager = get_pool_manager()
    initial_pool_count = len(pool_manager._pools)

    mysql_settings = MysqlSettings(
        host="localhost",
        port=3306,
        user="root",
        password="",
        pool_size=2,
        max_overflow=1,
        pool_name="reuse_test_pool",
    )

    # Create multiple API instances with same settings
    apis = []
    for i in range(3):
        api = MySQLApi(database=None, mysql_settings=mysql_settings)
        apis.append(api)

    # Should only have created one additional pool
    final_pool_count = len(pool_manager._pools)
    assert final_pool_count == initial_pool_count + 1, (
        f"Expected {initial_pool_count + 1} pools, got {final_pool_count}"
    )
    logger.info("✓ Connection pool properly reused across multiple API instances")

    # All APIs should reference the same pool
    first_pool = apis[0].connection_pool
    for i, api in enumerate(apis[1:], 1):
        assert api.connection_pool is first_pool, (
            f"API {i} should use the same pool as API 0"
        )

    logger.info("✓ All API instances reference the same connection pool object")


@pytest.mark.unit
def test_pool_configuration():
    """Test that pool configuration is applied correctly"""
    mysql_settings = MysqlSettings(
        host="localhost",
        port=3306,
        user="root",
        password="",
        pool_size=8,
        max_overflow=5,
        pool_name="config_test_pool",
    )

    pool_manager = get_pool_manager()
    pool = pool_manager.get_or_create_pool(
        mysql_settings=mysql_settings,
        pool_name=mysql_settings.pool_name,
        pool_size=mysql_settings.pool_size,
        max_overflow=mysql_settings.max_overflow,
    )

    # Verify pool was created with correct settings
    # Note: pool_size + max_overflow is capped at 32
    expected_pool_size = min(mysql_settings.pool_size + mysql_settings.max_overflow, 32)
    assert pool.pool_size == expected_pool_size


def test_pool_cleanup():
    """Test pool cleanup functionality"""
    pool_manager = get_pool_manager()

    # Create a pool
    mysql_settings = MysqlSettings(
        host="localhost",
        port=3306,
        user="root",
        password="",
        pool_size=2,
        max_overflow=1,
        pool_name="cleanup_test_pool",
    )

    pool = pool_manager.get_or_create_pool(
        mysql_settings=mysql_settings,
        pool_name=mysql_settings.pool_name,
        pool_size=mysql_settings.pool_size,
        max_overflow=mysql_settings.max_overflow,
    )

    assert len(pool_manager._pools) > 0

    # Clean up all pools
    pool_manager.close_all_pools()

    # Verify pools dict was cleared
    assert len(pool_manager._pools) == 0
