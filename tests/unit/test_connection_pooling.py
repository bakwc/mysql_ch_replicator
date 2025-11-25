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

# Database configurations for testing different DB types
DB_CONFIGS = [
    pytest.param(
        {"host": "localhost", "port": 9306, "name": "MySQL"}, 
        id="mysql"
    ),
    pytest.param(
        {"host": "localhost", "port": 9307, "name": "MariaDB"}, 
        id="mariadb"
    ),
]


@pytest.mark.unit
@pytest.mark.parametrize("db_config", DB_CONFIGS)
def test_basic_pooling(db_config):
    """Test basic connection pooling functionality"""
    logger.info(f"Testing basic connection pooling for {db_config['name']}...")
    
    # Use compatible collation for MariaDB
    collation = "utf8mb4_general_ci" if db_config["name"] == "MariaDB" else None

    mysql_settings = MysqlSettings(
        host=db_config["host"],
        port=db_config["port"],
        user="root",
        password="admin",
        pool_size=3,
        max_overflow=2,
        pool_name=f"test_pool_{db_config['name'].lower()}",
        collation=collation,
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
@pytest.mark.parametrize("db_config", DB_CONFIGS)
def test_concurrent_access(db_config):
    """Test concurrent access to the connection pool"""
    logger.info(f"Testing concurrent access to connection pool for {db_config['name']}...")
    
    # Use compatible collation for MariaDB
    collation = "utf8mb4_general_ci" if db_config["name"] == "MariaDB" else None

    mysql_settings = MysqlSettings(
        host=db_config["host"],
        port=db_config["port"],
        user="root",
        password="admin",
        pool_size=2,
        max_overflow=3,
        pool_name=f"concurrent_test_pool_{db_config['name'].lower()}",
        collation=collation,
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
@pytest.mark.parametrize("db_config", DB_CONFIGS)
def test_pool_reuse(db_config):
    """Test that connection pools are properly reused"""
    logger.info(f"Testing connection pool reuse for {db_config['name']}...")

    pool_manager = get_pool_manager()
    initial_pool_count = len(pool_manager._pools)
    
    # Use compatible collation for MariaDB
    collation = "utf8mb4_general_ci" if db_config["name"] == "MariaDB" else None

    mysql_settings = MysqlSettings(
        host=db_config["host"],
        port=db_config["port"],
        user="root",
        password="admin",
        pool_size=2,
        max_overflow=1,
        pool_name=f"reuse_test_pool_{db_config['name'].lower()}",
        collation=collation,
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
@pytest.mark.parametrize("db_config", DB_CONFIGS)
def test_pool_configuration(db_config):
    """Test that pool configuration is applied correctly"""
    # Use compatible collation for MariaDB
    collation = "utf8mb4_general_ci" if db_config["name"] == "MariaDB" else None
    
    mysql_settings = MysqlSettings(
        host=db_config["host"],
        port=db_config["port"],
        user="root",
        password="admin",
        pool_size=8,
        max_overflow=5,
        pool_name=f"config_test_pool_{db_config['name'].lower()}",
        collation=collation,
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


@pytest.mark.parametrize("db_config", DB_CONFIGS)
def test_pool_cleanup(db_config):
    """Test pool cleanup functionality"""
    pool_manager = get_pool_manager()
    
    # Use compatible collation for MariaDB
    collation = "utf8mb4_general_ci" if db_config["name"] == "MariaDB" else None

    # Create a pool
    mysql_settings = MysqlSettings(
        host=db_config["host"],
        port=db_config["port"],
        user="root",
        password="admin",
        pool_size=2,
        max_overflow=1,
        pool_name=f"cleanup_test_pool_{db_config['name'].lower()}",
        collation=collation,
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


@pytest.mark.unit
def test_long_hostname_pool_name():
    """Test that long AWS RDS hostnames don't cause pool name errors"""
    from mysql_ch_replicator.connection_pool import ConnectionPoolManager

    manager = ConnectionPoolManager()

    # Simulate a long AWS RDS hostname (74 chars total - exceeds 64 limit)
    long_pool_key = (
        "production.c1qddbamxlcn.us-east-1.rds.amazonaws.com:3306:replicator:default"
    )

    short_name = manager._generate_short_pool_name(long_pool_key, "replicator")

    assert len(short_name) <= 64, f"Pool name '{short_name}' exceeds 64 characters"
    assert short_name.startswith("pool_replicator_"), "Should have expected prefix"
    logger.info(f"✓ Long hostname shortened: '{long_pool_key}' -> '{short_name}'")


@pytest.mark.unit
def test_short_pool_name_deterministic():
    """Test that the same inputs produce the same shortened pool name"""
    from mysql_ch_replicator.connection_pool import ConnectionPoolManager

    manager = ConnectionPoolManager()
    pool_key = "production.example.com:3306:user:default"

    name1 = manager._generate_short_pool_name(pool_key, "user")
    name2 = manager._generate_short_pool_name(pool_key, "user")

    assert name1 == name2, "Same inputs should produce same output"
    logger.info(f"✓ Pool name generation is deterministic: '{name1}'")


@pytest.mark.unit
def test_short_pool_name_uniqueness():
    """Test that different inputs produce different shortened pool names"""
    from mysql_ch_replicator.connection_pool import ConnectionPoolManager

    manager = ConnectionPoolManager()

    name1 = manager._generate_short_pool_name("host1:3306:user:default", "user")
    name2 = manager._generate_short_pool_name("host2:3306:user:default", "user")

    assert name1 != name2, "Different inputs should produce different outputs"
    logger.info(f"✓ Different pool keys produce unique names: '{name1}' vs '{name2}'")


@pytest.mark.unit
def test_short_pool_name_with_long_username():
    """Test that very long usernames are truncated safely"""
    from mysql_ch_replicator.connection_pool import ConnectionPoolManager

    manager = ConnectionPoolManager()

    long_user = "verylongusernamethatexceedsnormallimits"
    pool_key = f"host:3306:{long_user}:default"

    short_name = manager._generate_short_pool_name(pool_key, long_user)

    assert len(short_name) <= 64, "Pool name should be under 64 characters"
    # Username should be truncated to 16 chars
    assert "verylongusername" in short_name
    assert long_user not in short_name, "Full long username should NOT appear"
    logger.info(f"✓ Long username truncated safely: '{short_name}'")
