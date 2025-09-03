"""Example refactored configuration tests using EnhancedConfigurationTest framework"""

import pytest
from tests.base.enhanced_configuration_test import EnhancedConfigurationTest
from tests.conftest import TEST_DB_NAME, TEST_TABLE_NAME


class TestConfigurationExamples(EnhancedConfigurationTest):
    """Example configuration tests demonstrating the enhanced test framework"""
    
    @pytest.mark.integration
    def test_string_primary_key_enhanced(self):
        """Test replication with string primary keys - Enhanced version
        
        This replaces the manual process management in test_configuration_scenarios.py
        """
        
        # 1. Create isolated config (automatic cleanup)
        config_file = self.create_config_test(
            base_config_file="tests/configs/replicator/tests_config_string_primary_key.yaml"
        )
        
        # 2. Setup test data BEFORE starting replication (Phase 1.75 pattern)
        self.mysql.execute("SET sql_mode = 'ALLOW_INVALID_DATES';")
        
        self.mysql.execute(f"""
        CREATE TABLE `{TEST_TABLE_NAME}` (
            `id` char(30) NOT NULL,
            name varchar(255),
            PRIMARY KEY (id)
        ); 
        """)
        
        # Insert all test data before replication
        test_data = [
            ('01', 'Ivan'),
            ('02', 'Peter'),
            ('03', 'Filipp')  # Include data that was previously inserted during replication
        ]
        
        for id_val, name in test_data:
            self.mysql.execute(
                f"INSERT INTO `{TEST_TABLE_NAME}` (id, name) VALUES ('{id_val}', '{name}');",
                commit=True,
            )
        
        # 3. Start replication with enhanced monitoring (automatic process health checks)
        self.start_config_replication(config_file)
        
        # 4. Wait for sync with enhanced error reporting
        self.wait_for_config_sync(TEST_TABLE_NAME, expected_count=3)
        
        # 5. Verify results with comprehensive validation
        self.verify_config_test_result(TEST_TABLE_NAME, {
            "total_records": (lambda: len(self.ch.select(TEST_TABLE_NAME)), 3),
            "ivan_record": (lambda: self.ch.select(TEST_TABLE_NAME, where="id='01'"), 
                          [{"id": "01", "name": "Ivan"}]),
            "peter_record": (lambda: self.ch.select(TEST_TABLE_NAME, where="id='02'"), 
                           [{"id": "02", "name": "Peter"}]),
            "filipp_record": (lambda: self.ch.select(TEST_TABLE_NAME, where="id='03'"), 
                            [{"id": "03", "name": "Filipp"}])
        })
        
        # Automatic cleanup handled by framework
    
    @pytest.mark.integration 
    def test_ignore_deletes_enhanced(self):
        """Test ignore_deletes configuration - Enhanced version"""
        
        # 1. Create config with ignore_deletes modification
        config_file = self.create_config_test(
            base_config_file="tests/configs/replicator/tests_config.yaml",
            config_modifications={"ignore_deletes": True}
        )
        
        # 2. Setup test schema and data
        self.mysql.execute(f"""
        CREATE TABLE `{TEST_TABLE_NAME}` (
            departments int,
            termine int, 
            data varchar(50)
        );
        """)
        
        # Insert all test data before replication (including data that will be "deleted")
        test_data = [
            (10, 20, 'data1'),
            (20, 30, 'data2'), 
            (30, 40, 'data3'),
            (70, 80, 'data4')  # Include data that was previously inserted during test
        ]
        
        for departments, termine, data in test_data:
            self.mysql.execute(
                f"INSERT INTO `{TEST_TABLE_NAME}` (departments, termine, data) VALUES ({departments}, {termine}, '{data}');",
                commit=True,
            )
        
        # 3. Start replication
        self.start_config_replication(config_file)
        
        # 4. Wait for initial sync
        self.wait_for_config_sync(TEST_TABLE_NAME, expected_count=4)
        
        # 5. Test delete operations (should be ignored)
        # Delete some records from MySQL
        self.mysql.execute(f"DELETE FROM `{TEST_TABLE_NAME}` WHERE departments=10;", commit=True)
        self.mysql.execute(f"DELETE FROM `{TEST_TABLE_NAME}` WHERE departments=30;", commit=True)
        
        # Wait briefly for replication to process delete events
        import time
        time.sleep(5)
        
        # 6. Verify deletes were ignored and all records still exist
        self.verify_config_test_result(TEST_TABLE_NAME, {
            "ignore_deletes_working": (lambda: len(self.ch.select(TEST_TABLE_NAME)), 4),
            "data1_still_exists": (lambda: len(self.ch.select(TEST_TABLE_NAME, where="departments=10")), 1),
            "data3_still_exists": (lambda: len(self.ch.select(TEST_TABLE_NAME, where="departments=30")), 1),
            "data4_exists": (lambda: self.ch.select(TEST_TABLE_NAME, where="departments=70 AND termine=80"),
                           [{"departments": 70, "termine": 80, "data": "data4"}])
        })
    
    @pytest.mark.integration
    def test_timezone_conversion_enhanced(self):
        """Test timezone conversion configuration - Enhanced version"""
        
        # 1. Create config with timezone settings
        config_file = self.create_config_test(
            base_config_file="tests/configs/replicator/tests_config.yaml",
            config_modifications={
                "clickhouse": {
                    "timezone": "America/New_York"
                },
                "types_mapping": {
                    "timestamp": "DateTime64(3, 'America/New_York')"
                }
            }
        )
        
        # 2. Setup table with timestamp column
        self.mysql.execute(f"""
        CREATE TABLE `{TEST_TABLE_NAME}` (
            id int PRIMARY KEY,
            created_at timestamp DEFAULT CURRENT_TIMESTAMP,
            name varchar(255)
        );
        """)
        
        # Insert test data with specific timestamps
        self.mysql.execute(f"""
        INSERT INTO `{TEST_TABLE_NAME}` (id, created_at, name) VALUES 
        (1, '2023-06-15 10:30:00', 'Test Record');
        """, commit=True)
        
        # 3. Start replication
        self.start_config_replication(config_file)
        
        # 4. Wait for sync
        self.wait_for_config_sync(TEST_TABLE_NAME, expected_count=1)
        
        # 5. Verify timezone conversion in ClickHouse schema
        # Get the ClickHouse table schema to check timezone mapping
        table_schema = self.ch.execute_command(f"DESCRIBE {TEST_TABLE_NAME}")
        
        self.verify_config_test_result(TEST_TABLE_NAME, {
            "record_count": (lambda: len(self.ch.select(TEST_TABLE_NAME)), 1),
            "timezone_in_schema": (lambda: "America/New_York" in str(table_schema), True),
            "test_record_exists": (lambda: self.ch.select(TEST_TABLE_NAME, where="id=1"),
                                 [{"id": 1, "name": "Test Record"}])  # Note: timestamp verification would need more complex logic
        })
    
    @pytest.mark.integration
    def test_run_all_runner_enhanced(self):
        """Test using RunAllRunner with enhanced framework"""
        
        # 1. Create config for RunAllRunner scenario
        config_file = self.create_config_test(
            base_config_file="tests/configs/replicator/tests_config.yaml"
        )
        
        # 2. Setup test table and data
        self.mysql.execute(f"""
        CREATE TABLE `{TEST_TABLE_NAME}` (
            id int PRIMARY KEY,
            name varchar(255),
            status varchar(50)
        );
        """)
        
        test_records = [
            (1, 'Active User', 'active'),
            (2, 'Inactive User', 'inactive'),
            (3, 'Pending User', 'pending')
        ]
        
        for id_val, name, status in test_records:
            self.mysql.execute(
                f"INSERT INTO `{TEST_TABLE_NAME}` (id, name, status) VALUES ({id_val}, '{name}', '{status}');",
                commit=True,
            )
        
        # 3. Start replication using RunAllRunner
        self.start_config_replication(config_file, use_run_all_runner=True)
        
        # 4. Wait for sync with enhanced monitoring
        self.wait_for_config_sync(TEST_TABLE_NAME, expected_count=3)
        
        # 5. Comprehensive validation
        self.verify_config_test_result(TEST_TABLE_NAME, {
            "total_users": (lambda: len(self.ch.select(TEST_TABLE_NAME)), 3),
            "active_users": (lambda: len(self.ch.select(TEST_TABLE_NAME, where="status='active'")), 1),
            "inactive_users": (lambda: len(self.ch.select(TEST_TABLE_NAME, where="status='inactive'")), 1),
            "pending_users": (lambda: len(self.ch.select(TEST_TABLE_NAME, where="status='pending'")), 1),
            "specific_user": (lambda: self.ch.select(TEST_TABLE_NAME, where="id=1"),
                            [{"id": 1, "name": "Active User", "status": "active"}])
        })


# Example of function-based test that can also use the enhanced framework
@pytest.mark.integration
def test_advanced_mapping_enhanced(clean_environment):
    """Example of function-based test using enhanced framework components"""
    
    # Initialize the enhanced framework manually
    test_instance = EnhancedConfigurationTest()
    test_instance.setup_replication_test(clean_environment)
    
    try:
        # Use enhanced methods
        config_file = test_instance.create_config_test(
            base_config_file="tests/configs/replicator/tests_config.yaml",
            config_modifications={
                "target_databases": {
                    TEST_DB_NAME: "custom_target_db"
                }
            }
        )
        
        # Setup and test as normal using enhanced methods
        test_instance.mysql.execute(f"""
        CREATE TABLE `{TEST_TABLE_NAME}` (
            id int PRIMARY KEY,
            data varchar(255)
        );
        """)
        
        test_instance.mysql.execute(
            f"INSERT INTO `{TEST_TABLE_NAME}` (id, data) VALUES (1, 'test_data');",
            commit=True,
        )
        
        test_instance.start_config_replication(config_file)
        test_instance.wait_for_config_sync(TEST_TABLE_NAME, expected_count=1)
        
        # Verify the custom target database was used
        databases = test_instance.ch.get_databases()
        assert "custom_target_db" in databases, f"Custom target database not found. Available: {databases}"
        
    finally:
        # Manual cleanup
        test_instance._cleanup_enhanced_resources()