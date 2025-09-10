"""Integration test to validate dynamic database isolation system"""

import pytest

from tests.base import BaseReplicationTest, DataTestMixin, SchemaTestMixin
from tests.conftest import TEST_TABLE_NAME, TEST_DB_NAME
from tests.utils.dynamic_config import get_config_manager


class TestDynamicDatabaseIsolation(BaseReplicationTest, SchemaTestMixin, DataTestMixin):
    """Test the new dynamic database isolation system"""
    
    @pytest.mark.integration
    def test_automatic_database_isolation(self):
        """Test that databases are automatically isolated for each test"""
        
        # Verify that our database name is unique and isolated
        assert "_w" in TEST_DB_NAME, "Database name should contain worker ID"
        assert len(TEST_DB_NAME.split("_")) >= 4, "Database name should be structured: test_db_<worker>_<testid>"
        
        # Create a simple table and insert data
        self.create_basic_table(TEST_TABLE_NAME)
        test_data = [
            {"id": 1, "name": "isolation_test_1"},
            {"id": 2, "name": "isolation_test_2"},
        ]
        self.insert_multiple_records(TEST_TABLE_NAME, test_data)
        
        # Start replication using simplified helper method
        self.start_isolated_replication()
        
        # Wait for sync and verify
        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=2)
        
        # Verify data replication worked
        ch_records = self.ch.select(TEST_TABLE_NAME)
        assert len(ch_records) == 2
        
        record_names = [record["name"] for record in ch_records]
        assert "isolation_test_1" in record_names
        assert "isolation_test_2" in record_names
    
    @pytest.mark.integration 
    def test_dynamic_target_database_mapping(self):
        """Test dynamic target database mapping functionality"""
        
        # Use the new helper method to create isolated target database name  
        target_db_name = self.create_isolated_target_database_name(TEST_DB_NAME, "test_target")
        
        # Verify target database name is properly isolated
        assert "_w" in target_db_name, "Target database name should contain worker ID"
        assert "test_target" in target_db_name, "Target database name should contain specified suffix"
        
        # Create dynamic config with target mapping using the helper method
        config_file = self.create_dynamic_config_with_target_mapping(
            source_db_name=TEST_DB_NAME,
            target_db_name=target_db_name
        )
        
        # Verify config file was created
        import os
        assert os.path.exists(config_file), "Dynamic config file should exist"
        
        # Load and verify config contents
        import yaml
        with open(config_file, 'r') as f:
            config_data = yaml.safe_load(f)
        
        assert 'target_databases' in config_data
        assert TEST_DB_NAME in config_data['target_databases']
        assert config_data['target_databases'][TEST_DB_NAME] == target_db_name
        
        # Verify data directory is isolated
        assert "w" in config_data['binlog_replicator']['data_dir']
        
        print(f"✅ Dynamic config test passed:")
        print(f"   Source DB: {TEST_DB_NAME}")
        print(f"   Target DB: {target_db_name}")
        print(f"   Config file: {config_file}")
    
    @pytest.mark.integration
    def test_config_manager_isolation_functions(self):
        """Test the config manager isolation utility functions"""
        
        config_manager = get_config_manager()
        
        # Test database name generation
        db_name = config_manager.get_isolated_database_name()
        assert "_w" in db_name, "Generated database name should be isolated"
        
        # Test table name generation
        table_name = config_manager.get_isolated_table_name()
        assert "_w" in table_name, "Generated table name should be isolated"
        
        # Test data directory generation  
        data_dir = config_manager.get_isolated_data_dir()
        assert "/app/binlog" in data_dir and "w" in data_dir, "Generated data directory should be isolated in binlog folder"
        
        # Test target database name generation
        target_name = config_manager.get_isolated_target_database_name(db_name, "custom_target")
        assert "_w" in target_name, "Generated target database name should be isolated" 
        assert "custom_target" in target_name, "Target name should include custom suffix"
        
        # Test target mapping creation
        source_databases = [db_name, config_manager.get_isolated_database_name("_2")]
        mappings = config_manager.create_isolated_target_mappings(
            source_databases=source_databases,
            target_prefix="mapped"
        )
        
        assert len(mappings) == 2, "Should create mapping for each source database"
        for source, target in mappings.items():
            assert "_w" in source, "Source database should be isolated"
            assert "_w" in target, "Target database should be isolated"
            assert "mapped" in target, "Target should include prefix"
        
        print(f"✅ Config manager test passed:")
        print(f"   Isolated DB: {db_name}")
        print(f"   Isolated Table: {table_name}")
        print(f"   Isolated Data Dir: {data_dir}")
        print(f"   Target DB: {target_name}")
        print(f"   Mappings: {mappings}")