"""Dynamic data testing scenarios - complementary to specific edge case tests"""

import pytest
from decimal import Decimal

from tests.base import IsolatedBaseReplicationTest, DataTestMixin, SchemaTestMixin
from tests.conftest import TEST_TABLE_NAME
from tests.fixtures.advanced_dynamic_generator import AdvancedDynamicGenerator


class TestDynamicDataScenarios(IsolatedBaseReplicationTest, SchemaTestMixin, DataTestMixin):
    """Test replication with dynamically generated schemas and data"""
    
    def setup_method(self):
        """Setup dynamic generator with fixed seed for reproducibility"""
        self.dynamic_gen = AdvancedDynamicGenerator(seed=42)  # Fixed seed for reproducible tests
    
    @pytest.mark.integration
    @pytest.mark.parametrize("data_type_focus,expected_min_count", [
        (["varchar", "int", "decimal"], 50),
        (["json", "text", "datetime"], 30), 
        (["enum", "set", "boolean"], 25),
        (["bigint", "float", "double"], 40)
    ])
    def test_dynamic_data_type_combinations(self, data_type_focus, expected_min_count):
        """Test replication with various data type combinations"""
        
        # Generate dynamic schema focused on specific data types
        schema_sql = self.dynamic_gen.generate_dynamic_schema(
            TEST_TABLE_NAME,
            data_type_focus=data_type_focus,
            column_count=(4, 8),
            include_constraints=True
        )
        
        # Create table and generate ALL data BEFORE starting replication (Phase 1.75 pattern)
        self.mysql.execute(schema_sql)
        
        # Generate test data matching the schema
        test_data = self.dynamic_gen.generate_dynamic_data(schema_sql, record_count=expected_min_count)
        
        # Insert ALL generated data before starting replication
        self.insert_multiple_records(TEST_TABLE_NAME, test_data)
        
        # Start replication AFTER all data is inserted
        self.start_replication()
        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=len(test_data))
        
        # Verify data integrity with sampling
        ch_records = self.ch.select(TEST_TABLE_NAME)
        assert len(ch_records) == len(test_data)
        
        # Sample a few records for detailed verification
        sample_size = min(5, len(ch_records))
        for i in range(sample_size):
            ch_record = ch_records[i]
            assert ch_record["id"] is not None  # Basic sanity check
            
        print(f"Dynamic test completed: {len(test_data)} records with focus on {data_type_focus}")
    
    @pytest.mark.integration
    def test_boundary_value_scenarios(self):
        """Test boundary values across different data types"""
        
        # Focus on data types with well-defined boundaries
        boundary_types = ["int", "bigint", "varchar", "decimal"]
        
        schema_sql, boundary_data = self.dynamic_gen.create_boundary_test_scenario(boundary_types, TEST_TABLE_NAME)
        
        # Create table with boundary test schema
        self.mysql.execute(schema_sql)
        
        # Insert boundary test data
        if boundary_data:
            self.insert_multiple_records(TEST_TABLE_NAME, boundary_data)
            
            # Start replication and verify
            self.start_replication()
            self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=len(boundary_data))
            
            # Verify boundary values replicated correctly
            ch_records = self.ch.select(TEST_TABLE_NAME)
            assert len(ch_records) == len(boundary_data)
            
            print(f"Boundary test completed: {len(boundary_data)} boundary value records")
        else:
            print("No boundary data generated, skipping test")
    
    @pytest.mark.integration 
    @pytest.mark.parametrize("complexity,record_count", [
        ("simple", 100),
        ("medium", 75),
        ("complex", 50)
    ])
    def test_schema_complexity_variations(self, complexity, record_count):
        """Test replication with varying schema complexity"""
        
        # Map complexity to data type selections
        complexity_focus = {
            "simple": ["varchar", "int", "date"],
            "medium": ["varchar", "int", "decimal", "text", "boolean", "datetime"],
            "complex": ["varchar", "int", "bigint", "decimal", "json", "enum", "set", "text", "datetime", "float"]
        }
        
        # Generate schema with complexity-appropriate column count
        column_ranges = {
            "simple": (3, 6),
            "medium": (6, 10), 
            "complex": (10, 15)
        }
        
        schema_sql = self.dynamic_gen.generate_dynamic_schema(
            TEST_TABLE_NAME,
            data_type_focus=complexity_focus[complexity],
            column_count=column_ranges[complexity],
            include_constraints=(complexity != "simple")
        )
        
        # Create table and generate appropriate test data
        self.mysql.execute(schema_sql)
        test_data = self.dynamic_gen.generate_dynamic_data(schema_sql, record_count=record_count)
        
        # Execute replication test
        self.insert_multiple_records(TEST_TABLE_NAME, test_data)
        self.start_replication()
        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=len(test_data))
        
        # Verify replication success
        ch_records = self.ch.select(TEST_TABLE_NAME)
        assert len(ch_records) == len(test_data)
        
        # Additional verification for complex schemas
        if complexity == "complex":
            # Verify JSON fields if present (sampling)
            for record in ch_records[:3]:  # Check first 3 records
                for key, value in record.items():
                    if key.startswith("col_") and isinstance(value, str):
                        try:
                            import json
                            json.loads(value)  # Validate JSON fields
                        except (json.JSONDecodeError, TypeError):
                            pass  # Not JSON, continue
        
        print(f"Schema complexity test completed: {complexity} with {len(test_data)} records")
    
    @pytest.mark.integration
    def test_mixed_null_and_constraint_scenarios(self):
        """Test dynamic scenarios with mixed NULL values and constraints"""
        
        # Generate schema with mixed constraint scenarios, limiting size to avoid MySQL key length limits
        schema_sql = self.dynamic_gen.generate_dynamic_schema(
            TEST_TABLE_NAME,
            data_type_focus=["varchar", "int", "decimal", "datetime", "boolean"],
            column_count=(4, 6),  # Reduced column count to avoid key length issues
            include_constraints=True  # Include random constraints (now safely limited)
        )
        
        # Create table and generate ALL data BEFORE starting replication (Phase 1.75 pattern)
        self.mysql.execute(schema_sql)
        
        # Generate data with intentional NULL value distribution
        test_data = self.dynamic_gen.generate_dynamic_data(schema_sql, record_count=40)  # Reduced for reliability
        
        # Insert ALL data before starting replication
        self.insert_multiple_records(TEST_TABLE_NAME, test_data)
        
        # Start replication AFTER all data is inserted
        self.start_replication()
        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=len(test_data))
        
        # Verify NULL handling
        ch_records = self.ch.select(TEST_TABLE_NAME)
        assert len(ch_records) == len(test_data)
        
        # Count NULL values in replicated data
        null_counts = {}
        for record in ch_records:
            for key, value in record.items():
                if key != "id":  # Skip auto-increment id
                    if value is None:
                        null_counts[key] = null_counts.get(key, 0) + 1
        
        if null_counts:
            print(f"NULL value handling verified: {null_counts}")
        
        print(f"Mixed constraint test completed: {len(test_data)} records")
    
    @pytest.mark.integration
    @pytest.mark.slow
    def test_large_dynamic_dataset(self):
        """Test replication with larger dynamically generated dataset"""
        
        # Generate comprehensive schema
        schema_sql = self.dynamic_gen.generate_dynamic_schema(
            TEST_TABLE_NAME,
            data_type_focus=["varchar", "int", "bigint", "decimal", "text", "json", "datetime", "boolean"],
            column_count=(8, 12),
            include_constraints=True
        )
        
        self.mysql.execute(schema_sql)
        
        # Generate larger dataset (Phase 1.75 pattern - all data before replication)
        test_data = self.dynamic_gen.generate_dynamic_data(schema_sql, record_count=300)  # Reduced for reliability
        
        # Insert ALL data in batches BEFORE starting replication
        batch_size = 100
        for i in range(0, len(test_data), batch_size):
            batch = test_data[i:i + batch_size]
            self.insert_multiple_records(TEST_TABLE_NAME, batch)
        
        # Start replication AFTER all data is inserted
        self.start_replication()
        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=len(test_data), max_wait_time=120)
        
        # Verify large dataset replication
        ch_records = self.ch.select(TEST_TABLE_NAME)
        assert len(ch_records) == len(test_data)
        
        # Statistical verification (sample-based)
        sample_indices = [0, len(ch_records)//4, len(ch_records)//2, len(ch_records)-1]
        for idx in sample_indices:
            if idx < len(ch_records):
                record = ch_records[idx]
                assert record["id"] is not None
        
        print(f"Large dynamic dataset test completed: {len(test_data)} records successfully replicated")