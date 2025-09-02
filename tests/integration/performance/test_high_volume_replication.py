"""High-volume replication testing with dynamic table generation"""

import time
import pytest

from tests.base import BaseReplicationTest, DataTestMixin, SchemaTestMixin
from tests.fixtures.dynamic_generator import DynamicTableGenerator
from tests.fixtures.data_factory import DataFactory


class TestHighVolumeReplication(BaseReplicationTest, SchemaTestMixin, DataTestMixin):
    """Test high-volume data replication scenarios"""

    @pytest.mark.integration
    @pytest.mark.performance
    def test_dynamic_table_high_volume_replication(self):
        """Test replication of dynamically generated table with high volume data"""
        # Generate dynamic table schema
        table_name = "dynamic_test_table"
        schema_sql = DynamicTableGenerator.generate_table_schema(table_name, "medium")
        
        # Create table
        self.mysql.execute(schema_sql)
        
        # Generate large dataset
        test_data = DynamicTableGenerator.generate_test_data(schema_sql, num_records=5000)
        
        # Start replication
        self.start_replication()
        self.wait_for_table_sync(table_name, expected_count=0)
        
        # Insert data in batches for better performance
        batch_size = 500
        total_inserted = 0
        start_time = time.time()
        
        for i in range(0, len(test_data), batch_size):
            batch = test_data[i:i + batch_size]
            self.insert_multiple_records(table_name, batch)
            total_inserted += len(batch)
            print(f"Inserted batch {i//batch_size + 1}, total records: {total_inserted}")
        
        insertion_time = time.time() - start_time
        
        # Wait for replication to complete
        replication_start = time.time()
        self.wait_for_table_sync(table_name, expected_count=len(test_data), max_wait_time=60)
        replication_time = time.time() - replication_start
        
        # Calculate performance metrics
        insertion_rate = total_inserted / insertion_time
        replication_rate = total_inserted / replication_time
        
        print(f"Performance Metrics:")
        print(f"- Records inserted: {total_inserted}")
        print(f"- Insertion time: {insertion_time:.2f}s ({insertion_rate:.1f} records/sec)")
        print(f"- Replication time: {replication_time:.2f}s ({replication_rate:.1f} records/sec)")
        
        # Verify data integrity
        self._verify_high_volume_data_integrity(table_name, len(test_data))
        
        # Performance assertions
        assert insertion_rate > 100, f"Insertion rate too slow: {insertion_rate:.1f} records/sec"
        assert replication_rate > 50, f"Replication rate too slow: {replication_rate:.1f} records/sec"

    @pytest.mark.integration
    @pytest.mark.performance
    def test_large_single_table_replication(self):
        """Test replication of a single table with very large dataset"""
        from tests.fixtures.schema_factory import SchemaFactory
        
        table_name = "large_performance_table"
        
        # Create performance-optimized table schema
        schema_sql = SchemaFactory.performance_test_table(table_name, "complex")
        self.mysql.execute(schema_sql)
        
        # Start replication
        self.start_replication()
        self.wait_for_table_sync(table_name, expected_count=0)
        
        # Generate and insert large dataset
        large_dataset = DataFactory.performance_test_data(count=10000, complexity="complex")
        
        start_time = time.time()
        batch_size = 1000
        total_records = 0
        
        for i in range(0, len(large_dataset), batch_size):
            batch = large_dataset[i:i + batch_size]
            self.insert_multiple_records(table_name, batch)
            total_records += len(batch)
            
            if i % (batch_size * 5) == 0:  # Progress update every 5 batches
                elapsed = time.time() - start_time
                print(f"Progress: {total_records}/{len(large_dataset)} records in {elapsed:.1f}s")
        
        # Wait for replication completion
        self.wait_for_table_sync(table_name, expected_count=len(large_dataset), max_wait_time=120)
        total_time = time.time() - start_time
        
        # Verify final results
        throughput = len(large_dataset) / total_time
        print(f"Large dataset test completed:")
        print(f"- Total records: {len(large_dataset)}")
        print(f"- Total time: {total_time:.2f}s")
        print(f"- Throughput: {throughput:.1f} records/sec")
        
        # Performance assertions
        assert throughput > 25, f"Throughput too low: {throughput:.1f} records/sec"
        self._verify_high_volume_data_integrity(table_name, len(large_dataset))

    def _verify_high_volume_data_integrity(self, table_name: str, expected_count: int):
        """Verify data integrity for high volume tests"""
        # Check record count
        ch_records = self.ch.select(table_name)
        assert len(ch_records) == expected_count, f"Expected {expected_count} records, got {len(ch_records)}"
        
        # Sample-based data verification (check 10% of records)
        sample_size = max(10, expected_count // 10)
        mysql_sample = self.mysql.fetch_all(
            f"SELECT * FROM `{table_name}` ORDER BY id LIMIT {sample_size}"
        )
        
        ch_sample = self.ch.select(table_name, order_by="id", final=True)[:sample_size]
        
        assert len(mysql_sample) == len(ch_sample), "Sample sizes don't match"
        
        # Verify sample records match (basic check)
        for mysql_row, ch_row in zip(mysql_sample, ch_sample):
            assert mysql_row['id'] == ch_row['id'], f"ID mismatch: {mysql_row['id']} vs {ch_row['id']}"
            
        print(f"Data integrity verified: {sample_size} sample records match")