"""Concurrent multi-table operations testing for replication performance"""

import random
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import pytest

from tests.base import BaseReplicationTest, DataTestMixin, SchemaTestMixin
from tests.fixtures.dynamic_generator import DynamicTableGenerator
from tests.fixtures.schema_factory import SchemaFactory
from tests.fixtures.data_factory import DataFactory


class TestConcurrentOperations(BaseReplicationTest, SchemaTestMixin, DataTestMixin):
    """Test concurrent operations across multiple tables"""

    @pytest.mark.integration
    @pytest.mark.performance
    def test_concurrent_multi_table_operations(self):
        """Test concurrent operations across multiple dynamically generated tables"""
        table_count = 5
        records_per_table = 2000
        
        # Generate multiple tables with different schemas
        tables_info = []
        for i in range(table_count):
            table_name = f"concurrent_table_{i+1}"
            complexity = random.choice(["simple", "medium", "complex"])
            schema_sql = DynamicTableGenerator.generate_table_schema(table_name, complexity)
            test_data = DynamicTableGenerator.generate_test_data(schema_sql, records_per_table)
            
            tables_info.append({
                "name": table_name,
                "schema": schema_sql,
                "data": test_data,
                "complexity": complexity
            })
            
            # Create table
            self.mysql.execute(schema_sql)
        
        # Start replication
        self.start_replication()
        
        # Wait for all tables to be created
        for table_info in tables_info:
            self.wait_for_table_sync(table_info["name"], expected_count=0)
        
        # Concurrent data insertion using thread pool
        start_time = time.time()
        
        def insert_table_data(table_info):
            """Insert data for a single table"""
            table_start = time.time()
            self.insert_multiple_records(table_info["name"], table_info["data"])
            table_time = time.time() - table_start
            return {
                "table": table_info["name"],
                "records": len(table_info["data"]),
                "time": table_time,
                "rate": len(table_info["data"]) / table_time
            }
        
        # Execute concurrent insertions
        with ThreadPoolExecutor(max_workers=table_count) as executor:
            futures = [executor.submit(insert_table_data, table_info) for table_info in tables_info]
            insertion_results = [future.result() for future in as_completed(futures)]
        
        total_insertion_time = time.time() - start_time
        total_records = sum(len(t["data"]) for t in tables_info)
        
        # Wait for replication to complete for all tables
        replication_start = time.time()
        for table_info in tables_info:
            self.wait_for_table_sync(table_info["name"], expected_count=len(table_info["data"]), max_wait_time=300)
        total_replication_time = time.time() - replication_start
        
        # Calculate performance metrics
        total_insertion_rate = total_records / total_insertion_time
        total_replication_rate = total_records / total_replication_time
        
        print(f"Concurrent Multi-Table Performance:")
        print(f"- Tables: {table_count}")
        print(f"- Total records: {total_records}")
        print(f"- Total insertion time: {total_insertion_time:.2f}s ({total_insertion_rate:.1f} records/sec)")
        print(f"- Total replication time: {total_replication_time:.2f}s ({total_replication_rate:.1f} records/sec)")
        
        # Per-table performance
        for result in insertion_results:
            print(f"  - {result['table']}: {result['records']} records in {result['time']:.2f}s ({result['rate']:.1f} records/sec)")
        
        # Verify data integrity for all tables
        for table_info in tables_info:
            self._verify_high_volume_data_integrity(table_info["name"], len(table_info["data"]))
        
        # Performance assertions
        assert total_insertion_rate > 200, f"Multi-table insertion rate too slow: {total_insertion_rate:.1f} records/sec"
        assert total_replication_rate > 100, f"Multi-table replication rate too slow: {total_replication_rate:.1f} records/sec"

    @pytest.mark.integration
    @pytest.mark.performance
    def test_concurrent_mixed_table_types(self):
        """Test concurrent operations on tables with different data type focuses"""
        
        # Create tables with different data type specializations
        tables_config = [
            {"name": "numeric_table", "factory": SchemaFactory.numeric_types_table, "data": DataFactory.numeric_boundary_data},
            {"name": "text_table", "factory": SchemaFactory.text_types_table, "data": DataFactory.text_and_binary_data},
            {"name": "temporal_table", "factory": SchemaFactory.temporal_types_table, "data": DataFactory.temporal_data},
            {"name": "json_table", "factory": SchemaFactory.json_types_table, "data": DataFactory.json_test_data},
        ]
        
        # Create all tables
        for config in tables_config:
            schema_sql = config["factory"](config["name"])
            self.mysql.execute(schema_sql)
        
        # Start replication
        self.start_replication()
        
        # Wait for all tables to be created in ClickHouse
        for config in tables_config:
            self.wait_for_table_sync(config["name"], expected_count=0)
        
        def insert_specialized_data(config):
            """Insert data for a specialized table type"""
            table_start = time.time()
            data_records = config["data"]()
            
            # Replicate data multiple times for volume
            extended_data = data_records * 500  # Multiply by 500 for volume
            
            self.insert_multiple_records(config["name"], extended_data)
            table_time = time.time() - table_start
            
            return {
                "table": config["name"],
                "records": len(extended_data),
                "time": table_time,
                "rate": len(extended_data) / table_time
            }
        
        # Execute concurrent operations on different table types
        start_time = time.time()
        with ThreadPoolExecutor(max_workers=len(tables_config)) as executor:
            futures = [executor.submit(insert_specialized_data, config) for config in tables_config]
            results = [future.result() for future in as_completed(futures)]
        
        total_time = time.time() - start_time
        total_records = sum(r["records"] for r in results)
        
        # Wait for replication completion
        for i, config in enumerate(tables_config):
            expected_count = results[i]["records"]
            self.wait_for_table_sync(config["name"], expected_count=expected_count, max_wait_time=180)
        
        # Report results
        overall_rate = total_records / total_time
        print(f"Mixed Table Types Concurrent Test:")
        print(f"- Total records: {total_records}")
        print(f"- Total time: {total_time:.2f}s")
        print(f"- Overall rate: {overall_rate:.1f} records/sec")
        
        for result in results:
            print(f"  - {result['table']}: {result['records']} records, {result['rate']:.1f} records/sec")
        
        # Performance assertion
        assert overall_rate > 50, f"Mixed table types rate too slow: {overall_rate:.1f} records/sec"
        
        # Verify each table has expected data
        for config in tables_config:
            ch_count = len(self.ch.select(config["name"]))
            expected_count = next(r["records"] for r in results if r["table"] == config["name"])
            assert ch_count == expected_count, f"Table {config['name']}: expected {expected_count}, got {ch_count}"

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