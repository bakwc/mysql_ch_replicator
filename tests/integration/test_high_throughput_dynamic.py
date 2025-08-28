"""High-throughput dynamic testing with generated tables and data"""

import random
import string
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from decimal import Decimal

import pytest

from tests.base import BaseReplicationTest, DataTestMixin, SchemaTestMixin
from tests.conftest import TEST_DB_NAME


class DynamicTableGenerator:
    """Generate dynamic table schemas and data for testing"""

    @staticmethod
    def generate_table_schema(table_name, complexity_level="medium"):
        """Generate dynamic table schema based on complexity level"""
        base_columns = [
            "id int NOT NULL AUTO_INCREMENT",
            "created_at timestamp DEFAULT CURRENT_TIMESTAMP"
        ]

        complexity_configs = {
            "simple": {
                "additional_columns": 3,
                "types": ["varchar(100)", "int", "decimal(10,2)"]
            },
            "medium": {
                "additional_columns": 8,
                "types": ["varchar(255)", "int", "bigint", "decimal(12,4)", "text", "json", "boolean", "datetime"]
            },
            "complex": {
                "additional_columns": 15,
                "types": ["varchar(500)", "tinyint", "smallint", "int", "bigint", "decimal(15,6)", 
                         "float", "double", "text", "longtext", "blob", "json", "boolean", 
                         "date", "datetime", "timestamp"]
            }
        }

        config = complexity_configs[complexity_level]
        columns = base_columns.copy()

        for i in range(config["additional_columns"]):
            col_type = random.choice(config["types"])
            col_name = f"field_{i+1}"
            
            # Add constraints for some columns
            constraint = ""
            if col_type.startswith("varchar") and random.random() < 0.3:
                constraint = " UNIQUE" if random.random() < 0.5 else " NOT NULL"
            
            columns.append(f"{col_name} {col_type}{constraint}")

        columns.append("PRIMARY KEY (id)")
        
        return f"CREATE TABLE `{table_name}` ({', '.join(columns)});"

    @staticmethod
    def generate_test_data(schema, num_records=1000):
        """Generate test data matching the schema"""
        # Parse schema to understand column types (simplified)
        data_generators = {
            "varchar": lambda size: ''.join(random.choices(string.ascii_letters + string.digits, k=min(int(size), 50))),
            "int": lambda: random.randint(-2147483648, 2147483647),
            "bigint": lambda: random.randint(-9223372036854775808, 9223372036854775807),
            "decimal": lambda p, s: Decimal(f"{random.uniform(-999999, 999999):.{min(int(s), 4)}f}"),
            "text": lambda: ' '.join(random.choices(string.ascii_words, k=random.randint(10, 100))),
            "json": lambda: f'{{"key_{random.randint(1,100)}": "value_{random.randint(1,1000)}", "number": {random.randint(1,100)}}}',
            "boolean": lambda: random.choice([True, False]),
            "datetime": lambda: f"2023-{random.randint(1,12):02d}-{random.randint(1,28):02d} {random.randint(0,23):02d}:{random.randint(0,59):02d}:{random.randint(0,59):02d}"
        }

        records = []
        for _ in range(num_records):
            record = {}
            # Generate data based on schema analysis (simplified implementation)
            # In a real implementation, you'd parse the CREATE TABLE statement
            for i in range(8):  # Medium complexity default
                field_name = f"field_{i+1}"
                data_type = random.choice(["varchar", "int", "decimal", "text", "json", "boolean", "datetime"])
                
                try:
                    if data_type == "varchar":
                        record[field_name] = data_generators["varchar"](100)
                    elif data_type == "decimal":
                        record[field_name] = data_generators["decimal"](12, 4)
                    else:
                        record[field_name] = data_generators[data_type]()
                except:
                    record[field_name] = f"default_value_{i}"
            
            records.append(record)
        
        return records


class TestHighThroughputDynamic(BaseReplicationTest, SchemaTestMixin, DataTestMixin):
    """Test high-throughput replication with dynamically generated tables and data"""

    @pytest.mark.performance
    @pytest.mark.slow
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
        self.wait_for_table_sync(table_name, expected_count=len(test_data), max_wait_time=300)
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

    @pytest.mark.performance
    @pytest.mark.slow
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

    @pytest.mark.performance 
    def test_mixed_operation_stress_test(self):
        """Test mixed INSERT/UPDATE/DELETE operations under stress"""
        table_name = "stress_test_table"
        
        # Create table optimized for mixed operations
        self.mysql.execute(f"""
        CREATE TABLE `{table_name}` (
            id int NOT NULL AUTO_INCREMENT,
            code varchar(50) UNIQUE NOT NULL,
            value decimal(12,4),
            status varchar(20),
            data text,
            updated_at timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            PRIMARY KEY (id),
            KEY idx_code (code),
            KEY idx_status (status)
        );
        """)
        
        # Start replication
        self.start_replication()
        self.wait_for_table_sync(table_name, expected_count=0)
        
        # Initial data load
        initial_data = []
        for i in range(3000):
            initial_data.append({
                "code": f"ITEM_{i:06d}",
                "value": Decimal(f"{random.uniform(1, 1000):.4f}"),
                "status": random.choice(["active", "inactive", "pending"]),
                "data": f"Initial data for item {i}"
            })
        
        self.insert_multiple_records(table_name, initial_data)
        self.wait_for_table_sync(table_name, expected_count=len(initial_data))
        
        # Mixed operations stress test
        operations_count = 2000
        start_time = time.time()
        
        for i in range(operations_count):
            operation = random.choices(
                ["insert", "update", "delete"],
                weights=[40, 50, 10],  # 40% insert, 50% update, 10% delete
                k=1
            )[0]
            
            if operation == "insert":
                new_code = f"NEW_{i:06d}_{random.randint(1000, 9999)}"
                self.mysql.execute(
                    f"INSERT INTO `{table_name}` (code, value, status, data) VALUES (%s, %s, %s, %s)",
                    (new_code, Decimal(f"{random.uniform(1, 1000):.4f}"), 
                     random.choice(["active", "inactive", "pending"]),
                     f"Stress test data {i}"),
                    commit=True
                )
                
            elif operation == "update":
                # Update random existing record
                update_id = random.randint(1, min(len(initial_data), 1000))
                self.mysql.execute(
                    f"UPDATE `{table_name}` SET value = %s, status = %s WHERE id = %s",
                    (Decimal(f"{random.uniform(1, 1000):.4f}"),
                     random.choice(["active", "inactive", "pending", "updated"]),
                     update_id),
                    commit=True
                )
                
            elif operation == "delete":
                # Delete random record (if it exists)
                delete_id = random.randint(1, min(len(initial_data), 1000))
                self.mysql.execute(
                    f"DELETE FROM `{table_name}` WHERE id = %s",
                    (delete_id,),
                    commit=True
                )
            
            # Progress indicator
            if (i + 1) % 500 == 0:
                print(f"Completed {i + 1}/{operations_count} mixed operations")
        
        operation_time = time.time() - start_time
        operation_rate = operations_count / operation_time
        
        # Wait for replication to stabilize
        replication_start = time.time()
        self.wait_for_stable_state(table_name, expected_count=None, wait_time=30)
        replication_time = time.time() - replication_start
        
        # Get final counts
        self.mysql.execute(f"SELECT COUNT(*) FROM `{table_name}`")
        mysql_final_count = self.mysql.cursor.fetchone()[0]
        
        ch_records = self.ch.select(table_name)
        ch_final_count = len(ch_records)
        
        print(f"Mixed Operations Stress Test Results:")
        print(f"- Operations executed: {operations_count}")
        print(f"- Operation time: {operation_time:.2f}s ({operation_rate:.1f} ops/sec)")
        print(f"- Replication stabilization: {replication_time:.2f}s")
        print(f"- Final record count: MySQL={mysql_final_count}, ClickHouse={ch_final_count}")
        
        # Verify data consistency
        assert mysql_final_count == ch_final_count, (
            f"Final count mismatch: MySQL={mysql_final_count}, ClickHouse={ch_final_count}"
        )
        
        # Performance assertions
        assert operation_rate > 10, f"Mixed operation rate too slow: {operation_rate:.1f} ops/sec"

    def _verify_high_volume_data_integrity(self, table_name, expected_count):
        """Verify data integrity for high volume datasets"""
        # Count verification
        ch_records = self.ch.select(table_name)
        ch_count = len(ch_records)
        
        assert ch_count == expected_count, (
            f"Record count mismatch: expected {expected_count}, got {ch_count}"
        )
        
        # Sample-based integrity check (check 1% of records)
        sample_size = max(100, expected_count // 100)
        if ch_count > sample_size:
            # Random sampling for large datasets
            sampled_records = random.sample(ch_records, sample_size)
            
            for record in sampled_records:
                record_id = record["id"]
                # Verify record exists in MySQL
                self.mysql.execute(f"SELECT COUNT(*) FROM `{table_name}` WHERE id = %s", (record_id,))
                mysql_exists = self.mysql.cursor.fetchone()[0] > 0
                assert mysql_exists, f"Record with id={record_id} missing from MySQL"
        
        print(f"Data integrity verified for {table_name}: {ch_count} records")