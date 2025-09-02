"""Mixed operation stress testing for replication under heavy load"""

import random
import time
from decimal import Decimal

import pytest

from tests.base import BaseReplicationTest, DataTestMixin, SchemaTestMixin
from tests.fixtures.schema_factory import SchemaFactory
from tests.fixtures.data_factory import DataFactory


class TestStressOperations(BaseReplicationTest, SchemaTestMixin, DataTestMixin):
    """Test mixed operations under stress conditions"""

    @pytest.mark.integration
    @pytest.mark.performance
    @pytest.mark.slow
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
                    commit=True,
                    args=(new_code, Decimal(f"{random.uniform(1, 1000):.4f}"), 
                     random.choice(["active", "inactive", "pending"]),
                     f"Stress test data {i}")
                )
                
            elif operation == "update":
                # Update random existing record
                update_id = random.randint(1, min(len(initial_data), 1000))
                self.mysql.execute(
                    f"UPDATE `{table_name}` SET value = %s, status = %s WHERE id = %s",
                    commit=True,
                    args=(Decimal(f"{random.uniform(1, 1000):.4f}"),
                     random.choice(["active", "inactive", "pending", "updated"]),
                     update_id)
                )
                
            elif operation == "delete":
                # Delete random record (if it exists)
                delete_id = random.randint(1, min(len(initial_data), 1000))
                self.mysql.execute(
                    f"DELETE FROM `{table_name}` WHERE id = %s",
                    commit=True,
                    args=(delete_id,)
                )
            
            # Progress indicator
            if (i + 1) % 500 == 0:
                print(f"Completed {i + 1}/{operations_count} mixed operations")
        
        operation_time = time.time() - start_time
        operation_rate = operations_count / operation_time
        
        # Wait for replication to stabilize
        replication_start = time.time()
        self.wait_for_stable_state(table_name, expected_count=None, max_wait_time=30)
        replication_time = time.time() - replication_start
        
        # Get final counts
        mysql_final_count = len(self.mysql.fetch_all(f"SELECT * FROM `{table_name}`"))
        ch_records = self.ch.select(table_name)
        ch_final_count = len(ch_records)
        
        print(f"Mixed Operations Stress Test Results:")
        print(f"- Operations executed: {operations_count}")
        print(f"- Operation time: {operation_time:.2f}s ({operation_rate:.1f} ops/sec)")
        print(f"- Replication stabilization: {replication_time:.2f}s")
        print(f"- Final MySQL count: {mysql_final_count}")
        print(f"- Final ClickHouse count: {ch_final_count}")
        
        # Performance assertions
        assert operation_rate > 50, f"Operation rate too slow: {operation_rate:.1f} ops/sec"
        assert abs(mysql_final_count - ch_final_count) <= 5, f"Count mismatch: MySQL {mysql_final_count} vs ClickHouse {ch_final_count}"

    @pytest.mark.integration
    @pytest.mark.performance
    def test_burst_operation_stress(self):
        """Test handling of burst operations with varying intensity"""
        table_name = "burst_test_table"
        
        # Create table with performance schema
        schema_sql = SchemaFactory.performance_test_table(table_name, "medium")
        self.mysql.execute(schema_sql)
        
        # Start replication
        self.start_replication()
        self.wait_for_table_sync(table_name, expected_count=0)
        
        total_operations = 0
        burst_cycles = 5
        
        for cycle in range(burst_cycles):
            print(f"Starting burst cycle {cycle + 1}/{burst_cycles}")
            
            # Generate burst data
            burst_size = random.randint(500, 1500)
            burst_data = DataFactory.performance_test_data(count=burst_size, complexity="medium")
            
            # Execute burst insert
            burst_start = time.time()
            self.insert_multiple_records(table_name, burst_data)
            burst_time = time.time() - burst_start
            
            total_operations += burst_size
            burst_rate = burst_size / burst_time
            
            print(f"  Burst {cycle + 1}: {burst_size} records in {burst_time:.2f}s ({burst_rate:.1f} records/sec)")
            
            # Brief pause between bursts
            if cycle < burst_cycles - 1:
                pause_time = random.uniform(0.5, 2.0)
                time.sleep(pause_time)
        
        # Wait for final replication
        self.wait_for_table_sync(table_name, expected_count=total_operations, max_wait_time=60)
        
        # Verify final state
        ch_count = len(self.ch.select(table_name))
        assert ch_count == total_operations, f"Expected {total_operations} records, got {ch_count}"
        
        print(f"Burst stress test completed: {total_operations} total records processed")

    @pytest.mark.integration
    @pytest.mark.performance 
    def test_sustained_load_stress(self):
        """Test sustained load over extended period"""
        table_name = "sustained_load_table"
        
        # Create optimized table
        schema_sql = SchemaFactory.basic_user_table(table_name, ["score int", "metadata json"])
        self.mysql.execute(schema_sql)
        
        # Start replication  
        self.start_replication()
        self.wait_for_table_sync(table_name, expected_count=0)
        
        # Sustained load parameters
        duration_seconds = 60  # 1 minute sustained load
        target_rate = 100  # Target 100 operations per second
        operation_interval = 1.0 / target_rate
        
        operations_executed = 0
        start_time = time.time()
        
        while (time.time() - start_time) < duration_seconds:
            operation_start = time.time()
            
            # Execute operation
            operation_type = random.choice(["insert", "update"])
            
            if operation_type == "insert":
                record = {
                    "name": f"SustainedUser_{operations_executed}",
                    "age": random.randint(18, 65),
                    "score": random.randint(0, 100),
                    "metadata": '{"test": "sustained_load"}'
                }
                self.insert_multiple_records(table_name, [record])
            else:
                # Update random existing record
                if operations_executed > 0:
                    update_id = random.randint(1, min(operations_executed, 100))
                    self.mysql.execute(
                        f"UPDATE `{table_name}` SET score = %s WHERE id = %s",
                        commit=True,
                        args=(random.randint(0, 100), update_id)
                    )
            
            operations_executed += 1
            
            # Control rate
            operation_time = time.time() - operation_start
            if operation_time < operation_interval:
                time.sleep(operation_interval - operation_time)
            
            # Progress reporting
            if operations_executed % 500 == 0:
                elapsed = time.time() - start_time
                current_rate = operations_executed / elapsed
                print(f"Sustained load progress: {operations_executed} ops in {elapsed:.1f}s ({current_rate:.1f} ops/sec)")
        
        total_time = time.time() - start_time
        actual_rate = operations_executed / total_time
        
        # Wait for replication to catch up
        self.wait_for_stable_state(table_name, expected_count=None, max_wait_time=60)
        
        # Final verification
        mysql_count = len(self.mysql.fetch_all(f"SELECT * FROM `{table_name}`"))
        ch_count = len(self.ch.select(table_name))
        
        print(f"Sustained Load Test Results:")
        print(f"- Duration: {total_time:.1f}s")
        print(f"- Operations: {operations_executed}")
        print(f"- Rate: {actual_rate:.1f} ops/sec")
        print(f"- MySQL final count: {mysql_count}")  
        print(f"- ClickHouse final count: {ch_count}")
        
        # Assertions
        assert actual_rate > 50, f"Sustained rate too low: {actual_rate:.1f} ops/sec"
        assert abs(mysql_count - ch_count) <= 10, f"Count difference too large: {abs(mysql_count - ch_count)}"