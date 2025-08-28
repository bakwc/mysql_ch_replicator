"""Percona-specific feature tests for MySQL ClickHouse Replicator"""

import pytest

from tests.base import BaseReplicationTest, DataTestMixin, SchemaTestMixin
from tests.conftest import TEST_TABLE_NAME


class TestPerconaFeatures(BaseReplicationTest, SchemaTestMixin, DataTestMixin):
    """Test Percona-specific features and optimizations"""

    @pytest.mark.integration
    def test_percona_audit_log_compatibility(self):
        """Test that replication works with Percona audit log enabled"""
        # Create basic table for testing
        self.create_basic_table(TEST_TABLE_NAME)

        # Insert test data
        test_data = [
            {"name": "PerconaUser1", "age": 25},
            {"name": "PerconaUser2", "age": 30},
            {"name": "PerconaUser3", "age": 35}
        ]
        self.insert_multiple_records(TEST_TABLE_NAME, test_data)

        # Start replication
        self.start_replication()
        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=3)

        # Verify all records were replicated correctly
        for record in test_data:
            self.verify_record_exists(TEST_TABLE_NAME, f"name='{record['name']}'", {"age": record["age"]})

    @pytest.mark.integration
    def test_percona_slow_query_log_compatibility(self):
        """Test replication with slow query log enabled"""
        # Create table with more complex structure
        self.mysql.execute(f"""
        CREATE TABLE `{TEST_TABLE_NAME}` (
            id int NOT NULL AUTO_INCREMENT,
            name varchar(255),
            metadata json,
            created_at timestamp DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (id),
            INDEX idx_name (name),
            INDEX idx_created (created_at)
        );
        """)

        # Insert test data that might trigger slow query log
        import json
        test_data = [
            {
                "name": "SlowQueryTest1",
                "metadata": json.dumps({
                    "performance": {"slow_query": True, "execution_time": "5s"},
                    "tags": ["percona", "slow", "test"]
                })
            },
            {
                "name": "SlowQueryTest2", 
                "metadata": json.dumps({
                    "performance": {"slow_query": False, "execution_time": "0.1s"},
                    "tags": ["percona", "fast", "test"]
                })
            }
        ]

        self.insert_multiple_records(TEST_TABLE_NAME, test_data)

        # Start replication
        self.start_replication()
        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=2)

        # Verify records with complex queries
        self.verify_record_exists(TEST_TABLE_NAME, "name='SlowQueryTest1'")
        self.verify_record_exists(TEST_TABLE_NAME, "name='SlowQueryTest2'")

    @pytest.mark.integration
    def test_percona_query_response_time_compatibility(self):
        """Test that replication works with query response time plugin enabled"""
        # Create table with performance-sensitive structure
        self.mysql.execute(f"""
        CREATE TABLE `{TEST_TABLE_NAME}` (
            id int NOT NULL AUTO_INCREMENT,
            name varchar(255),
            data longtext,
            performance_metric decimal(10,4),
            PRIMARY KEY (id),
            INDEX idx_performance (performance_metric),
            FULLTEXT idx_data (data)
        );
        """)

        # Insert data that exercises different performance characteristics
        test_data = [
            {
                "name": "FastOperation",
                "data": "Small amount of data for fast operations",
                "performance_metric": 0.0001
            },
            {
                "name": "MediumOperation", 
                "data": "Medium amount of data " * 100,  # Repeat for larger content
                "performance_metric": 0.1
            },
            {
                "name": "SlowOperation",
                "data": "Large amount of data " * 1000,  # Much larger content
                "performance_metric": 1.5
            }
        ]

        self.insert_multiple_records(TEST_TABLE_NAME, test_data)

        # Start replication
        self.start_replication()
        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=3)

        # Verify all performance test records
        for record in test_data:
            self.verify_record_exists(
                TEST_TABLE_NAME, 
                f"name='{record['name']}'",
                {"performance_metric": record["performance_metric"]}
            )

    @pytest.mark.integration
    def test_percona_innodb_optimizations(self):
        """Test replication with Percona InnoDB optimizations"""
        # Create table that benefits from InnoDB optimizations
        self.mysql.execute(f"""
        CREATE TABLE `{TEST_TABLE_NAME}` (
            id int NOT NULL AUTO_INCREMENT,
            name varchar(255),
            large_data longblob,
            transaction_data json,
            created_at timestamp DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (id),
            INDEX idx_created (created_at)
        ) ENGINE=InnoDB;
        """)

        # Insert data in batches to test transaction handling
        import json
        batch_1 = [
            {
                "name": f"BatchUser{i}",
                "large_data": b"Binary data content " * 100,  # Large binary data
                "transaction_data": json.dumps({
                    "batch": 1,
                    "user_id": i,
                    "transaction_time": "2024-01-01T12:00:00"
                })
            }
            for i in range(1, 6)
        ]

        batch_2 = [
            {
                "name": f"BatchUser{i}",
                "large_data": b"Different binary content " * 150,
                "transaction_data": json.dumps({
                    "batch": 2, 
                    "user_id": i,
                    "transaction_time": "2024-01-01T13:00:00"
                })
            }
            for i in range(6, 11)
        ]

        # Insert first batch
        self.insert_multiple_records(TEST_TABLE_NAME, batch_1)

        # Start replication
        self.start_replication()
        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=5)

        # Insert second batch during replication
        self.insert_multiple_records(TEST_TABLE_NAME, batch_2)
        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=10)

        # Verify all batches were replicated correctly
        for i in range(1, 11):
            self.verify_record_exists(TEST_TABLE_NAME, f"name='BatchUser{i}'")

    @pytest.mark.integration
    def test_percona_gtid_consistency(self):
        """Test GTID consistency with Percona-specific features"""
        # Create table for GTID testing
        self.create_basic_table(TEST_TABLE_NAME)

        # Insert initial data
        initial_data = [{"name": "GTIDTest1", "age": 20}]
        self.insert_multiple_records(TEST_TABLE_NAME, initial_data)

        # Start replication
        self.start_replication()
        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=1)

        # Perform multiple operations to test GTID handling
        operations = [
            {"name": "GTIDInsert", "age": 25},
            {"name": "GTIDUpdate", "age": 30},  # Will be updated below
        ]

        self.insert_multiple_records(TEST_TABLE_NAME, operations)
        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=3)

        # Update a record to test GTID with updates
        self.update_record(TEST_TABLE_NAME, "name='GTIDUpdate'", {"age": 35})
        self.wait_for_record_update(TEST_TABLE_NAME, "name='GTIDUpdate'", {"age": 35})

        # Delete a record to test GTID with deletes  
        self.delete_records(TEST_TABLE_NAME, "name='GTIDTest1'")
        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=2)

        # Verify final state
        self.verify_record_exists(TEST_TABLE_NAME, "name='GTIDInsert'", {"age": 25})
        self.verify_record_exists(TEST_TABLE_NAME, "name='GTIDUpdate'", {"age": 35})
        self.verify_record_does_not_exist(TEST_TABLE_NAME, "name='GTIDTest1'")

    @pytest.mark.integration
    def test_percona_character_set_handling(self):
        """Test character set handling with Percona-specific configurations"""
        # Create table with specific character sets
        self.mysql.execute(f"""
        CREATE TABLE `{TEST_TABLE_NAME}` (
            id int NOT NULL AUTO_INCREMENT,
            name varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
            description text CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
            latin_data varchar(255) CHARACTER SET latin1,
            PRIMARY KEY (id)
        );
        """)

        # Test data with various character sets and encodings
        test_data = [
            {
                "name": "UnicodeTest",
                "description": "Testing émojis 🎉 and spëcial çharacters αβγ العربية 测试",
                "latin_data": "Simple ASCII text only"
            },
            {
                "name": "LatinTest",
                "description": "Standard Latin characters: àáâãäå æç èéêë",
                "latin_data": "Latin-1 compatible text"
            },
            {
                "name": "PerconaCharTest",
                "description": "Percona specific test with mixed: русский 日本語 한국어",
                "latin_data": "Basic Latin only"
            }
        ]

        self.insert_multiple_records(TEST_TABLE_NAME, test_data)

        # Start replication
        self.start_replication()
        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=3)

        # Verify character set preservation
        for record in test_data:
            self.verify_record_exists(TEST_TABLE_NAME, f"name='{record['name']}'")
            
            # Get the actual record and verify content integrity
            ch_record = self.ch.select(TEST_TABLE_NAME, where=f"name='{record['name']}'")[0]
            assert ch_record['description'] == record['description'], \
                f"Description mismatch for {record['name']}"
            assert ch_record['latin_data'] == record['latin_data'], \
                f"Latin data mismatch for {record['name']}"