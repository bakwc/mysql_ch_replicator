"""Data consistency and checksum validation tests"""

import hashlib
import time
from decimal import Decimal

import pytest

from tests.base import BaseReplicationTest, DataTestMixin, SchemaTestMixin
from tests.conftest import TEST_TABLE_NAME


class TestDataConsistency(BaseReplicationTest, SchemaTestMixin, DataTestMixin):
    """Test data consistency and checksum validation between MySQL and ClickHouse"""

    @pytest.mark.integration
    def test_checksum_validation_basic_data(self):
        """Test checksum validation for basic data types"""
        # Create table with diverse data types
        self.mysql.execute(f"""
        CREATE TABLE `{TEST_TABLE_NAME}` (
            id int NOT NULL AUTO_INCREMENT,
            name varchar(255),
            age int,
            salary decimal(10,2),
            is_active boolean,
            created_at timestamp DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (id)
        );
        """)

        # Insert test data with known values
        test_data = [
            {
                "name": "Alice Johnson", 
                "age": 30, 
                "salary": Decimal("75000.50"), 
                "is_active": True
            },
            {
                "name": "Bob Smith", 
                "age": 25, 
                "salary": Decimal("60000.00"), 
                "is_active": False
            },
            {
                "name": "Carol Davis", 
                "age": 35, 
                "salary": Decimal("85000.75"), 
                "is_active": True
            },
        ]

        self.insert_multiple_records(TEST_TABLE_NAME, test_data)

        # Start replication
        self.start_replication()
        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=3)

        # Calculate checksums for both MySQL and ClickHouse
        mysql_checksum = self._calculate_table_checksum_mysql(TEST_TABLE_NAME)
        clickhouse_checksum = self._calculate_table_checksum_clickhouse(TEST_TABLE_NAME)

        # Add debugging information
        mysql_data = self._get_normalized_data_mysql(TEST_TABLE_NAME)
        clickhouse_data = self._get_normalized_data_clickhouse(TEST_TABLE_NAME)
        
        if mysql_checksum != clickhouse_checksum:
            print(f"MySQL normalized data: {mysql_data}")
            print(f"ClickHouse normalized data: {clickhouse_data}")

        # Checksums should match
        assert mysql_checksum == clickhouse_checksum, (
            f"Data checksum mismatch: MySQL={mysql_checksum}, ClickHouse={clickhouse_checksum}"
        )

        # Add more data and verify consistency
        additional_data = [
            {
                "name": "David Wilson",
                "age": 28,
                "salary": Decimal("70000.00"),
                "is_active": True
            }
        ]

        self.insert_multiple_records(TEST_TABLE_NAME, additional_data)
        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=4)

        # Recalculate and verify checksums
        mysql_checksum_2 = self._calculate_table_checksum_mysql(TEST_TABLE_NAME)
        clickhouse_checksum_2 = self._calculate_table_checksum_clickhouse(TEST_TABLE_NAME)

        assert mysql_checksum_2 == clickhouse_checksum_2, (
            "Checksums don't match after additional data insertion"
        )
        assert mysql_checksum != mysql_checksum_2, "Checksum should change after data modification"

    @pytest.mark.integration
    def test_row_level_consistency_verification(self):
        """Test row-by-row data consistency verification"""
        # Create table for detailed comparison
        self.mysql.execute(f"""
        CREATE TABLE `{TEST_TABLE_NAME}` (
            id int NOT NULL AUTO_INCREMENT,
            code varchar(50),
            value decimal(12,4),
            description text,
            flags json,
            PRIMARY KEY (id)
        );
        """)

        # Insert data with complex types
        complex_data = [
            {
                "code": "TEST_001",
                "value": Decimal("123.4567"),
                "description": "First test record with unicode: 测试数据",
                "flags": '{"active": true, "priority": 1, "tags": ["test", "data"]}'
            },
            {
                "code": "TEST_002", 
                "value": Decimal("987.6543"),
                "description": "Second test record with symbols: !@#$%^&*()",
                "flags": '{"active": false, "priority": 2, "tags": []}'
            }
        ]

        self.insert_multiple_records(TEST_TABLE_NAME, complex_data)

        # Start replication
        self.start_replication()
        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=2)

        # Perform row-level consistency check
        mysql_rows = self._get_sorted_table_data_mysql(TEST_TABLE_NAME)
        clickhouse_rows = self._get_sorted_table_data_clickhouse(TEST_TABLE_NAME)

        assert len(mysql_rows) == len(clickhouse_rows), (
            f"Row count mismatch: MySQL={len(mysql_rows)}, ClickHouse={len(clickhouse_rows)}"
        )

        # Compare each row
        for i, (mysql_row, ch_row) in enumerate(zip(mysql_rows, clickhouse_rows)):
            self._compare_row_data(mysql_row, ch_row, f"Row {i}")

    def _normalize_value(self, value):
        """Normalize a value for consistent comparison"""
        # Handle timezone-aware datetime by removing timezone info
        if hasattr(value, 'replace') and hasattr(value, 'tzinfo') and value.tzinfo is not None:
            value = value.replace(tzinfo=None)
        
        # Convert boolean integers to booleans for consistency
        if isinstance(value, int) and value in (0, 1):
            # Keep as int to match MySQL behavior
            pass
        
        # Convert booleans to integers to match MySQL storage
        if isinstance(value, bool):
            value = 1 if value else 0
            
        # Convert float/Decimal to consistent format (2 decimal places for currency)
        if isinstance(value, (float, Decimal)):
            # For currency-like values, format to 2 decimal places
            value = f"{float(value):.2f}"
            
        return value

    def _calculate_table_checksum_mysql(self, table_name):
        """Calculate checksum for MySQL table data (normalized format)"""
        # Get data in consistent order using proper context manager
        query = f"SELECT * FROM `{table_name}` ORDER BY id"
        with self.mysql.get_connection() as (connection, cursor):
            cursor.execute(query)
            rows = cursor.fetchall()
            
            # Get column names within the same connection context
            cursor.execute(f"DESCRIBE `{table_name}`")
            columns = [col[0] for col in cursor.fetchall()]
        
        # Normalize data: convert to sorted tuples of (key, value) pairs
        normalized_rows = []
        if rows:
            for row in rows:
                # Create dict and exclude internal ClickHouse columns for comparison
                row_dict = dict(zip(columns, row))
                # Remove internal ClickHouse columns that don't exist in MySQL
                filtered_dict = {k: v for k, v in row_dict.items() if not k.startswith('_')}
                # Normalize values for consistent comparison
                normalized_dict = {k: self._normalize_value(v) for k, v in filtered_dict.items()}
                normalized_rows.append(tuple(sorted(normalized_dict.items())))
        
        # Create deterministic string representation
        data_str = "|".join([str(row) for row in normalized_rows])
        return hashlib.md5(data_str.encode('utf-8')).hexdigest()

    def _calculate_table_checksum_clickhouse(self, table_name):
        """Calculate checksum for ClickHouse table data (normalized format)"""
        # Get data in consistent order
        rows = self.ch.select(table_name, order_by="id")
        
        # Normalize data: convert to sorted tuples of (key, value) pairs
        normalized_rows = []
        for row in rows:
            # Remove internal ClickHouse columns that don't exist in MySQL
            filtered_dict = {k: v for k, v in row.items() if not k.startswith('_')}
            # Normalize values for consistent comparison
            normalized_dict = {k: self._normalize_value(v) for k, v in filtered_dict.items()}
            normalized_rows.append(tuple(sorted(normalized_dict.items())))
        
        # Create deterministic string representation
        data_str = "|".join([str(row) for row in normalized_rows])
        return hashlib.md5(data_str.encode('utf-8')).hexdigest()

    def _get_sorted_table_data_mysql(self, table_name):
        """Get sorted table data from MySQL"""
        query = f"SELECT * FROM `{table_name}` ORDER BY id"
        with self.mysql.get_connection() as (connection, cursor):
            cursor.execute(query)
            return cursor.fetchall()

    def _get_sorted_table_data_clickhouse(self, table_name):
        """Get sorted table data from ClickHouse"""
        return self.ch.select(table_name, order_by="id")

    def _compare_row_data(self, mysql_row, ch_row, context=""):
        """Compare individual row data between MySQL and ClickHouse"""
        # Convert ClickHouse row to tuple for comparison, filtering out internal columns
        if isinstance(ch_row, dict):
            # Filter out internal ClickHouse columns that don't exist in MySQL
            filtered_ch_row = {k: v for k, v in ch_row.items() if not k.startswith('_')}
            ch_values = tuple(filtered_ch_row.values())
        else:
            ch_values = ch_row

        # Compare values (allowing for minor type differences)
        assert len(mysql_row) == len(ch_values), (
            f"{context}: Column count mismatch - MySQL: {len(mysql_row)}, ClickHouse: {len(ch_values)}"
        )

        for i, (mysql_val, ch_val) in enumerate(zip(mysql_row, ch_values)):
            # Handle type conversions and None values
            if mysql_val is None and ch_val is None:
                continue
            elif mysql_val is None or ch_val is None:
                assert False, f"{context}, Column {i}: NULL mismatch - MySQL: {mysql_val}, ClickHouse: {ch_val}"
            
            # Handle decimal precision differences
            if isinstance(mysql_val, Decimal) and isinstance(ch_val, (float, Decimal)):
                assert abs(float(mysql_val) - float(ch_val)) < 0.001, (
                    f"{context}, Column {i}: Decimal precision mismatch - MySQL: {mysql_val}, ClickHouse: {ch_val}"
                )
            else:
                assert str(mysql_val) == str(ch_val), (
                    f"{context}, Column {i}: Value mismatch - MySQL: {mysql_val} ({type(mysql_val)}), "
                    f"ClickHouse: {ch_val} ({type(ch_val)})"
                )

    def _get_normalized_data_mysql(self, table_name):
        """Get normalized data from MySQL for debugging"""
        query = f"SELECT * FROM `{table_name}` ORDER BY id"
        with self.mysql.get_connection() as (connection, cursor):
            cursor.execute(query)
            rows = cursor.fetchall()
            
            cursor.execute(f"DESCRIBE `{table_name}`")
            columns = [col[0] for col in cursor.fetchall()]
        
        normalized_rows = []
        if rows:
            for row in rows:
                row_dict = dict(zip(columns, row))
                filtered_dict = {k: v for k, v in row_dict.items() if not k.startswith('_')}
                normalized_dict = {k: self._normalize_value(v) for k, v in filtered_dict.items()}
                normalized_rows.append(tuple(sorted(normalized_dict.items())))
        
        return normalized_rows

    def _get_normalized_data_clickhouse(self, table_name):
        """Get normalized data from ClickHouse for debugging"""
        rows = self.ch.select(table_name, order_by="id")
        
        normalized_rows = []
        for row in rows:
            filtered_dict = {k: v for k, v in row.items() if not k.startswith('_')}
            normalized_dict = {k: self._normalize_value(v) for k, v in filtered_dict.items()}
            normalized_rows.append(tuple(sorted(normalized_dict.items())))
        
        return normalized_rows