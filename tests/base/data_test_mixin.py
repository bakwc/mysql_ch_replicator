"""Mixin for data-related test operations"""

import datetime
from decimal import Decimal
from typing import Any, Dict, List


class DataTestMixin:
    """Mixin providing common data operation methods"""

    def _format_sql_value(self, value):
        """Convert a Python value to SQL format with proper escaping"""
        if value is None:
            return "NULL"
        elif isinstance(value, str):
            # Escape single quotes and backslashes for SQL safety
            escaped_value = value.replace("\\", "\\\\").replace("'", "\\'")
            return f"'{escaped_value}'"
        elif isinstance(value, bytes):
            # Decode bytes and escape special characters
            decoded_value = value.decode('utf-8', errors='replace')
            escaped_value = decoded_value.replace("\\", "\\\\").replace("'", "\\'")
            return f"'{escaped_value}'"
        elif isinstance(value, (datetime.datetime, datetime.date)):
            return f"'{value}'"
        elif isinstance(value, Decimal):
            return str(value)
        elif isinstance(value, bool):
            return "1" if value else "0"
        else:
            return str(value)

    def insert_basic_record(self, table_name, name, age, **kwargs):
        """Insert a basic record with name and age using parameterized queries"""
        # Build the field list and values
        fields = ["name", "age"]
        values = [name, age]
        
        if kwargs:
            fields.extend(kwargs.keys())
            values.extend(kwargs.values())
        
        fields_str = ", ".join(f"`{field}`" for field in fields)
        placeholders = ", ".join(["%s"] * len(values))
        
        self.mysql.execute(
            f"INSERT INTO `{table_name}` ({fields_str}) VALUES ({placeholders})",
            commit=True,
            args=values
        )

    def insert_multiple_records(self, table_name, records: List[Dict[str, Any]]):
        """Insert multiple records from list of dictionaries using parameterized queries"""
        for record in records:
            fields = ", ".join(f"`{field}`" for field in record.keys())
            placeholders = ", ".join(["%s"] * len(record))
            values = list(record.values())
            
            # Use parameterized query for better SQL injection protection
            self.mysql.execute(
                f"INSERT INTO `{table_name}` ({fields}) VALUES ({placeholders})",
                commit=True,
                args=values
            )

    def update_record(self, table_name, where_clause, updates: Dict[str, Any]):
        """Update records with given conditions using parameterized queries"""
        set_clause = ", ".join(f"`{field}` = %s" for field in updates.keys())
        values = list(updates.values())
        
        # Note: where_clause should be pre-constructed safely by the caller
        self.mysql.execute(
            f"UPDATE `{table_name}` SET {set_clause} WHERE {where_clause}",
            commit=True,
            args=values
        )

    def delete_records(self, table_name, where_clause):
        """Delete records matching condition"""
        self.mysql.execute(
            f"DELETE FROM `{table_name}` WHERE {where_clause};",
            commit=True,
        )

    def get_mysql_count(self, table_name, where_clause=""):
        """Get count of records in MySQL table"""
        where = f" WHERE {where_clause}" if where_clause else ""
        with self.mysql.get_connection() as (connection, cursor):
            cursor.execute(f"SELECT COUNT(*) FROM `{table_name}`{where}")
            return cursor.fetchone()[0]

    def get_clickhouse_count(self, table_name, where_clause=""):
        """Get count of records in ClickHouse table"""
        where = f" WHERE {where_clause}" if where_clause else ""
        result = self.ch.execute_query(f"SELECT COUNT(*) FROM `{table_name}`{where}")
        return result[0][0] if result else 0

    def verify_record_exists(self, table_name, where_clause, expected_fields=None):
        """Verify a record exists in ClickHouse with expected field values"""
        records = self.ch.select(table_name, where=where_clause)
        assert len(records) > 0, f"No records found with condition: {where_clause}"

        if expected_fields:
            record = records[0]
            for field, expected_value in expected_fields.items():
                actual_value = record.get(field)
                assert actual_value == expected_value, (
                    f"Field {field}: expected {expected_value}, got {actual_value}"
                )

        return records[0]

    def verify_counts_match(self, table_name, where_clause=""):
        """Verify MySQL and ClickHouse have same record count"""
        mysql_count = self.get_mysql_count(table_name, where_clause)
        ch_count = self.get_clickhouse_count(table_name, where_clause)
        assert mysql_count == ch_count, (
            f"Count mismatch: MySQL={mysql_count}, ClickHouse={ch_count}"
        )
        return mysql_count

    def wait_for_record_exists(self, table_name, where_clause, expected_fields=None, max_wait_time=20.0):
        """
        Wait for a record to exist in ClickHouse with expected field values
        
        Args:
            table_name: Name of the table to check
            where_clause: SQL WHERE condition to match
            expected_fields: Optional dict of field values to verify
            max_wait_time: Maximum time to wait in seconds
            
        Raises:
            AssertionError: If the record is not found within the timeout period
        """
        def condition():
            try:
                self.verify_record_exists(table_name, where_clause, expected_fields)
                return True
            except AssertionError:
                return False
        
        # Use wait_for_condition method from BaseReplicationTest
        try:
            self.wait_for_condition(condition, max_wait_time=max_wait_time)
        except AssertionError:
            # Provide helpful debugging information on timeout
            current_records = self.ch.select(table_name)
            raise AssertionError(
                f"Record not found in table '{table_name}' with condition '{where_clause}' "
                f"after {max_wait_time}s. Current records: {current_records}"
            )

    def wait_for_record_update(self, table_name, where_clause, expected_fields, max_wait_time=20.0):
        """Wait for a record to be updated with expected field values"""
        def condition():
            try:
                self.verify_record_exists(table_name, where_clause, expected_fields)
                return True
            except AssertionError:
                return False
        
        # Use wait_for_condition method from BaseReplicationTest
        self.wait_for_condition(condition, max_wait_time=max_wait_time)

    def verify_record_does_not_exist(self, table_name, where_clause):
        """Verify a record does not exist in ClickHouse"""
        records = self.ch.select(table_name, where=where_clause)
        assert len(records) == 0, f"Unexpected records found with condition: {where_clause}"

    def wait_for_stable_state(self, table_name, expected_count, max_wait_time=20.0):
        """Wait for table to reach and maintain a stable record count"""
        def condition():
            try:
                ch_count = self.get_clickhouse_count(table_name)
                return ch_count == expected_count
            except Exception:
                return False
        
        # Use wait_for_condition method from BaseReplicationTest
        self.wait_for_condition(condition, max_wait_time=max_wait_time)
