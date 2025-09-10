"""Mixin for data-related test operations"""

import datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List


class DataTestMixin:
    """Mixin providing common data operation methods"""

    def _refresh_database_context(self):
        """Refresh ClickHouse database context if database has transitioned from _tmp to final"""
        try:
            databases = self.ch.get_databases()
            current_db = self.ch.database
            if current_db and current_db.endswith('_tmp'):
                target_db = current_db.replace('_tmp', '')
                if target_db in databases and target_db != current_db:
                    print(f"DEBUG: Database transitioned from '{current_db}' to '{target_db}' during replication")
                    self.ch.update_database_context(target_db)
        except Exception as e:
            print(f"DEBUG: Error refreshing database context: {e}")
            # Continue with current context - don't fail the test on context refresh issues

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
        if not records:
            return
        
        # Build all INSERT commands with parameterized queries
        commands = []
        for record in records:
            fields = ", ".join(f"`{field}`" for field in record.keys())
            placeholders = ", ".join(["%s"] * len(record))
            values = list(record.values())
            
            # Add command and args as tuple for execute_batch
            commands.append((
                f"INSERT INTO `{table_name}` ({fields}) VALUES ({placeholders})",
                values
            ))
        
        # Execute all inserts in a single transaction using execute_batch
        # This ensures atomicity and proper binlog event ordering
        self.mysql.execute_batch(commands, commit=True)

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
        # Refresh database context before querying (might have changed during replication)
        self._refresh_database_context()
        records = self.ch.select(table_name, where=where_clause)
        return len(records) if records else 0

    def _normalize_datetime_comparison(self, expected_value, actual_value):
        """Normalize datetime values for comparison between MySQL and ClickHouse"""
        import datetime
        
        # Handle datetime vs datetime comparison (timezone-aware vs naive)
        if isinstance(expected_value, datetime.datetime) and isinstance(actual_value, datetime.datetime):
            # If actual has timezone info but expected is naive, compare without timezone
            if actual_value.tzinfo is not None and expected_value.tzinfo is None:
                # Convert timezone-aware datetime to naive datetime
                actual_naive = actual_value.replace(tzinfo=None)
                return expected_value == actual_naive
            # If both are timezone-aware or both are naive, direct comparison
            return expected_value == actual_value
        
        # Handle datetime vs string comparison
        if isinstance(expected_value, datetime.datetime) and isinstance(actual_value, str):
            try:
                # Remove timezone info if present for comparison
                if '+' in actual_value and actual_value.endswith('+00:00'):
                    actual_value = actual_value[:-6]
                elif actual_value.endswith('Z'):
                    actual_value = actual_value[:-1]
                
                # Parse the string back to datetime
                actual_datetime = datetime.datetime.fromisoformat(actual_value)
                return expected_value == actual_datetime
            except (ValueError, TypeError):
                # If parsing fails, fall back to string comparison
                return str(expected_value) == str(actual_value)
        
        # Handle date vs string comparison
        if isinstance(expected_value, datetime.date) and isinstance(actual_value, str):
            try:
                actual_date = datetime.datetime.fromisoformat(actual_value).date()
                return expected_value == actual_date
            except (ValueError, TypeError):
                return str(expected_value) == str(actual_value)
        
        # Handle Decimal comparisons - ClickHouse may return float or string for decimals
        if isinstance(expected_value, Decimal):
            try:
                if isinstance(actual_value, (float, int)):
                    # Convert float/int to Decimal for comparison
                    actual_decimal = Decimal(str(actual_value))
                    return expected_value == actual_decimal
                elif isinstance(actual_value, str):
                    # Parse string as Decimal
                    actual_decimal = Decimal(actual_value)
                    return expected_value == actual_decimal
                elif isinstance(actual_value, Decimal):
                    return expected_value == actual_value
            except (ValueError, TypeError, InvalidOperation):
                # Fall back to string comparison if decimal parsing fails
                return str(expected_value) == str(actual_value)
        
        # Default comparison for all other cases
        return expected_value == actual_value

    def verify_record_exists(self, table_name, where_clause, expected_fields=None):
        """Verify a record exists in ClickHouse with expected field values"""
        # Refresh database context before querying (might have changed during replication)
        self._refresh_database_context()
        records = self.ch.select(table_name, where=where_clause)
        assert len(records) > 0, f"No records found with condition: {where_clause}"

        if expected_fields:
            record = records[0]
            for field, expected_value in expected_fields.items():
                actual_value = record.get(field)
                
                # Use normalized comparison for datetime values
                if self._normalize_datetime_comparison(expected_value, actual_value):
                    # Normalized comparison passed, continue to next field
                    continue
                
                # Try numeric comparison for decimal/float precision issues
                try:
                    if isinstance(expected_value, (int, float, Decimal)) and isinstance(actual_value, (int, float, Decimal)):
                        # Convert to float for comparison to handle decimal precision
                        if float(expected_value) == float(actual_value):
                            continue
                except (TypeError, ValueError):
                    pass
                
                # If normalized comparison failed or not applicable, use standard comparison
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
            # Refresh database context before debugging query
            self._refresh_database_context()
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
        # Refresh database context before querying (might have changed during replication)
        self._refresh_database_context()
        records = self.ch.select(table_name, where=where_clause)
        assert len(records) == 0, f"Unexpected records found with condition: {where_clause}"

    def wait_for_stable_state(self, table_name, expected_count=None, max_wait_time=20.0):
        """Wait for table to reach and maintain a stable record count"""
        def condition():
            try:
                ch_count = self.get_clickhouse_count(table_name)
                if expected_count is None:
                    # Just wait for table to exist and have some records
                    return ch_count >= 0  # Table exists
                return ch_count == expected_count
            except Exception as e:
                print(f"DEBUG: wait_for_stable_state error: {e}")
                return False
        
        # Use wait_for_condition method from BaseReplicationTest
        self.wait_for_condition(condition, max_wait_time=max_wait_time)
