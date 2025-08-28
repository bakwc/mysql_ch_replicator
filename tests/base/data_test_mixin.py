"""Mixin for data-related test operations"""

import datetime
from decimal import Decimal
from typing import Any, Dict, List


class DataTestMixin:
    """Mixin providing common data operation methods"""

    def _format_sql_value(self, value):
        """Convert a Python value to SQL format"""
        if value is None:
            return "NULL"
        elif isinstance(value, str):
            return f"'{value}'"
        elif isinstance(value, bytes):
            return f"'{value.decode('utf-8', errors='replace')}'"
        elif isinstance(value, (datetime.datetime, datetime.date)):
            return f"'{value}'"
        elif isinstance(value, Decimal):
            return str(value)
        elif isinstance(value, bool):
            return "1" if value else "0"
        else:
            return str(value)

    def insert_basic_record(self, table_name, name, age, **kwargs):
        """Insert a basic record with name and age"""
        extra_fields = ""
        extra_values = ""

        if kwargs:
            fields = list(kwargs.keys())
            values = list(kwargs.values())
            extra_fields = ", " + ", ".join(fields)
            extra_values = ", " + ", ".join(self._format_sql_value(v) for v in values)

        self.mysql.execute(
            f"INSERT INTO `{table_name}` (name, age{extra_fields}) VALUES ('{name}', {age}{extra_values});",
            commit=True,
        )

    def insert_multiple_records(self, table_name, records: List[Dict[str, Any]]):
        """Insert multiple records from list of dictionaries"""
        for record in records:
            fields = ", ".join(record.keys())
            values = ", ".join(self._format_sql_value(v) for v in record.values())
            self.mysql.execute(
                f"INSERT INTO `{table_name}` ({fields}) VALUES ({values});",
                commit=True,
            )

    def update_record(self, table_name, where_clause, updates: Dict[str, Any]):
        """Update records with given conditions"""
        set_clause = ", ".join(
            f"{field} = {self._format_sql_value(value)}"
            for field, value in updates.items()
        )
        self.mysql.execute(
            f"UPDATE `{table_name}` SET {set_clause} WHERE {where_clause};",
            commit=True,
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
