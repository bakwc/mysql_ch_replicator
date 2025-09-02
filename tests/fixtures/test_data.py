"""Test data generators for various scenarios"""

import datetime
from decimal import Decimal
from typing import Any, Dict, List


class TestDataGenerator:
    """Generate test data for various scenarios"""

    @staticmethod
    def basic_users() -> List[Dict[str, Any]]:
        """Generate basic user test data"""
        return [
            {"name": "Ivan", "age": 42},
            {"name": "Peter", "age": 33},
            {"name": "Mary", "age": 25},
            {"name": "John", "age": 28},
            {"name": "Alice", "age": 31},
        ]

    @staticmethod
    def users_with_blobs() -> List[Dict[str, Any]]:
        """Generate users with blob/text data"""
        return [
            {"name": "Ivan", "age": 42, "field1": "test1", "field2": "test2"},
            {"name": "Peter", "age": 33, "field1": None, "field2": None},
            {
                "name": "Mary",
                "age": 25,
                "field1": "long text data",
                "field2": "binary data",
            },
        ]

    @staticmethod
    def datetime_records() -> List[Dict[str, Any]]:
        """Generate records with datetime fields"""
        return [
            {
                "name": "Ivan",
                "modified_date": "2023-01-01 10:00:00",
                "test_date": datetime.date(2015, 5, 28),
            },
            {
                "name": "Alex",
                "modified_date": "2023-01-01 10:00:00",
                "test_date": datetime.date(2015, 6, 2),
            },
            {
                "name": "Givi",
                "modified_date": datetime.datetime(2023, 1, 8, 3, 11, 9),
                "test_date": datetime.date(2015, 6, 2),
            },
        ]

    @staticmethod
    def complex_employee_records() -> List[Dict[str, Any]]:
        """Generate complex employee records"""
        return [
            {
                "name": "Ivan",
                "employee": 0,
                "position": 0,
                "job_title": 0,
                "department": 0,
                "job_level": 0,
                "job_grade": 0,
                "level": 0,
                "team": 0,
                "factory": 0,
                "ship": 0,
                "report_to": 0,
                "line_manager": 0,
                "location": 0,
                "customer": 0,
                "effective_date": "2023-01-01",
                "status": 0,
                "promotion": 0,
                "promotion_id": 0,
                "note": "",
                "is_change_probation_time": 0,
                "deleted": 0,
                "created_by": 0,
                "created_by_name": "",
                "created_date": "2023-01-01 10:00:00",
                "modified_by": 0,
                "modified_by_name": "",
                "modified_date": "2023-01-01 10:00:00",
                "entity": 0,
                "sent_2_tac": "0",
            },
            {
                "name": "Alex",
                "employee": 0,
                "position": 0,
                "job_title": 0,
                "department": 0,
                "job_level": 0,
                "job_grade": 0,
                "level": 0,
                "team": 0,
                "factory": 0,
                "ship": 0,
                "report_to": 0,
                "line_manager": 0,
                "location": 0,
                "customer": 0,
                "effective_date": "2023-01-01",
                "status": 0,
                "promotion": 0,
                "promotion_id": 0,
                "note": "",
                "is_change_probation_time": 0,
                "deleted": 0,
                "created_by": 0,
                "created_by_name": "",
                "created_date": "2023-01-01 10:00:00",
                "modified_by": 0,
                "modified_by_name": "",
                "modified_date": "2023-01-01 10:00:00",
                "entity": 0,
                "sent_2_tac": "0",
            },
        ]

    @staticmethod
    def spatial_records() -> List[Dict[str, Any]]:
        """Generate records with spatial data"""
        return [
            {
                "name": "Ivan",
                "age": 42,
                "rate": None,
                "coordinate": "POINT(10.0, 20.0)",
            },
            {
                "name": "Peter",
                "age": 33,
                "rate": None,
                "coordinate": "POINT(15.0, 25.0)",
            },
        ]

    @staticmethod
    def reserved_keyword_records() -> List[Dict[str, Any]]:
        """Generate records for reserved keyword table"""
        return [
            {"name": "Peter", "age": 33, "rate": Decimal("10.2")},
            {"name": "Mary", "age": 25, "rate": Decimal("15.5")},
            {"name": "John", "age": 28, "rate": Decimal("12.8")},
        ]

    @staticmethod
    def incremental_data(
        base_records: List[Dict[str, Any]], start_id: int = 1000
    ) -> List[Dict[str, Any]]:
        """Generate incremental test data based on existing records"""
        incremental = []
        for i, record in enumerate(base_records):
            new_record = record.copy()
            new_record["id"] = start_id + i
            # Modify some fields to make it different
            if "age" in new_record:
                new_record["age"] = new_record["age"] + 10
            if "name" in new_record:
                new_record["name"] = f"{new_record['name']}_updated"
            incremental.append(new_record)
        return incremental
