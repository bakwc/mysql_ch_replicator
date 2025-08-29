"""Comprehensive JSON data type testing including Unicode keys and complex structures"""

import json

import pytest

from tests.base import IsolatedBaseReplicationTest, DataTestMixin, SchemaTestMixin
from tests.conftest import TEST_TABLE_NAME


class TestJsonComprehensive(IsolatedBaseReplicationTest, SchemaTestMixin, DataTestMixin):
    """Test comprehensive JSON data type handling including Unicode keys"""

    @pytest.mark.integration
    def test_json_basic_operations(self):
        """Test basic JSON data type operations"""
        # Create table with JSON columns
        self.mysql.execute(f"""
        CREATE TABLE `{TEST_TABLE_NAME}` (
            id int NOT NULL AUTO_INCREMENT,
            name varchar(255),
            profile json,
            settings json,
            metadata json,
            PRIMARY KEY (id)
        );
        """)

        # Insert JSON test data
        json_data = [
            {
                "name": "User1",
                "profile": json.dumps({
                    "firstName": "John",
                    "lastName": "Doe",
                    "age": 30,
                    "isActive": True,
                    "skills": ["Python", "MySQL", "ClickHouse"]
                }),
                "settings": json.dumps({
                    "theme": "dark",
                    "notifications": {"email": True, "sms": False},
                    "preferences": {"language": "en", "timezone": "UTC"}
                }),
                "metadata": json.dumps({
                    "created": "2023-01-15T10:30:00Z",
                    "lastLogin": "2023-06-15T14:22:30Z",
                    "loginCount": 42
                })
            }
        ]

        self.insert_multiple_records(TEST_TABLE_NAME, json_data)

        # Start replication
        self.start_replication()
        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=1)

        # Verify JSON data integrity
        records = self.ch.select(TEST_TABLE_NAME)
        user_record = records[0]
        
        # Parse and verify JSON content
        profile = json.loads(user_record["profile"])
        settings = json.loads(user_record["settings"])
        
        assert profile["firstName"] == "John"
        assert profile["age"] == 30
        assert settings["theme"] == "dark"
        assert len(profile["skills"]) == 3

    @pytest.mark.integration  
    def test_json_unicode_keys(self):
        """Test JSON with Unicode (non-Latin) keys and values"""
        # Create table with JSON column
        self.mysql.execute(f"""
        CREATE TABLE `{TEST_TABLE_NAME}` (
            id int NOT NULL AUTO_INCREMENT,
            name varchar(255),
            data json,
            PRIMARY KEY (id)
        );
        """)

        # Insert JSON data with Unicode keys (Cyrillic, Arabic, Chinese)
        unicode_data = [
            {
                "name": "Unicode Test 1",
                "data": json.dumps({
                    "а": "б",  # Cyrillic
                    "в": [1, 2, 3],
                    "中文": "测试",  # Chinese
                    "العربية": "نص"  # Arabic
                })
            },
            {
                "name": "Unicode Test 2", 
                "data": json.dumps({
                    "在": "值",
                    "ключ": {"nested": "значение"},
                    "مفتاح": ["array", "values"]
                })
            }
        ]

        self.insert_multiple_records(TEST_TABLE_NAME, unicode_data)

        # Start replication
        self.start_replication()
        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=2)

        # Verify Unicode JSON data
        records = self.ch.select(TEST_TABLE_NAME, order_by="id")
        
        # Test first record
        data1 = json.loads(records[0]["data"])
        assert data1["а"] == "б"
        assert data1["в"] == [1, 2, 3]
        assert data1["中文"] == "测试"
        
        # Test second record  
        data2 = json.loads(records[1]["data"])
        assert data2["在"] == "值"
        assert data2["ключ"]["nested"] == "значение"
        assert isinstance(data2["مفتاح"], list)

    @pytest.mark.integration
    def test_json_complex_structures(self):
        """Test complex nested JSON structures"""
        # Create table
        self.mysql.execute(f"""
        CREATE TABLE `{TEST_TABLE_NAME}` (
            id int NOT NULL AUTO_INCREMENT,
            name varchar(255),
            complex_data json,
            PRIMARY KEY (id)
        );
        """)

        # Complex nested JSON data
        complex_data = [
            {
                "name": "Complex Structure",
                "complex_data": json.dumps({
                    "level1": {
                        "level2": {
                            "level3": {
                                "arrays": [[1, 2], [3, 4]],
                                "mixed": [
                                    {"type": "object", "value": 100},
                                    {"type": "string", "value": "test"},
                                    {"type": "null", "value": None}
                                ]
                            }
                        }
                    },
                    "metadata": {
                        "version": "1.0",
                        "features": ["a", "b", "c"],
                        "config": {
                            "enabled": True,
                            "timeout": 30,
                            "retry": {"max": 3, "delay": 1000}
                        }
                    }
                })
            }
        ]

        self.insert_multiple_records(TEST_TABLE_NAME, complex_data)
        
        # Start replication
        self.start_replication()
        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=1)

        # Verify complex nested structure
        record = self.ch.select(TEST_TABLE_NAME)[0]
        data = json.loads(record["complex_data"])
        
        # Deep nested access verification
        assert data["level1"]["level2"]["level3"]["arrays"] == [[1, 2], [3, 4]]
        assert data["metadata"]["config"]["retry"]["max"] == 3
        assert len(data["metadata"]["features"]) == 3