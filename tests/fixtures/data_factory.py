"""
Centralized data factory to eliminate INSERT statement duplication across test files.
Reduces 72+ inline INSERT statements to reusable factory methods.
"""

import json
import random
import string
from datetime import datetime, date, time
from decimal import Decimal
from typing import List, Dict, Any, Optional


class DataFactory:
    """Factory for generating common test data patterns"""
    
    @staticmethod
    def sample_users(count: int = 10, name_prefix: str = "User") -> List[Dict[str, Any]]:
        """
        Generate sample user data for basic user table tests.
        
        Args:
            count: Number of user records to generate
            name_prefix: Prefix for generated usernames
            
        Returns:
            List of user dictionaries
        """
        return [
            {
                "name": f"{name_prefix}{i}",
                "age": 20 + (i % 50)  # Ages 20-69
            }
            for i in range(count)
        ]
    
    @staticmethod
    def numeric_boundary_data() -> List[Dict[str, Any]]:
        """Generate data for numeric boundary testing"""
        return [
            {
                "tiny_int_col": 127,  # TINYINT max
                "small_int_col": 32767,  # SMALLINT max
                "medium_int_col": 8388607,  # MEDIUMINT max
                "int_col": 2147483647,  # INT max
                "big_int_col": 9223372036854775807,  # BIGINT max
                "decimal_col": Decimal("99999999.99"),
                "float_col": 3.14159,
                "double_col": 2.718281828459045,
                "unsigned_int_col": 4294967295,  # UNSIGNED INT max
                "unsigned_bigint_col": 18446744073709551615  # UNSIGNED BIGINT max
            },
            {
                "tiny_int_col": -128,  # TINYINT min
                "small_int_col": -32768,  # SMALLINT min
                "medium_int_col": -8388608,  # MEDIUMINT min
                "int_col": -2147483648,  # INT min
                "big_int_col": -9223372036854775808,  # BIGINT min
                "decimal_col": Decimal("-99999999.99"),
                "float_col": -3.14159,
                "double_col": -2.718281828459045,
                "unsigned_int_col": 0,  # UNSIGNED INT min
                "unsigned_bigint_col": 0  # UNSIGNED BIGINT min
            },
            {
                "tiny_int_col": 0,
                "small_int_col": 0,
                "medium_int_col": 0,
                "int_col": 0,
                "big_int_col": 0,
                "decimal_col": Decimal("0.00"),
                "float_col": 0.0,
                "double_col": 0.0,
                "unsigned_int_col": 12345,
                "unsigned_bigint_col": 123456789012345
            }
        ]
    
    @staticmethod
    def text_and_binary_data() -> List[Dict[str, Any]]:
        """Generate data for text and binary type testing"""
        long_text = "Lorem ipsum " * 1000  # Long text for testing
        binary_data = b'\x00\x01\x02\x03\xff\xfe\xfd\xfc' * 2  # 16 bytes
        
        return [
            {
                "varchar_col": "Standard varchar text",
                "char_col": "char_test",
                "text_col": "This is a text field with moderate length content.",
                "mediumtext_col": long_text,
                "longtext_col": long_text * 5,
                "binary_col": binary_data,
                "varbinary_col": b'varbinary_test_data',
                "blob_col": b'blob_test_data',
                "mediumblob_col": binary_data * 100,
                "longblob_col": binary_data * 1000
            },
            {
                "varchar_col": "Unicode test: café, naïve, résumé",
                "char_col": "unicode",
                "text_col": "Unicode text: 你好世界, здравствуй мир, مرحبا بالعالم",
                "mediumtext_col": "Medium unicode: " + "🌍🌎🌏" * 100,
                "longtext_col": "Long unicode: " + "测试数据" * 10000,
                "binary_col": b'\xe4\xb8\xad\xe6\x96\x87' + b'\x00' * 10,  # UTF-8 Chinese + padding
                "varbinary_col": b'\xc4\x85\xc4\x99\xc5\x82',  # UTF-8 Polish chars
                "blob_col": binary_data,
                "mediumblob_col": binary_data * 50,
                "longblob_col": binary_data * 500
            }
        ]
    
    @staticmethod
    def temporal_data() -> List[Dict[str, Any]]:
        """Generate data for date/time type testing"""
        return [
            {
                "date_col": date(2024, 1, 15),
                "time_col": time(14, 30, 45),
                "datetime_col": datetime(2024, 1, 15, 14, 30, 45),
                "timestamp_col": datetime(2024, 1, 15, 14, 30, 45),
                "year_col": 2024
            },
            {
                "date_col": date(1999, 12, 31),
                "time_col": time(23, 59, 59),
                "datetime_col": datetime(1999, 12, 31, 23, 59, 59),
                "timestamp_col": datetime(1999, 12, 31, 23, 59, 59),
                "year_col": 1999
            },
            {
                "date_col": date(2000, 1, 1),
                "time_col": time(0, 0, 0),
                "datetime_col": datetime(2000, 1, 1, 0, 0, 0),
                "timestamp_col": datetime(2000, 1, 1, 0, 0, 0),
                "year_col": 2000
            }
        ]
    
    @staticmethod
    def json_test_data() -> List[Dict[str, Any]]:
        """Generate data for JSON type testing"""
        return [
            {
                "json_col": json.dumps({"name": "John", "age": 30, "city": "New York"}),
                "metadata": json.dumps({
                    "tags": ["important", "review"],
                    "priority": 1,
                    "settings": {
                        "notifications": True,
                        "theme": "dark"
                    }
                }),
                "config": json.dumps({
                    "database": {
                        "host": "localhost",
                        "port": 3306,
                        "ssl": True
                    },
                    "cache": {
                        "enabled": True,
                        "ttl": 3600
                    }
                })
            },
            {
                "json_col": json.dumps([1, 2, 3, {"nested": "array"}]),
                "metadata": json.dumps({
                    "unicode": "测试数据 café naïve",
                    "special_chars": "!@#$%^&*()_+-=[]{}|;:,.<>?",
                    "null_value": None,
                    "boolean": True
                }),
                "config": json.dumps({
                    "complex": {
                        "nested": {
                            "deeply": {
                                "structure": "value"
                            }
                        }
                    },
                    "array": [1, "two", 3.14, {"four": 4}]
                })
            }
        ]
    
    @staticmethod
    def enum_and_set_data() -> List[Dict[str, Any]]:
        """Generate data for ENUM and SET type testing"""
        return [
            {
                "status": "active",
                "tags": "tag1,tag2",
                "category": "A"
            },
            {
                "status": "inactive",
                "tags": "tag2,tag3,tag4",
                "category": "B"
            },
            {
                "status": "pending",
                "tags": "tag1",
                "category": "C"
            }
        ]
    
    @staticmethod
    def multi_column_key_data() -> List[Dict[str, Any]]:
        """Generate data for multi-column primary key testing"""
        return [
            {
                "company_id": 1,
                "user_id": 1,
                "name": "John Doe",
                "created_at": datetime(2024, 1, 1, 10, 0, 0)
            },
            {
                "company_id": 1,
                "user_id": 2,
                "name": "Jane Smith",
                "created_at": datetime(2024, 1, 1, 11, 0, 0)
            },
            {
                "company_id": 2,
                "user_id": 1,
                "name": "Bob Wilson",
                "created_at": datetime(2024, 1, 1, 12, 0, 0)
            }
        ]
    
    @staticmethod
    def performance_test_data(count: int = 1000, complexity: str = "medium") -> List[Dict[str, Any]]:
        """
        Generate data for performance testing.
        
        Args:
            count: Number of records to generate
            complexity: "simple", "medium", or "complex"
        """
        def random_string(length: int) -> str:
            return ''.join(random.choices(string.ascii_letters + string.digits, k=length))
        
        def generate_record(i: int) -> Dict[str, Any]:
            base_record = {
                "created_at": datetime.now()
            }
            
            if complexity == "simple":
                base_record.update({
                    "name": f"PerformanceTest{i}",
                    "value": Decimal(f"{random.randint(1, 10000)}.{random.randint(10, 99)}"),
                    "status": random.choice([0, 1])
                })
            elif complexity == "medium":
                base_record.update({
                    "name": f"PerformanceTest{i}",
                    "description": f"Description for performance test record {i}",
                    "value": Decimal(f"{random.randint(1, 100000)}.{random.randint(1000, 9999)}"),
                    "metadata": json.dumps({
                        "test_id": i,
                        "random_value": random.randint(1, 1000),
                        "category": random.choice(["A", "B", "C"])
                    }),
                    "status": random.choice(["active", "inactive", "pending"]),
                    "updated_at": datetime.now()
                })
            else:  # complex
                base_record.update({
                    "name": f"ComplexPerformanceTest{i}",
                    "short_name": f"CPT{i}",
                    "description": f"Complex description for performance test record {i} with more detailed information.",
                    "long_description": f"Very long description for performance test record {i}. " + random_string(500),
                    "value": Decimal(f"{random.randint(1, 1000000)}.{random.randint(100000, 999999)}"),
                    "float_value": random.uniform(1.0, 1000.0),
                    "double_value": random.uniform(1.0, 1000000.0),
                    "metadata": json.dumps({
                        "test_id": i,
                        "complex_data": {
                            "nested": {
                                "value": random.randint(1, 1000),
                                "array": [random.randint(1, 100) for _ in range(5)]
                            }
                        }
                    }),
                    "config": json.dumps({
                        "settings": {
                            "option1": random.choice([True, False]),
                            "option2": random.randint(1, 10),
                            "option3": random_string(20)
                        }
                    }),
                    "tags": random.choice(["urgent", "important", "review", "archived"]),
                    "status": random.choice(["draft", "active", "inactive", "pending", "archived"]),
                    "created_by": random.randint(1, 100),
                    "updated_by": random.randint(1, 100),
                    "updated_at": datetime.now()
                })
            
            return base_record
        
        return [generate_record(i) for i in range(count)]
    
    @staticmethod
    def replication_test_data() -> List[Dict[str, Any]]:
        """Generate standard data for replication testing"""
        return [
            {
                "name": "Ivan",
                "age": 42,
                "config": json.dumps({"role": "admin", "permissions": ["read", "write"]})
            },
            {
                "name": "Peter",
                "age": 33,
                "config": json.dumps({"role": "user", "permissions": ["read"]})
            },
            {
                "name": "Maria",
                "age": 28,
                "config": json.dumps({"role": "editor", "permissions": ["read", "write", "edit"]})
            }
        ]