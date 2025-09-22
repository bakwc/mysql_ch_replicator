"""
Centralized schema factory to eliminate CREATE TABLE duplication across test files.
Reduces 102+ inline CREATE TABLE statements to reusable factory methods.
"""

from typing import List, Dict, Optional


class SchemaFactory:
    """Factory for generating common test table schemas"""
    
    # Common column templates to reduce duplication across 55 CREATE TABLE statements
    COMMON_COLUMNS = {
        "id_auto": "id int NOT NULL AUTO_INCREMENT",
        "name_varchar": "name varchar(255)",  # Used 49 times
        "age_int": "age int",
        "email_varchar": "email varchar(255)",
        "status_enum": "status enum('active','inactive','pending') DEFAULT 'active'",
        "created_timestamp": "created_at timestamp DEFAULT CURRENT_TIMESTAMP",
        "updated_timestamp": "updated_at timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
        "data_json": "data json",
        "primary_key_id": "PRIMARY KEY (id)"  # Used 69 times
    }
    
    @classmethod
    def _build_table_sql(cls, table_name, columns, engine="InnoDB", charset="utf8mb4"):
        """Build CREATE TABLE SQL from column templates"""
        column_defs = []
        for col in columns:
            if col in cls.COMMON_COLUMNS:
                column_defs.append(cls.COMMON_COLUMNS[col])
            else:
                column_defs.append(col)
        
        return f"CREATE TABLE `{table_name}` (\n    " + ",\n    ".join(column_defs) + f"\n) ENGINE={engine} DEFAULT CHARSET={charset};"
    
    @staticmethod
    def basic_user_table(table_name: str, additional_columns: Optional[List[str]] = None) -> str:
        """
        Standard user table schema used across multiple tests.
        
        Args:
            table_name: Name of the table to create
            additional_columns: Optional list of additional column definitions
            
        Returns:
            CREATE TABLE SQL statement
        """
        columns = [
            "id int NOT NULL AUTO_INCREMENT",
            "name varchar(255)",
            "age int",
            "PRIMARY KEY (id)"
        ]
        
        if additional_columns:
            # Insert additional columns before PRIMARY KEY
            columns = columns[:-1] + additional_columns + [columns[-1]]
        
        columns_sql = ",\n            ".join(columns)
        
        return f"""CREATE TABLE `{table_name}` (
            {columns_sql}
        )"""
    
    @staticmethod
    def data_type_test_table(table_name: str, data_types: List[str]) -> str:
        """
        Dynamic schema for data type testing.
        
        Args:
            table_name: Name of the table to create
            data_types: List of MySQL data types to test
            
        Returns:
            CREATE TABLE SQL statement with specified data types
        """
        columns = ["id int NOT NULL AUTO_INCREMENT"]
        
        for i, data_type in enumerate(data_types, 1):
            columns.append(f"field_{i} {data_type}")
        
        columns.append("PRIMARY KEY (id)")
        columns_sql = ",\n            ".join(columns)
        
        return f"""CREATE TABLE `{table_name}` (
            {columns_sql}
        )"""
    
    @staticmethod
    def numeric_types_table(table_name: str) -> str:
        """Schema for comprehensive numeric type testing"""
        return f"""CREATE TABLE `{table_name}` (
            id int NOT NULL AUTO_INCREMENT,
            tiny_int_col tinyint,
            small_int_col smallint,
            medium_int_col mediumint,
            int_col int,
            big_int_col bigint,
            decimal_col decimal(10,2),
            float_col float,
            double_col double,
            unsigned_int_col int unsigned,
            unsigned_bigint_col bigint unsigned,
            PRIMARY KEY (id)
        )"""
    
    @staticmethod
    def text_types_table(table_name: str) -> str:
        """Schema for text and binary type testing"""
        return f"""CREATE TABLE `{table_name}` (
            id int NOT NULL AUTO_INCREMENT,
            varchar_col varchar(255),
            char_col char(10),
            text_col text,
            mediumtext_col mediumtext,
            longtext_col longtext,
            binary_col binary(16),
            varbinary_col varbinary(255),
            blob_col blob,
            mediumblob_col mediumblob,
            longblob_col longblob,
            PRIMARY KEY (id)
        )"""
    
    @staticmethod
    def temporal_types_table(table_name: str) -> str:
        """Schema for date/time type testing"""
        return f"""CREATE TABLE `{table_name}` (
            id int NOT NULL AUTO_INCREMENT,
            date_col date,
            time_col time,
            datetime_col datetime,
            timestamp_col timestamp DEFAULT CURRENT_TIMESTAMP,
            year_col year,
            PRIMARY KEY (id)
        )"""
    
    @staticmethod
    def json_types_table(table_name: str) -> str:
        """Schema for JSON type testing"""
        return f"""CREATE TABLE `{table_name}` (
            id int NOT NULL AUTO_INCREMENT,
            json_col json,
            metadata json,
            config json,
            PRIMARY KEY (id)
        )"""
    
    @staticmethod
    def enum_and_set_table(table_name: str) -> str:
        """Schema for ENUM and SET type testing"""
        return f"""CREATE TABLE `{table_name}` (
            id int NOT NULL AUTO_INCREMENT,
            status enum('active', 'inactive', 'pending'),
            tags set('tag1', 'tag2', 'tag3', 'tag4'),
            category enum('A', 'B', 'C') DEFAULT 'A',
            PRIMARY KEY (id)
        )"""
    
    @staticmethod
    def multi_column_primary_key_table(table_name: str) -> str:
        """Schema with multi-column primary key for complex testing"""
        return f"""CREATE TABLE `{table_name}` (
            company_id int NOT NULL,
            user_id int NOT NULL,
            name varchar(255),
            created_at timestamp DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (company_id, user_id)
        )"""
    
    @staticmethod
    def performance_test_table(table_name: str, complexity: str = "medium") -> str:
        """
        Schema optimized for performance testing.
        
        Args:
            table_name: Name of the table to create
            complexity: "simple", "medium", or "complex"
        """
        base_columns = [
            "id int NOT NULL AUTO_INCREMENT",
            "created_at timestamp DEFAULT CURRENT_TIMESTAMP"
        ]
        
        complexity_configs = {
            "simple": [
                "name varchar(100)",
                "value decimal(10,2)",
                "status tinyint DEFAULT 1"
            ],
            "medium": [
                "name varchar(255)",
                "description text",
                "value decimal(12,4)",
                "metadata json",
                "status enum('active', 'inactive', 'pending') DEFAULT 'active'",
                "updated_at datetime"
            ],
            "complex": [
                "name varchar(500)",
                "short_name varchar(50)",
                "description text",
                "long_description longtext",
                "value decimal(15,6)",
                "float_value float",
                "double_value double",
                "metadata json",
                "config json",
                "tags set('urgent', 'important', 'review', 'archived')",
                "status enum('draft', 'active', 'inactive', 'pending', 'archived') DEFAULT 'draft'",
                "created_by int",
                "updated_by int",
                "updated_at datetime"
            ]
        }
        
        additional_columns = complexity_configs.get(complexity, complexity_configs["medium"])
        all_columns = base_columns + additional_columns + ["PRIMARY KEY (id)"]
        columns_sql = ",\n            ".join(all_columns)
        
        return f"""CREATE TABLE `{table_name}` (
            {columns_sql}
        )"""
    
    @staticmethod
    def replication_test_table(table_name: str, with_comments: bool = False) -> str:
        """Schema commonly used for replication testing"""
        comment_sql = " COMMENT 'Test replication table'" if with_comments else ""
        name_comment = " COMMENT 'User name field'" if with_comments else ""
        age_comment = " COMMENT 'User age field'" if with_comments else ""
        
        return f"""CREATE TABLE `{table_name}` (
            id int NOT NULL AUTO_INCREMENT,
            name varchar(255){name_comment},
            age int{age_comment},
            config json,
            PRIMARY KEY (id)
        ){comment_sql}"""
    
    # ===================== ENHANCED DRY TEMPLATES =====================
    # The following methods eliminate massive table creation duplication
    
    @classmethod  
    def standard_user_table(cls, table_name):
        """Most common table pattern - eliminates the 49 name varchar(255) duplicates"""
        return cls._build_table_sql(table_name, [
            "id_auto", "name_varchar", "age_int", "primary_key_id"
        ])
    
    @classmethod
    def json_test_table(cls, table_name):
        """Standard JSON testing table - consolidates JSON test patterns"""
        return cls._build_table_sql(table_name, [
            "id_auto", "name_varchar", "data_json", "primary_key_id"  
        ])
    
    @classmethod
    def user_profile_table(cls, table_name):
        """Standard user profile table - combines user + email patterns"""
        return cls._build_table_sql(table_name, [
            "id_auto", "name_varchar", "email_varchar", "age_int", "primary_key_id"
        ])
    
    @classmethod
    def auditable_table(cls, table_name, additional_columns=None):
        """Table with audit trail - combines timestamp patterns"""
        columns = ["id_auto", "name_varchar", "created_timestamp", "updated_timestamp", "primary_key_id"]
        if additional_columns:
            columns = columns[:-1] + additional_columns + [columns[-1]]  # Insert before PRIMARY KEY
        return cls._build_table_sql(table_name, columns)
    
    @classmethod
    def enum_status_table(cls, table_name):
        """Table with status enum - consolidates ENUM testing patterns"""
        return cls._build_table_sql(table_name, [
            "id_auto", "name_varchar", "status_enum", "primary_key_id"
        ])