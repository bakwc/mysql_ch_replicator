"""Advanced dynamic data generation for comprehensive replication testing"""

import random
import string
import re
from decimal import Decimal
from datetime import datetime, date, timedelta
from typing import List, Dict, Any, Optional, Tuple


class AdvancedDynamicGenerator:
    """Enhanced dynamic table and data generation with controlled randomness"""
    
    def __init__(self, seed: Optional[int] = None):
        """Initialize with optional seed for reproducible tests"""
        if seed is not None:
            random.seed(seed)
        self.seed = seed
    
    # MySQL Data Type Definitions with Boundaries
    DATA_TYPES = {
        # Numeric Types
        "tinyint": {"range": (-128, 127), "unsigned_range": (0, 255)},
        "smallint": {"range": (-32768, 32767), "unsigned_range": (0, 65535)},
        "mediumint": {"range": (-8388608, 8388607), "unsigned_range": (0, 16777215)},
        "int": {"range": (-2147483648, 2147483647), "unsigned_range": (0, 4294967295)},
        "bigint": {"range": (-9223372036854775808, 9223372036854775807), "unsigned_range": (0, 18446744073709551615)},
        
        # String Types
        "varchar": {"max_length": 65535},
        "char": {"max_length": 255},
        "text": {"max_length": 65535},
        "longtext": {"max_length": 4294967295},
        
        # Decimal Types
        "decimal": {"max_precision": 65, "max_scale": 30},
        "float": {"range": (-3.402823466e+38, 3.402823466e+38)},
        "double": {"range": (-1.7976931348623157e+308, 1.7976931348623157e+308)},
        
        # Date/Time Types
        "date": {"range": (date(1000, 1, 1), date(9999, 12, 31))},
        "datetime": {"range": (datetime(1000, 1, 1, 0, 0, 0), datetime(9999, 12, 31, 23, 59, 59))},
        "timestamp": {"range": (datetime(1970, 1, 1, 0, 0, 1), datetime(2038, 1, 19, 3, 14, 7))},
        
        # Special Types
        "json": {"max_depth": 5, "max_keys": 10},
        "enum": {"max_values": 65535},
        "set": {"max_values": 64}
    }
    
    def generate_dynamic_schema(self, 
                               table_name: str, 
                               data_type_focus: Optional[List[str]] = None,
                               column_count: Tuple[int, int] = (5, 15),
                               include_constraints: bool = True) -> str:
        """
        Generate dynamic table schema with specific data type focus
        
        Args:
            table_name: Name of the table
            data_type_focus: Specific data types to focus on (e.g., ['json', 'decimal', 'varchar'])
            column_count: Min and max number of columns (min, max)
            include_constraints: Whether to include random constraints
            
        Returns:
            CREATE TABLE SQL statement
        """
        columns = ["id int NOT NULL AUTO_INCREMENT"]
        
        # Determine column count
        num_columns = random.randint(*column_count)
        
        # Available data types
        available_types = data_type_focus if data_type_focus else list(self.DATA_TYPES.keys())
        
        for i in range(num_columns):
            col_name = f"col_{i+1}"
            data_type = random.choice(available_types)
            
            # Generate specific column definition
            col_def = self._generate_column_definition(col_name, data_type, include_constraints)
            columns.append(col_def)
        
        # Add primary key
        columns.append("PRIMARY KEY (id)")
        
        return f"CREATE TABLE `{table_name}` (\n    {',\n    '.join(columns)}\n);"
    
    def _generate_column_definition(self, col_name: str, data_type: str, include_constraints: bool) -> str:
        """Generate specific column definition with random parameters"""
        
        if data_type == "varchar":
            length = random.choice([50, 100, 255, 500, 1000])
            col_def = f"{col_name} varchar({length})"
            
        elif data_type == "char":
            length = random.randint(1, 255)
            col_def = f"{col_name} char({length})"
            
        elif data_type == "decimal":
            precision = random.randint(1, 65)
            scale = random.randint(0, min(precision, 30))
            col_def = f"{col_name} decimal({precision},{scale})"
            
        elif data_type in ["tinyint", "smallint", "mediumint", "int", "bigint"]:
            unsigned = random.choice([True, False])
            col_def = f"{col_name} {data_type}"
            if unsigned:
                col_def += " unsigned"
                
        elif data_type == "enum":
            # Generate random enum values
            enum_count = random.randint(2, 8)
            enum_values = [f"'value_{i}'" for i in range(enum_count)]
            col_def = f"{col_name} enum({','.join(enum_values)})"
            
        elif data_type == "set":
            # Generate random set values
            set_count = random.randint(2, 6)
            set_values = [f"'option_{i}'" for i in range(set_count)]
            col_def = f"{col_name} set({','.join(set_values)})"
            
        else:
            # Simple data types
            col_def = f"{col_name} {data_type}"
        
        # Add random constraints (avoid NOT NULL without DEFAULT to prevent data generation issues)
        if include_constraints and random.random() < 0.3:
            if data_type in ["varchar", "char", "text"]:
                col_def += random.choice([" DEFAULT ''", " UNIQUE"])
            elif data_type in ["int", "bigint", "decimal"]:
                col_def += random.choice([" DEFAULT 0", " UNSIGNED"])
        
        return col_def
    
    def generate_dynamic_data(self, schema_sql: str, record_count: int = 100) -> List[Dict[str, Any]]:
        """
        Generate test data that matches the dynamic schema
        
        Args:
            schema_sql: CREATE TABLE statement to parse
            record_count: Number of records to generate
            
        Returns:
            List of record dictionaries
        """
        # Parse the schema to extract column information
        columns_info = self._parse_schema(schema_sql)
        
        records = []
        for _ in range(record_count):
            record = {}
            
            for col_name, col_type, col_constraints in columns_info:
                if col_name == "id":  # Skip auto-increment id
                    continue
                    
                # Generate value based on column type
                value = self._generate_value_for_type(col_type, col_constraints)
                record[col_name] = value
            
            records.append(record)
        
        return records
    
    def _parse_schema(self, schema_sql: str) -> List[Tuple[str, str, str]]:
        """Parse CREATE TABLE statement to extract column information"""
        columns_info = []
        
        # Extract columns between parentheses
        match = re.search(r'CREATE TABLE.*?\\((.*?)\\)', schema_sql, re.DOTALL | re.IGNORECASE)
        if not match:
            return columns_info
        
        columns_text = match.group(1)
        
        # Split by commas and clean up
        column_lines = [line.strip() for line in columns_text.split(',')]
        
        for line in column_lines:
            if line.startswith('PRIMARY KEY') or line.startswith('KEY') or line.startswith('INDEX'):
                continue
                
            # Extract column name and type
            parts = line.split()
            if len(parts) >= 2:
                col_name = parts[0].strip('`')
                col_type = parts[1].lower()
                col_constraints = ' '.join(parts[2:]) if len(parts) > 2 else ''
                
                columns_info.append((col_name, col_type, col_constraints))
        
        return columns_info
    
    def _generate_value_for_type(self, col_type: str, constraints: str) -> Any:
        """Generate appropriate value for given column type and constraints"""
        
        # Handle NULL constraints
        if "not null" not in constraints.lower() and random.random() < 0.1:
            return None
        
        # Extract type information
        if col_type.startswith("varchar"):
            length_match = re.search(r'varchar\\((\\d+)\\)', col_type)
            max_length = int(length_match.group(1)) if length_match else 255
            length = random.randint(1, min(max_length, 50))
            return ''.join(random.choices(string.ascii_letters + string.digits + ' ', k=length))
        
        elif col_type.startswith("char"):
            length_match = re.search(r'char\\((\\d+)\\)', col_type)
            max_length = int(length_match.group(1)) if length_match else 1
            return ''.join(random.choices(string.ascii_letters, k=max_length))
        
        elif col_type.startswith("decimal"):
            precision_match = re.search(r'decimal\\((\\d+),(\\d+)\\)', col_type)
            if precision_match:
                precision, scale = int(precision_match.group(1)), int(precision_match.group(2))
                max_val = 10**(precision - scale) - 1
                return Decimal(f"{random.uniform(-max_val, max_val):.{scale}f}")
            return Decimal(f"{random.uniform(-999999, 999999):.2f}")
        
        elif col_type in ["tinyint", "smallint", "mediumint", "int", "bigint"]:
            type_info = self.DATA_TYPES.get(col_type, {"range": (-1000, 1000)})
            if "unsigned" in constraints.lower():
                range_info = type_info.get("unsigned_range", (0, 1000))
            else:
                range_info = type_info.get("range", (-1000, 1000))
            return random.randint(*range_info)
        
        elif col_type == "float":
            return round(random.uniform(-1000000.0, 1000000.0), 6)
        
        elif col_type == "double":
            return round(random.uniform(-1000000000.0, 1000000000.0), 10)
        
        elif col_type in ["text", "longtext"]:
            length = random.randint(10, 1000)
            return ' '.join([
                ''.join(random.choices(string.ascii_letters, k=random.randint(3, 10)))
                for _ in range(length // 10)
            ])
        
        elif col_type == "json":
            return self._generate_random_json()
        
        elif col_type.startswith("enum"):
            enum_match = re.search(r"enum\\((.*?)\\)", col_type)
            if enum_match:
                values = [v.strip().strip("'\"") for v in enum_match.group(1).split(',')]
                return random.choice(values)
            return "value_0"
        
        elif col_type.startswith("set"):
            set_match = re.search(r"set\\((.*?)\\)", col_type)
            if set_match:
                values = [v.strip().strip("'\"") for v in set_match.group(1).split(',')]
                # Select random subset of set values
                selected_count = random.randint(1, len(values))
                selected_values = random.sample(values, selected_count)
                return ','.join(selected_values)
            return "option_0"
        
        elif col_type == "date":
            start_date = date(2020, 1, 1)
            end_date = date(2024, 12, 31)
            days_between = (end_date - start_date).days
            random_date = start_date + timedelta(days=random.randint(0, days_between))
            return random_date
        
        elif col_type in ["datetime", "timestamp"]:
            start_datetime = datetime(2020, 1, 1, 0, 0, 0)
            end_datetime = datetime(2024, 12, 31, 23, 59, 59)
            seconds_between = int((end_datetime - start_datetime).total_seconds())
            random_datetime = start_datetime + timedelta(seconds=random.randint(0, seconds_between))
            return random_datetime
        
        elif col_type == "boolean":
            return random.choice([True, False])
        
        # Default fallback
        return f"dynamic_value_{random.randint(1, 1000)}"
    
    def _generate_random_json(self, max_depth: int = 3) -> str:
        """Generate random JSON structure"""
        
        def generate_json_value(depth=0):
            if depth >= max_depth:
                return random.choice([
                    random.randint(1, 1000),
                    f"string_{random.randint(1, 100)}",
                    random.choice([True, False]),
                    None
                ])
            
            choice = random.randint(1, 4)
            if choice == 1:  # Object
                obj = {}
                for i in range(random.randint(1, 5)):
                    key = f"key_{random.randint(1, 100)}"
                    obj[key] = generate_json_value(depth + 1)
                return obj
            elif choice == 2:  # Array
                return [generate_json_value(depth + 1) for _ in range(random.randint(1, 5))]
            elif choice == 3:  # String
                return f"value_{random.randint(1, 1000)}"
            else:  # Number
                return random.randint(1, 1000)
        
        import json
        return json.dumps(generate_json_value())
    
    def create_boundary_test_scenario(self, data_types: List[str], table_name: str = None) -> Tuple[str, List[Dict]]:
        """
        Create a test scenario focusing on boundary values for specific data types
        
        Args:
            data_types: List of data types to test boundary values for
            table_name: Name of the table to create (if None, generates random name)
            
        Returns:
            Tuple of (schema_sql, test_data)
        """
        if table_name is None:
            table_name = f"boundary_test_{random.randint(1000, 9999)}"
        
        columns = ["id int NOT NULL AUTO_INCREMENT"]
        test_records = []
        
        for i, data_type in enumerate(data_types):
            col_name = f"boundary_{data_type}_{i+1}"
            
            if data_type in self.DATA_TYPES:
                type_info = self.DATA_TYPES[data_type]
                
                # Create column definition
                if data_type == "varchar":
                    columns.append(f"{col_name} varchar(255)")
                    # Boundary values: empty, max length, special chars
                    test_records.extend([
                        {col_name: ""},
                        {col_name: "A" * 255},
                        {col_name: "Special chars: !@#$%^&*()"},
                        {col_name: None}
                    ])
                    
                elif data_type in ["int", "bigint"]:
                    columns.append(f"{col_name} {data_type}")
                    range_info = type_info["range"]
                    test_records.extend([
                        {col_name: range_info[0]},  # Min value
                        {col_name: range_info[1]},  # Max value
                        {col_name: 0},              # Zero
                        {col_name: None}            # NULL
                    ])
        
        columns.append("PRIMARY KEY (id)")
        schema_sql = f"CREATE TABLE `{table_name}` (\n    {',\n    '.join(columns)}\n);"
        
        # Combine individual field records into complete records
        combined_records = []
        if test_records:
            for i in range(max(len(test_records) // len(data_types), 4)):
                record = {}
                for j, data_type in enumerate(data_types):
                    col_name = f"boundary_{data_type}_{j+1}"
                    # Cycle through the test values
                    record_index = (i * len(data_types) + j) % len(test_records)
                    if col_name in test_records[record_index]:
                        record[col_name] = test_records[record_index][col_name]
                combined_records.append(record)
        
        return schema_sql, combined_records