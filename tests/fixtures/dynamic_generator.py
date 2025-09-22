"""Dynamic table and data generation for performance testing"""

import random
import string
from decimal import Decimal


class DynamicTableGenerator:
    """Generate dynamic table schemas and data for testing"""

    @staticmethod
    def generate_table_schema(table_name: str, complexity_level: str = "medium") -> str:
        """Generate dynamic table schema based on complexity level"""
        base_columns = [
            "id int NOT NULL AUTO_INCREMENT",
            "created_at timestamp DEFAULT CURRENT_TIMESTAMP"
        ]

        complexity_configs = {
            "simple": {
                "additional_columns": 3,
                "types": ["varchar(100)", "int", "decimal(10,2)"]
            },
            "medium": {
                "additional_columns": 8,
                "types": ["varchar(255)", "int", "bigint", "decimal(12,4)", "text", "json", "boolean", "datetime"]
            },
            "complex": {
                "additional_columns": 15,
                "types": ["varchar(500)", "tinyint", "smallint", "int", "bigint", "decimal(15,6)", 
                         "float", "double", "text", "longtext", "blob", "json", "boolean", 
                         "date", "datetime", "timestamp"]
            }
        }

        config = complexity_configs[complexity_level]
        columns = base_columns.copy()

        for i in range(config["additional_columns"]):
            col_type = random.choice(config["types"])
            col_name = f"field_{i+1}"
            
            # Add constraints for some columns
            constraint = ""
            if col_type.startswith("varchar") and random.random() < 0.3:
                constraint = " UNIQUE" if random.random() < 0.5 else " NOT NULL"
            
            columns.append(f"{col_name} {col_type}{constraint}")

        columns.append("PRIMARY KEY (id)")
        
        return f"CREATE TABLE `{table_name}` ({', '.join(columns)});"

    @staticmethod
    def generate_test_data(schema: str, num_records: int = 1000) -> list:
        """Generate test data matching the schema"""
        # Parse schema to understand column types (simplified)
        data_generators = {
            "varchar": lambda size: ''.join(random.choices(string.ascii_letters + string.digits, k=min(int(size), 50))),
            "int": lambda: random.randint(-2147483648, 2147483647),
            "bigint": lambda: random.randint(-9223372036854775808, 9223372036854775807),
            "decimal": lambda p, s: Decimal(f"{random.uniform(-999999, 999999):.{min(int(s), 4)}f}"),
            "text": lambda: ' '.join(random.choices(string.ascii_letters.split(), k=random.randint(10, 50))),
            "json": lambda: f'{{"key_{random.randint(1,100)}": "value_{random.randint(1,1000)}", "number": {random.randint(1,100)}}}',
            "boolean": lambda: random.choice([True, False]),
            "datetime": lambda: f"2023-{random.randint(1,12):02d}-{random.randint(1,28):02d} {random.randint(0,23):02d}:{random.randint(0,59):02d}:{random.randint(0,59):02d}"
        }

        records = []
        for _ in range(num_records):
            record = {}
            # Generate data based on schema analysis (simplified implementation)
            # In a real implementation, you'd parse the CREATE TABLE statement
            for i in range(8):  # Medium complexity default
                field_name = f"field_{i+1}"
                data_type = random.choice(["varchar", "int", "decimal", "text", "json", "boolean", "datetime"])
                
                try:
                    if data_type == "varchar":
                        record[field_name] = data_generators["varchar"](100)
                    elif data_type == "decimal":
                        record[field_name] = data_generators["decimal"](12, 4)
                    else:
                        record[field_name] = data_generators[data_type]()
                except:
                    record[field_name] = f"default_value_{i}"
            
            records.append(record)
        
        return records