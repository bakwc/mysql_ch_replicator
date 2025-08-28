"""
Example showing how to use the refactored test structure

This demonstrates the key benefits:
1. Reusable base classes and mixins
2. Predefined table schemas
3. Test data generators
4. Assertion helpers
5. Clean, focused test organization
"""

import pytest

from tests.base import BaseReplicationTest, DataTestMixin, SchemaTestMixin
from tests.conftest import TEST_TABLE_NAME
from tests.fixtures import AssertionHelpers, TableSchemas, TestDataGenerator


class ExampleTest(BaseReplicationTest, SchemaTestMixin, DataTestMixin):
    """Example test class demonstrating the refactored structure"""

    @pytest.mark.integration
    def test_simple_replication_example(self):
        """Simple example using the new structure"""

        # 1. Create table using predefined schema
        schema = TableSchemas.basic_user_table(TEST_TABLE_NAME)
        self.mysql.execute(schema.sql)

        # 2. Insert test data using generator
        test_data = TestDataGenerator.basic_users()[:3]
        self.insert_multiple_records(TEST_TABLE_NAME, test_data)

        # 3. Start replication (handled by base class)
        self.start_replication()

        # 4. Verify replication using helpers
        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=3)

        # 5. Verify specific data using built-in methods
        for record in test_data:
            self.verify_record_exists(
                TEST_TABLE_NAME, f"name='{record['name']}'", {"age": record["age"]}
            )

    @pytest.mark.integration
    def test_schema_changes_example(self):
        """Example of testing schema changes"""

        # Start with basic table
        schema = TableSchemas.basic_user_table(TEST_TABLE_NAME)
        self.mysql.execute(schema.sql)

        initial_data = TestDataGenerator.basic_users()[:2]
        self.insert_multiple_records(TEST_TABLE_NAME, initial_data)

        # Start replication
        self.start_replication()
        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=2)

        # Use schema mixin methods for DDL operations
        self.add_column(TEST_TABLE_NAME, "email varchar(255)")
        self.add_column(TEST_TABLE_NAME, "salary decimal(10,2)", "AFTER age")

        # Insert data with new columns using data mixin
        self.insert_basic_record(
            TEST_TABLE_NAME, "NewUser", 28, email="test@example.com", salary=50000.00
        )

        # Verify schema changes replicated
        self.wait_for_data_sync(
            TEST_TABLE_NAME, "name='NewUser'", "test@example.com", "email"
        )

    @pytest.mark.integration
    def test_complex_data_types_example(self):
        """Example testing complex data types"""

        # Use predefined complex schema
        schema = TableSchemas.datetime_test_table(TEST_TABLE_NAME)
        self.mysql.execute(schema.sql)

        # Use specialized test data generator
        datetime_data = TestDataGenerator.datetime_records()
        self.insert_multiple_records(TEST_TABLE_NAME, datetime_data)

        # Start replication
        self.start_replication()

        # Verify datetime handling
        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=len(datetime_data))

        # Use assertion helpers for complex validations
        assertions = AssertionHelpers(self.mysql, self.ch)
        assertions.assert_field_is_null(TEST_TABLE_NAME, "name='Ivan'", "modified_date")
        assertions.assert_field_not_null(
            TEST_TABLE_NAME, "name='Givi'", "modified_date"
        )

    @pytest.mark.integration
    def test_error_handling_example(self):
        """Example of testing error conditions and edge cases"""

        schema = TableSchemas.basic_user_table(TEST_TABLE_NAME)
        self.mysql.execute(schema.sql)

        # Insert initial data
        self.insert_basic_record(TEST_TABLE_NAME, "TestUser", 30)

        # Start replication
        self.start_replication()
        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=1)

        # Test edge cases
        try:
            # Try to insert invalid data
            self.mysql.execute(
                f"INSERT INTO `{TEST_TABLE_NAME}` (name, age) VALUES ('', -1);",
                commit=True,
            )

            # Verify system handles edge cases gracefully
            self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=2)

        except Exception as e:
            # Log the error but continue testing
            print(f"Expected error handled: {e}")

        # Verify original data is still intact
        self.verify_record_exists(TEST_TABLE_NAME, "name='TestUser'", {"age": 30})

    @pytest.mark.integration
    def test_performance_example(self):
        """Example of performance testing with bulk data"""

        schema = TableSchemas.basic_user_table(TEST_TABLE_NAME)
        self.mysql.execute(schema.sql)

        # Generate bulk test data
        bulk_data = []
        for i in range(100):
            bulk_data.append({"name": f"BulkUser_{i:03d}", "age": 20 + (i % 50)})

        # Insert in batches and measure
        import time

        start_time = time.time()

        self.insert_multiple_records(TEST_TABLE_NAME, bulk_data)

        # Start replication
        self.start_replication()

        # Verify bulk replication
        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=100)

        replication_time = time.time() - start_time
        print(f"Replicated 100 records in {replication_time:.2f} seconds")

        # Verify data integrity with sampling
        sample_indices = [0, 25, 50, 75, 99]
        for i in sample_indices:
            expected_record = bulk_data[i]
            self.verify_record_exists(
                TEST_TABLE_NAME,
                f"name='{expected_record['name']}'",
                {"age": expected_record["age"]},
            )


class CustomSchemaExampleTest(BaseReplicationTest, SchemaTestMixin, DataTestMixin):
    """Example showing how to extend with custom schemas and data"""

    def create_custom_table(self, table_name):
        """Custom table creation method"""
        self.mysql.execute(f"""
        CREATE TABLE `{table_name}` (
            id int NOT NULL AUTO_INCREMENT,
            product_name varchar(255) NOT NULL,
            category_id int,
            price decimal(12,4),
            inventory_count int DEFAULT 0,
            created_at timestamp DEFAULT CURRENT_TIMESTAMP,
            updated_at timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            metadata json,
            PRIMARY KEY (id),
            INDEX idx_category (category_id),
            INDEX idx_price (price)
        );
        """)

    def generate_custom_product_data(self, count=5):
        """Custom data generator for products"""
        import json

        products = []
        categories = ["Electronics", "Books", "Clothing", "Home", "Sports"]

        for i in range(count):
            products.append(
                {
                    "product_name": f"Product_{i:03d}",
                    "category_id": (i % 5) + 1,
                    "price": round(10.0 + (i * 2.5), 2),
                    "inventory_count": 50 + (i * 10),
                    "metadata": json.dumps(
                        {
                            "tags": [categories[i % 5], f"tag_{i}"],
                            "features": {"weight": i + 1, "color": "blue"},
                        }
                    ),
                }
            )
        return products

    @pytest.mark.integration
    def test_custom_schema_example(self):
        """Example using custom schema and data"""

        # Use custom table creation
        self.create_custom_table(TEST_TABLE_NAME)

        # Generate and insert custom data
        product_data = self.generate_custom_product_data(10)
        self.insert_multiple_records(TEST_TABLE_NAME, product_data)

        # Start replication
        self.start_replication()

        # Verify replication
        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=10)

        # Test custom validations
        self.verify_record_exists(
            TEST_TABLE_NAME,
            "product_name='Product_005'",
            {"category_id": 1, "price": 22.5, "inventory_count": 100},
        )

        # Verify JSON metadata handling
        records = self.ch.select(TEST_TABLE_NAME, where="product_name='Product_000'")
        assert len(records) > 0
        # JSON comparison would depend on how ClickHouse handles JSON
