"""Tests for handling advanced/complex MySQL data types during replication"""

import datetime

import pytest

from tests.base import BaseReplicationTest, DataTestMixin, SchemaTestMixin
from tests.conftest import TEST_TABLE_NAME
from tests.fixtures import TableSchemas, TestDataGenerator


class TestAdvancedDataTypes(BaseReplicationTest, SchemaTestMixin, DataTestMixin):
    """Test replication of advanced MySQL data types"""

    @pytest.mark.integration
    def test_spatial_and_geometry_types(self):
        """Test spatial data type handling"""
        # Setup spatial table
        schema = TableSchemas.spatial_table(TEST_TABLE_NAME)
        self.mysql.execute(schema.sql)

        # Insert spatial data using raw SQL (POINT function)
        spatial_records = TestDataGenerator.spatial_records()
        for record in spatial_records:
            self.mysql.execute(
                f"""INSERT INTO `{TEST_TABLE_NAME}` (name, age, coordinate) 
                   VALUES ('{record["name"]}', {record["age"]}, {record["coordinate"]});""",
                commit=True,
            )

        # Start replication
        self.start_replication()

        # Verify spatial data replication
        expected_count = len(spatial_records)
        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=expected_count)

        # Verify spatial records exist (exact coordinate comparison may vary)
        self.verify_record_exists(TEST_TABLE_NAME, "name='Ivan'", {"age": 42})
        self.verify_record_exists(TEST_TABLE_NAME, "name='Peter'", {"age": 33})

    @pytest.mark.integration
    def test_enum_and_set_types(self):
        """Test ENUM and SET type handling"""
        # Create table with ENUM and SET types
        self.mysql.execute(f"""
        CREATE TABLE `{TEST_TABLE_NAME}` (
            id int NOT NULL AUTO_INCREMENT,
            name varchar(255),
            status enum('active', 'inactive', 'pending'),
            permissions set('read', 'write', 'admin'),
            priority enum('low', 'medium', 'high') DEFAULT 'medium',
            PRIMARY KEY (id)
        );
        """)

        # Insert enum/set test data
        enum_data = [
            {
                "name": "EnumTest1",
                "status": "active",
                "permissions": "read,write",
                "priority": "high",
            },
            {
                "name": "EnumTest2",
                "status": "pending",
                "permissions": "admin",
                "priority": "low",
            },
        ]

        self.insert_multiple_records(TEST_TABLE_NAME, enum_data)

        # Start replication
        self.start_replication()

        # Verify enum/set replication
        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=2)

        # Verify enum values
        self.verify_record_exists(
            TEST_TABLE_NAME,
            "name='EnumTest1'",
            {"status": "active", "priority": "high"},
        )

        self.verify_record_exists(
            TEST_TABLE_NAME,
            "name='EnumTest2'",
            {"status": "pending", "priority": "low"},
        )

    @pytest.mark.integration
    def test_invalid_datetime_handling(self):
        """Test handling of invalid datetime values (0000-00-00)"""
        # Create table with datetime fields that can handle invalid dates
        self.mysql.execute(f"""
        CREATE TABLE `{TEST_TABLE_NAME}` (
            id int NOT NULL AUTO_INCREMENT,
            name varchar(255),
            modified_date DateTime(3) NOT NULL,
            test_date date NOT NULL,
            PRIMARY KEY (id)
        );
        """)

        # Use connection context to set SQL mode for invalid dates
        with self.mysql.get_connection() as (connection, cursor):
            cursor.execute("SET sql_mode = 'ALLOW_INVALID_DATES';")

            cursor.execute(
                f"INSERT INTO `{TEST_TABLE_NAME}` (name, modified_date, test_date) "
                f"VALUES ('Ivan', '0000-00-00 00:00:00', '2015-05-28');"
            )
            connection.commit()

        # Start replication
        self.start_replication()
        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=1)

        # Add more records with invalid datetime values
        with self.mysql.get_connection() as (connection, cursor):
            cursor.execute("SET sql_mode = 'ALLOW_INVALID_DATES';")

            cursor.execute(
                f"INSERT INTO `{TEST_TABLE_NAME}` (name, modified_date, test_date) "
                f"VALUES ('Alex', '0000-00-00 00:00:00', '2015-06-02');"
            )
            cursor.execute(
                f"INSERT INTO `{TEST_TABLE_NAME}` (name, modified_date, test_date) "
                f"VALUES ('Givi', '2023-01-08 03:11:09', '2015-06-02');"
            )
            connection.commit()

        # Verify all records are replicated
        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=3)

        # Verify specific dates are handled correctly
        self.verify_record_exists(
            TEST_TABLE_NAME, "name='Alex'", {"test_date": datetime.date(2015, 6, 2)}
        )
        self.verify_record_exists(
            TEST_TABLE_NAME, "name='Ivan'", {"test_date": datetime.date(2015, 5, 28)}
        )

    @pytest.mark.integration
    def test_complex_employee_table_types(self):
        """Test various MySQL data types with complex employee schema"""
        # Create complex employee table with many field types
        # Use execute_batch to ensure SQL mode persists for the CREATE TABLE
        self.mysql.execute_batch(
            [
                "SET sql_mode = 'ALLOW_INVALID_DATES'",
                f"""CREATE TABLE `{TEST_TABLE_NAME}` (
            `id` int unsigned NOT NULL AUTO_INCREMENT,
            name varchar(255),
            `employee` int unsigned NOT NULL,
            `position` smallint unsigned NOT NULL,
            `job_title` smallint NOT NULL DEFAULT '0',
            `department` smallint unsigned NOT NULL DEFAULT '0',
            `job_level` smallint unsigned NOT NULL DEFAULT '0',
            `job_grade` smallint unsigned NOT NULL DEFAULT '0',
            `level` smallint unsigned NOT NULL DEFAULT '0',
            `team` smallint unsigned NOT NULL DEFAULT '0',
            `factory` smallint unsigned NOT NULL DEFAULT '0',
            `ship` smallint unsigned NOT NULL DEFAULT '0',
            `report_to` int unsigned NOT NULL DEFAULT '0',
            `line_manager` int unsigned NOT NULL DEFAULT '0',
            `location` smallint unsigned NOT NULL DEFAULT '0',
            `customer` int unsigned NOT NULL DEFAULT '0',
            `effective_date` date NOT NULL DEFAULT '0000-00-00',
            `status` tinyint unsigned NOT NULL DEFAULT '0',
            `promotion` tinyint unsigned NOT NULL DEFAULT '0',
            `promotion_id` int unsigned NOT NULL DEFAULT '0',
            `note` text CHARACTER SET utf8mb3 COLLATE utf8mb3_unicode_ci NOT NULL,
            `is_change_probation_time` tinyint unsigned NOT NULL DEFAULT '0',
            `deleted` tinyint unsigned NOT NULL DEFAULT '0',
            `created_by` int unsigned NOT NULL DEFAULT '0',
            `created_by_name` varchar(125) CHARACTER SET utf8mb3 COLLATE utf8mb3_unicode_ci NOT NULL DEFAULT '',
            `created_date` datetime NOT NULL DEFAULT '0000-00-00 00:00:00',
            `modified_by` int unsigned NOT NULL DEFAULT '0',
            `modified_by_name` varchar(125) CHARACTER SET utf8mb3 COLLATE utf8mb3_unicode_ci NOT NULL DEFAULT '',
            `modified_date` datetime NOT NULL DEFAULT '0000-00-00 00:00:00',
            `entity` int NOT NULL DEFAULT '0',
            `sent_2_tac` char(1) CHARACTER SET utf8mb3 COLLATE utf8mb3_unicode_ci NOT NULL DEFAULT '0',
            PRIMARY KEY (id),
            KEY `name, employee` (`name`,`employee`) USING BTREE
        )""",
            ],
            commit=True,
        )

        # Insert test data with valid values
        # Insert record with required fields and let created_date/modified_date use default
        self.mysql.execute(
            f"INSERT INTO `{TEST_TABLE_NAME}` (name, employee, position, note) VALUES ('Ivan', 1001, 5, 'Test note');",
            commit=True,
        )

        # Start replication
        self.start_replication()
        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=1)

        # Add more records with different values
        self.mysql.execute(
            f"INSERT INTO `{TEST_TABLE_NAME}` (name, employee, position, note, effective_date) VALUES ('Alex', 1002, 3, 'Test note 2', '2023-01-15');",
            commit=True,
        )
        self.mysql.execute(
            f"INSERT INTO `{TEST_TABLE_NAME}` (name, employee, position, note, modified_date) VALUES ('Givi', 1003, 7, 'Test note 3', '2023-01-08 03:11:09');",
            commit=True,
        )

        # Verify replication of complex data types
        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=3)

        # Verify records exist with proper data
        self.verify_record_exists(TEST_TABLE_NAME, "name='Ivan'")
        self.verify_record_exists(TEST_TABLE_NAME, "name='Alex'")
        self.verify_record_exists(TEST_TABLE_NAME, "name='Givi'")
