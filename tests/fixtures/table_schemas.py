"""Predefined table schemas for testing"""

from dataclasses import dataclass


@dataclass
class TableSchema:
    """Represents a table schema with SQL and metadata"""

    name: str
    sql: str
    columns: list
    primary_key: str = "id"


class TableSchemas:
    """Collection of predefined table schemas for testing"""

    @staticmethod
    def basic_user_table(table_name="test_table"):
        """Basic table with id, name, age"""
        return TableSchema(
            name=table_name,
            sql=f"""
            CREATE TABLE `{table_name}` (
                id int NOT NULL AUTO_INCREMENT,
                name varchar(255) COMMENT 'Dân tộc, ví dụ: Kinh',
                age int COMMENT 'CMND Cũ', 
                PRIMARY KEY (id)
            );
            """,
            columns=["id", "name", "age"],
        )

    @staticmethod
    def basic_user_with_blobs(table_name="test_table"):
        """Basic table with text and blob fields"""
        return TableSchema(
            name=table_name,
            sql=f"""
            CREATE TABLE `{table_name}` (
                id int NOT NULL AUTO_INCREMENT,
                name varchar(255) COMMENT 'Dân tộc, ví dụ: Kinh',
                age int COMMENT 'CMND Cũ',
                field1 text,
                field2 blob,
                PRIMARY KEY (id)
            );
            """,
            columns=["id", "name", "age", "field1", "field2"],
        )

    @staticmethod
    def complex_employee_table(table_name="test_table"):
        """Complex employee table with many fields and types"""
        return TableSchema(
            name=table_name,
            sql=f"""
            CREATE TABLE `{table_name}` (
                id int unsigned NOT NULL AUTO_INCREMENT,
                name varchar(255) DEFAULT NULL,
                employee int unsigned NOT NULL,
                position smallint unsigned NOT NULL,
                job_title smallint NOT NULL DEFAULT '0',
                department smallint unsigned NOT NULL DEFAULT '0',
                job_level smallint unsigned NOT NULL DEFAULT '0',
                job_grade smallint unsigned NOT NULL DEFAULT '0',
                level smallint unsigned NOT NULL DEFAULT '0',
                team smallint unsigned NOT NULL DEFAULT '0',
                factory smallint unsigned NOT NULL DEFAULT '0',
                ship smallint unsigned NOT NULL DEFAULT '0',
                report_to int unsigned NOT NULL DEFAULT '0',
                line_manager int unsigned NOT NULL DEFAULT '0',
                location smallint unsigned NOT NULL DEFAULT '0',
                customer int unsigned NOT NULL DEFAULT '0',
                effective_date date NOT NULL DEFAULT '0000-00-00',
                status tinyint unsigned NOT NULL DEFAULT '0',
                promotion tinyint unsigned NOT NULL DEFAULT '0',
                promotion_id int unsigned NOT NULL DEFAULT '0',
                note text CHARACTER SET utf8mb3 COLLATE utf8mb3_unicode_ci NOT NULL,
                is_change_probation_time tinyint unsigned NOT NULL DEFAULT '0',
                deleted tinyint unsigned NOT NULL DEFAULT '0',
                created_by int unsigned NOT NULL DEFAULT '0',
                created_by_name varchar(125) CHARACTER SET utf8mb3 COLLATE utf8mb3_unicode_ci NOT NULL DEFAULT '',
                created_date datetime NOT NULL DEFAULT '0000-00-00 00:00:00',
                modified_by int unsigned NOT NULL DEFAULT '0',
                modified_by_name varchar(125) CHARACTER SET utf8mb3 COLLATE utf8mb3_unicode_ci NOT NULL DEFAULT '',
                modified_date datetime NOT NULL DEFAULT '0000-00-00 00:00:00',
                entity int NOT NULL DEFAULT '0',
                sent_2_tac char(1) CHARACTER SET utf8mb3 COLLATE utf8mb3_unicode_ci NOT NULL DEFAULT '0',
                PRIMARY KEY (id)
            );
            """,
            columns=[
                "id",
                "name",
                "employee",
                "position",
                "job_title",
                "department",
                "job_level",
                "job_grade",
                "level",
                "team",
                "factory",
                "ship",
                "report_to",
                "line_manager",
                "location",
                "customer",
                "effective_date",
                "status",
                "promotion",
                "promotion_id",
                "note",
                "is_change_probation_time",
                "deleted",
                "created_by",
                "created_by_name",
                "created_date",
                "modified_by",
                "modified_by_name",
                "modified_date",
                "entity",
                "sent_2_tac",
            ],
        )

    @staticmethod
    def datetime_test_table(table_name="test_table"):
        """Table for testing datetime handling"""
        return TableSchema(
            name=table_name,
            sql=f"""
            CREATE TABLE `{table_name}` (
                id int NOT NULL AUTO_INCREMENT,
                name varchar(255),
                modified_date datetime(3) NOT NULL,
                test_date date NOT NULL,
                PRIMARY KEY (id)
            );
            """,
            columns=["id", "name", "modified_date", "test_date"],
        )

    @staticmethod
    def spatial_table(table_name="test_table"):
        """Table with spatial data types"""
        return TableSchema(
            name=table_name,
            sql=f"""
            CREATE TABLE `{table_name}` (
                id int NOT NULL AUTO_INCREMENT,
                name varchar(255),
                age int,
                rate decimal(10,4),
                coordinate point NOT NULL,
                KEY `IDX_age` (`age`),
                FULLTEXT KEY `IDX_name` (`name`),
                PRIMARY KEY (id),
                SPATIAL KEY `coordinate` (`coordinate`)
            ) ENGINE=InnoDB AUTO_INCREMENT=2478808 DEFAULT CHARSET=latin1;
            """,
            columns=["id", "name", "age", "rate", "coordinate"],
        )

    @staticmethod
    def reserved_keyword_table(table_name="group"):
        """Table with reserved keyword name"""
        return TableSchema(
            name=table_name,
            sql=f"""
            CREATE TABLE `{table_name}` (
                id int NOT NULL AUTO_INCREMENT,
                name varchar(255) NOT NULL,
                age int,
                rate decimal(10,4),
                PRIMARY KEY (id)
            );
            """,
            columns=["id", "name", "age", "rate"],
        )
