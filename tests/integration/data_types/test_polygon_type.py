"""Integration test for POLYGON geometry type replication"""

import pytest

from tests.base import BaseReplicationTest, DataTestMixin, SchemaTestMixin
from tests.conftest import TEST_DB_NAME, TEST_TABLE_NAME


class TestPolygonType(BaseReplicationTest, SchemaTestMixin, DataTestMixin):
    """Verify POLYGON columns replicate and materialize as arrays of points."""

    @pytest.mark.integration
    def test_polygon_replication(self):
        # Create table with polygon columns
        self.mysql.execute(
            f"""
            CREATE TABLE `{TEST_TABLE_NAME}` (
                id INT NOT NULL AUTO_INCREMENT,
                name VARCHAR(50) NOT NULL,
                area POLYGON NOT NULL,
                nullable_area POLYGON,
                PRIMARY KEY (id)
            );
            """
        )

        # Insert polygons using WKT
        self.mysql.execute(
            f"""
            INSERT INTO `{TEST_TABLE_NAME}` (name, area, nullable_area) VALUES 
            ('Square', ST_GeomFromText('POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))'), ST_GeomFromText('POLYGON((0 0, 0 2, 2 2, 2 0, 0 0))')),
            ('Triangle', ST_GeomFromText('POLYGON((0 0, 1 0, 0.5 1, 0 0))'), NULL),
            ('Complex', ST_GeomFromText('POLYGON((0 0, 0 3, 3 3, 3 0, 0 0))'), ST_GeomFromText('POLYGON((1 1, 1 2, 2 2, 2 1, 1 1))'));
            """,
            commit=True,
        )

        # Start replication
        self.start_replication()

        # Verify initial rows
        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=3)
        results = self.ch.select(TEST_TABLE_NAME)
        assert results[0]["name"] == "Square"
        assert len(results[0]["area"]) == 5
        assert len(results[0]["nullable_area"]) == 5

        assert results[1]["name"] == "Triangle"
        assert len(results[1]["area"]) == 4
        assert results[1]["nullable_area"] == []

        assert results[2]["name"] == "Complex"
        assert len(results[2]["area"]) == 5
        assert len(results[2]["nullable_area"]) == 5

        # Realtime replication: add more shapes
        self.mysql.execute(
            f"""
            INSERT INTO `{TEST_TABLE_NAME}` (name, area, nullable_area) VALUES 
            ('Pentagon', ST_GeomFromText('POLYGON((0 0, 1 0, 1.5 1, 0.5 1.5, 0 0))'), ST_GeomFromText('POLYGON((0.2 0.2, 0.8 0.2, 1 0.8, 0.5 1, 0.2 0.2))')),
            ('Hexagon', ST_GeomFromText('POLYGON((0 0, 1 0, 1.5 0.5, 1 1, 0.5 1, 0 0))'), NULL);
            """,
            commit=True,
        )

        self.wait_for_table_sync(TEST_TABLE_NAME, expected_count=5)

        pent = self.ch.select(TEST_TABLE_NAME, where="name='Pentagon'")[0]
        hexa = self.ch.select(TEST_TABLE_NAME, where="name='Hexagon'")[0]

        assert len(pent["area"]) == 5 and len(pent["nullable_area"]) == 5
        assert len(hexa["area"]) == 6 and hexa["nullable_area"] == []
