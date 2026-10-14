"""Catalog size invariants on the rollback-isolated PostgreSQL test database."""

import psycopg
import pytest
from psycopg.rows import TupleRow

from src.database.db_counts import PROJECT_SCHEMAS
from src.database.db_sizes import schema_totals, table_sizes

pytestmark = pytest.mark.integration


def test_live_sizes_reconcile_and_exclude_nonproject_tables(
    test_db: psycopg.Connection[TupleRow],
) -> None:
    test_db.execute("CREATE TEMP TABLE coverage_size_fixture (value text)")
    test_db.execute("INSERT INTO coverage_size_fixture VALUES ('fixture')")
    rows = table_sizes(test_db)
    assert rows
    assert "raw.stream_events" in {row.name for row in rows}
    assert all(row.name.split(".")[0] in PROJECT_SCHEMAS for row in rows)
    assert not any("coverage_size_fixture" in row.name for row in rows)
    assert rows == sorted(rows, key=lambda row: (-row.total_bytes, row.name))
    for row in rows:
        assert min(row.heap_bytes, row.index_bytes, row.other_bytes) >= 0
        assert row.heap_bytes + row.index_bytes + row.other_bytes == row.total_bytes
    assert sum(schema_totals(rows).values()) == sum(row.total_bytes for row in rows)
