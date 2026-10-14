"""The quality gate's statistics function makes a transaction's uncommitted rows visible to the
planner (it replaces the earlier ``SET LOCAL enable_nestloop = off``).

Reproduces the load situation in the rolled-back test database: a table whose statistics
already exist, then new rows of another batch inserted in the same, still open transaction.
"""

import psycopg
import pytest
from psycopg.rows import TupleRow

pytestmark = pytest.mark.integration

Connection = psycopg.Connection[TupleRow]
ROWS = 2000

INSERT_BATCH = """
    INSERT INTO staging.rejected_records (batch_id, source, record_key, record_payload, reason)
    SELECT %s, 'orders', 'planner-' || %s || '-' || n, '{}'::jsonb, 'planner_test'
    FROM generate_series(1, %s) AS n
"""


def estimated_rows(conn: Connection, batch_id: str) -> int:
    """The planner's row estimate for one batch of staging.rejected_records."""
    row = conn.execute(
        "EXPLAIN (FORMAT JSON) SELECT * FROM staging.rejected_records WHERE batch_id = %s",
        (batch_id,),
    ).fetchone()
    assert row is not None
    return int(row[0][0]["Plan"]["Plan Rows"])


def test_analyze_function_counts_uncommitted_rows(test_db: Connection) -> None:
    test_db.execute(INSERT_BATCH, ("planner-a", "a", ROWS))
    test_db.execute("SELECT staging.analyze_quality_tables()")  # statistics know batch a only
    test_db.execute(INSERT_BATCH, ("planner-b", "b", ROWS))

    before = estimated_rows(test_db, "planner-b")
    test_db.execute("SELECT staging.analyze_quality_tables()")
    after = estimated_rows(test_db, "planner-b")

    assert before < ROWS // 10  # the planner did not know the new batch
    assert ROWS * 0.8 <= after <= ROWS * 1.2  # now it does, although nothing is committed
