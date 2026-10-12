"""Integration tests of the PostgreSQL load (test database, ETL role only).

Every test runs inside an outer transaction that is rolled back (the ``test_db`` fixture), so
the load's transactions A, B and C become savepoints and nothing is left behind.
"""

from pathlib import Path
from typing import LiteralString

import psycopg
import pytest
from psycopg import sql
from psycopg.rows import TupleRow

from src.contract import SOURCE_ORDERS
from src.database import load_postgres
from src.database.db_counts import table_counts
from src.database.load_postgres import (
    LOADED,
    SKIPPED,
    SOURCES,
    load_batch_folder,
    load_dataset,
    read_batch_identity,
    write_analytics,
)
from src.database.postgres_state import PostgresState
from src.groundtruth.manifest import MANIFEST_FILE_NAME, load_manifest
from src.groundtruth.reconcile import compare_with_manifest
from src.groundtruth.reconcile_db import load_db_dispositions
from src.transformations.pipeline import BatchResult, TransformConfig, process_batch
from src.transformations.records import batch_folders, load_batch
from src.transformations.rules import validate_driver, validate_order
from src.transformations.state import BatchSequenceError, LocalState
from tests.unit.transform_helpers import ASSIGNED_ORDER, DELIVERED_ORDER, DRIVER

pytestmark = pytest.mark.integration

Connection = psycopg.Connection[TupleRow]
CONFIG = TransformConfig()
STATE_TABLES = (
    ("staging", "record_state"),
    ("staging", "gps_reference"),
    ("staging", "pending_events"),
)
BUSINESS_TABLES = (
    ("analytics", "drivers"),
    ("analytics", "orders"),
    ("analytics", "delivery_events"),
    ("analytics", "driver_shift_events"),
    ("analytics", "daily_delivery_metrics"),
    ("analytics", "driver_performance"),
    ("analytics", "dim_date"),
    ("analytics", "dim_city"),
    *STATE_TABLES,
)
RAW_TABLES = ("drivers_raw", "orders_raw", "events_raw", "shift_events_raw")


class InjectedError(RuntimeError):
    """Raised on purpose in the middle of a batch transaction."""


def counts(conn: Connection) -> dict[str, int]:
    return dict(table_counts(conn))


def digest(conn: Connection, table: tuple[str, str]) -> tuple[int, str]:
    """Row count and md5 over the table's rows (ordered by their text form)."""
    query = sql.SQL(
        "SELECT count(*), md5(coalesce(string_agg(t::text, '|' ORDER BY t::text), '')) FROM {} AS t"
    ).format(sql.Identifier(*table))
    row = conn.execute(query).fetchone()
    assert row is not None
    return int(row[0]), str(row[1])


def digests(conn: Connection, tables: tuple[tuple[str, str], ...]) -> dict[str, tuple[int, str]]:
    return {".".join(table): digest(conn, table) for table in tables}


def all_tables(conn: Connection) -> tuple[tuple[str, str], ...]:
    names = [name.split(".") for name in counts(conn)]
    return tuple((schema, table) for schema, table in names)


def rows_for_batch(conn: Connection, batch_id: str) -> dict[str, int]:
    """Rows any batch-specific table holds for ``batch_id``."""
    found: dict[str, int] = {}
    for table in RAW_TABLES:
        query = sql.SQL("SELECT count(*) FROM {} WHERE batch_id = %s").format(
            sql.Identifier("raw", table)
        )
        row = conn.execute(query, (batch_id,)).fetchone()
        found[f"raw.{table}"] = 0 if row is None else int(row[0])
    queries: tuple[tuple[str, LiteralString], ...] = (
        (
            "audit.record_dispositions",
            "SELECT count(*) FROM audit.record_dispositions WHERE batch_id = %s",
        ),
        (
            "staging.rejected_records",
            "SELECT count(*) FROM staging.rejected_records WHERE batch_id = %s",
        ),
        (
            "audit.pending_resolutions",
            "SELECT count(*) FROM audit.pending_resolutions WHERE resolved_in_batch_id = %s",
        ),
    )
    for name, statement in queries:
        row = conn.execute(statement, (batch_id,)).fetchone()
        found[name] = 0 if row is None else int(row[0])
    return found


def local_results(dataset_dir: Path) -> list[BatchResult]:
    state = LocalState()
    return [process_batch(load_batch(f), state, CONFIG) for f in batch_folders(dataset_dir)]


def disposition_keys(result: BatchResult) -> list[tuple[str, int, str, str, str]]:
    return [
        (d.source, d.row_number, d.record_key, d.disposition, d.reason) for d in result.dispositions
    ]


def resolution_keys(result: BatchResult) -> list[tuple[str, str, str, str, str]]:
    return [
        (r.record.source, r.record.record_key, r.outcome, r.disposition, r.reason)
        for r in result.resolutions
    ]


# ----------------------------------------------------------------- equivalence ---


def test_postgres_state_gives_the_same_dispositions_as_local_state(
    test_db: Connection, fixture_dataset: Path
) -> None:
    loads = load_dataset(test_db, fixture_dataset, CONFIG)
    local = local_results(fixture_dataset)
    assert [load.status for load in loads] == [LOADED] * len(local)
    for load, expected in zip(loads, local, strict=True):
        assert load.result is not None
        assert disposition_keys(load.result) == disposition_keys(expected)
        assert resolution_keys(load.result) == resolution_keys(expected)
        stored = test_db.execute(
            "SELECT source, row_number, record_key, disposition, reason "
            "FROM audit.record_dispositions WHERE batch_id = %s",
            (load.batch_id,),
        ).fetchall()
        assert sorted(tuple(row) for row in stored) == sorted(disposition_keys(expected))


def test_stored_dispositions_reconcile_with_the_manifest(
    test_db: Connection, fixture_dataset: Path
) -> None:
    load_dataset(test_db, fixture_dataset, CONFIG)
    root = fixture_dataset.parent.parent
    manifest = load_manifest(root / "ground_truth" / fixture_dataset.name / MANIFEST_FILE_NAME)
    rows, resolved = load_db_dispositions(test_db, fixture_dataset.name)
    assert compare_with_manifest(fixture_dataset, manifest, rows, resolved) == []


# ------------------------------------------------------------------ idempotency ---


def test_reloading_is_a_logged_no_op(
    test_db: Connection, fixture_dataset: Path, caplog: pytest.LogCaptureFixture
) -> None:
    load_dataset(test_db, fixture_dataset, CONFIG)
    before = digests(test_db, all_tables(test_db))
    with caplog.at_level("INFO", logger="deliverypulse.load"):
        second = load_dataset(test_db, fixture_dataset, CONFIG)
    assert [load.status for load in second] == [SKIPPED] * len(second)
    assert digests(test_db, all_tables(test_db)) == before
    assert caplog.text.count("already loaded; skipping (no-op)") == len(second)


def test_full_load_inside_a_rolled_back_transaction_leaves_nothing(
    etl_test_conn: Connection, fixture_dataset: Path
) -> None:
    assert set(counts(etl_test_conn).values()) == {0}, "the test database is not empty"
    with etl_test_conn.transaction(force_rollback=True):
        load_dataset(etl_test_conn, fixture_dataset, CONFIG)
        inside = counts(etl_test_conn)
        assert inside["analytics.orders"] > 0
        assert inside["audit.pipeline_runs"] == 4 * len(batch_folders(fixture_dataset))
    after = counts(etl_test_conn)
    assert after == dict.fromkeys(after, 0)


# ---------------------------------------------------------------------- failures ---


def test_failed_batch_rolls_back_completely_and_reruns_cleanly(
    test_db: Connection, fixture_dataset: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    folders = batch_folders(fixture_dataset)
    dataset = fixture_dataset.name
    with test_db.transaction(force_rollback=True):
        load_dataset(test_db, fixture_dataset, CONFIG)
        reference = digests(test_db, BUSINESS_TABLES)

    load_batch_folder(test_db, dataset, folders[0], CONFIG)
    before = digests(test_db, BUSINESS_TABLES)
    _, failing_batch = read_batch_identity(folders[1])

    def fail(*_: object, **__: object) -> None:
        raise InjectedError("injected after raw and staging writes")

    monkeypatch.setattr(load_postgres, "write_analytics", fail)
    with pytest.raises(InjectedError):
        load_batch_folder(test_db, dataset, folders[1], CONFIG)

    assert set(rows_for_batch(test_db, failing_batch).values()) == {0}
    assert digests(test_db, BUSINESS_TABLES) == before  # analytics and all state unchanged
    runs = test_db.execute(
        "SELECT status, attempt, rows_input, rows_failed, rows_accepted, rows_inserted, "
        "failure_reason FROM audit.pipeline_runs WHERE batch_id = %s",
        (failing_batch,),
    ).fetchall()
    assert len(runs) == len(SOURCES)
    for status, attempt, rows_input, rows_failed, accepted, inserted, reason in runs:
        assert (status, attempt, accepted, inserted) == ("failed", 1, 0, 0)
        assert rows_failed == rows_input
        assert "InjectedError" in reason
    assert sum(row[2] for row in runs) > 0

    monkeypatch.undo()
    loads = load_dataset(test_db, fixture_dataset, CONFIG)
    assert [(load.status, load.attempt) for load in loads] == [
        (SKIPPED, 0),
        (LOADED, 2),
        (LOADED, 1),
    ]
    assert digests(test_db, BUSINESS_TABLES) == reference


def test_abandoned_attempt_is_marked_failed_and_retried(
    test_db: Connection, fixture_dataset: Path
) -> None:
    folder = batch_folders(fixture_dataset)[0]
    sequence, batch_id = read_batch_identity(folder)
    for source in SOURCES:
        test_db.execute(
            "INSERT INTO audit.pipeline_runs "
            "(dataset, batch_id, batch_sequence, attempt, source, status) "
            "VALUES (%s, %s, %s, 1, %s, 'started')",
            (fixture_dataset.name, batch_id, sequence, source),
        )
    load = load_batch_folder(test_db, fixture_dataset.name, folder, CONFIG)
    assert (load.status, load.attempt) == (LOADED, 2)
    rows = test_db.execute(
        "SELECT attempt, status, failure_reason FROM audit.pipeline_runs WHERE batch_id = %s",
        (batch_id,),
    ).fetchall()
    assert sorted(tuple(row) for row in rows) == sorted(
        [(1, "failed", "abandoned")] * len(SOURCES) + [(2, "succeeded", None)] * len(SOURCES)
    )


def test_batches_load_strictly_in_sequence(test_db: Connection, fixture_dataset: Path) -> None:
    second = batch_folders(fixture_dataset)[1]
    with pytest.raises(BatchSequenceError):
        load_batch_folder(test_db, fixture_dataset.name, second, CONFIG)
    assert set(counts(test_db).values()) == {0}


# ------------------------------------------------------------- database guards ---


def test_database_stale_guard_keeps_the_newer_order(test_db: Connection) -> None:
    _, driver = validate_driver(DRIVER)
    _, newer = validate_order(DELIVERED_ORDER)
    _, older = validate_order(ASSIGNED_ORDER)
    assert driver is not None and newer is not None and older is not None
    first = PostgresState(test_db, "guard-test")
    first.new_drivers = {driver.driver_id: driver}
    first.new_orders = {newer.order_id: newer}
    with test_db.cursor() as cur:
        write_analytics(cur, first)
    stale = PostgresState(test_db, "guard-test")
    stale.new_orders = {older.order_id: older}
    with test_db.cursor() as cur:
        effects = write_analytics(cur, stale).effects
    orders = effects[SOURCE_ORDERS]
    assert (orders.inserted, orders.updated, orders.unchanged) == (0, 0, 1)
    row = test_db.execute(
        "SELECT status FROM analytics.orders WHERE order_id = %s", (newer.order_id,)
    ).fetchone()
    assert row is not None and row[0] == "delivered"


def test_audit_rows_satisfy_the_reconciliation_identity(
    test_db: Connection, fixture_dataset: Path
) -> None:
    load_dataset(test_db, fixture_dataset, CONFIG)
    rows = test_db.execute(
        "SELECT rows_input, rows_accepted, rows_rejected, rows_duplicate, rows_stale_ignored, "
        "rows_pending FROM audit.pipeline_runs WHERE status = 'succeeded'"
    ).fetchall()
    assert len(rows) == len(SOURCES) * len(batch_folders(fixture_dataset))
    for row in rows:
        assert row[0] == sum(row[1:])
    raw_total = sum(counts(test_db)[f"raw.{table}"] for table in RAW_TABLES)
    assert sum(row[0] for row in rows) == raw_total
