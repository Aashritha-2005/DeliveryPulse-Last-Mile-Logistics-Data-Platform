"""Edge cases through the PostgreSQL load path (handmade batch folders, rolled back).

Empty files, malformed rows, a duplicate batch, a missing required column and an extra column,
each loaded with ``load_dataset`` exactly as `make load` does.
"""

import json
from pathlib import Path
from typing import LiteralString

import psycopg
import pytest
from psycopg import sql
from psycopg.rows import TupleRow

from src.database.load_postgres import LOADED, SKIPPED, LoadError, load_dataset
from src.transformations.pipeline import TransformConfig
from src.transformations.records import BatchSchemaError
from tests.integration.test_load import BUSINESS_TABLES, all_tables, digests

pytestmark = pytest.mark.integration

Connection = psycopg.Connection[TupleRow]
CONFIG = TransformConfig()
DATASET = "edge-dataset"
BATCH_ID = "edge-batch-1"
DRIVERS_HEADER = "driver_id,city,vehicle_type,joined_date,rating"
ORDERS_HEADER = (
    "order_id,customer_id,driver_id,order_created_ts,pickup_ts,delivery_ts,promised_ts,"
    "distance_km,status,city,fare_amount,source_updated_at"
)
SHIFTS_HEADER = "event_id,driver_id,event_type,event_ts"
DRIVER_ROW = "DRV-00001,Delhi,motorbike,2025-01-01,4.50"
ORDER_ROW = (
    "ORD-0000001,CUST-000001,DRV-00001,2026-09-10T10:00:00Z,2026-09-10T10:15:00Z,"
    "2026-09-10T10:40:00Z,2026-09-10T10:45:00Z,4.200,delivered,Delhi,67.00,2026-09-10T10:40:00Z"
)
EVENT: dict[str, object] = {
    "event_id": "EVT-000000001",
    "order_id": "ORD-0000001",
    "driver_id": "DRV-00001",
    "event_type": "gps_ping",
    "event_ts": "2026-09-10T10:20:00Z",
    "lat": 28.6,
    "lon": 77.2,
}
SHIFT_ROW = "SHF-0000001,DRV-00001,online,2026-09-10T09:00:00Z"


def write_batch(
    root: Path,
    batch_id: str = BATCH_ID,
    drivers: tuple[str, ...] = (DRIVERS_HEADER, DRIVER_ROW),
    orders: tuple[str, ...] = (ORDERS_HEADER, ORDER_ROW),
    events: tuple[str, ...] = (json.dumps(EVENT),),
    shifts: tuple[str, ...] = (SHIFTS_HEADER, SHIFT_ROW),
) -> Path:
    """Write ``root/<dataset>/batch-1`` with the given file lines; return the dataset folder."""
    folder = root / DATASET / "batch-1"
    folder.mkdir(parents=True, exist_ok=True)
    metadata = {
        "batch_id": batch_id,
        "batch_sequence": 1,
        "dataset": DATASET,
        "window_start": "2026-09-10T00:00:00Z",
        "window_end_exclusive": "2026-09-11T00:00:00Z",
    }
    (folder / "batch.json").write_text(json.dumps(metadata), encoding="utf-8")
    for name, lines in (
        ("drivers.csv", drivers),
        ("orders.csv", orders),
        ("delivery_events.jsonl", events),
        ("shift_events.csv", shifts),
    ):
        text = "".join(line + "\n" for line in lines)
        (folder / name).write_text(text, encoding="utf-8")
    return folder.parent


def audit_rows(conn: Connection) -> list[tuple[object, ...]]:
    rows = conn.execute(
        "SELECT source, attempt, status, rows_input, rows_accepted, rows_rejected, "
        "failure_reason FROM audit.pipeline_runs ORDER BY attempt, source"
    ).fetchall()
    return [tuple(row) for row in rows]


def scalar(conn: Connection, query: LiteralString) -> object:
    row = conn.execute(query).fetchone()
    assert row is not None
    return row[0]


# --------------------------------------------------------------------- empty files ---


def test_empty_files_load_as_an_empty_successful_batch(test_db: Connection, tmp_path: Path) -> None:
    dataset = write_batch(
        tmp_path,
        drivers=(DRIVERS_HEADER,),
        orders=(ORDERS_HEADER,),
        events=(),
        shifts=(SHIFTS_HEADER,),
    )
    [load] = load_dataset(test_db, dataset, CONFIG)
    assert load.status == LOADED
    assert load.quality is not None and load.quality.blocking_failures == []
    runs = audit_rows(test_db)
    assert len(runs) == 4
    assert {(row[2], row[3]) for row in runs} == {("succeeded", 0)}
    assert scalar(test_db, "SELECT count(*) FROM raw.orders_raw") == 0


# ------------------------------------------------------------------ malformed rows ---


def test_malformed_rows_are_rejected_kept_in_raw_and_quarantined(
    test_db: Connection, tmp_path: Path
) -> None:
    dataset = write_batch(
        tmp_path,
        orders=(ORDERS_HEADER, ORDER_ROW, "ORD-0000002,CUST-000002"),  # wrong field count
        events=(json.dumps(EVENT), "{not json"),
    )
    [load] = load_dataset(test_db, dataset, CONFIG)
    assert load.status == LOADED
    dispositions = test_db.execute(
        "SELECT source, row_number, disposition, reason FROM audit.record_dispositions "
        "WHERE disposition <> 'accepted' ORDER BY source"
    ).fetchall()
    assert [tuple(row) for row in dispositions] == [
        ("delivery_events", 2, "rejected", "malformed_record"),
        ("orders", 2, "rejected", "malformed_record"),
    ]
    raw_order = test_db.execute(
        "SELECT order_id, extra_columns ->> '_raw_line' FROM raw.orders_raw "
        "WHERE source_row_number = 2"
    ).fetchone()
    assert raw_order is not None and raw_order[0] is None
    assert raw_order[1] == '["ORD-0000002", "CUST-000002"]'  # the row kept whole
    raw_event = scalar(
        test_db,
        "SELECT extra_columns ->> '_raw_line' FROM raw.events_raw WHERE source_row_number = 2",
    )
    assert raw_event == "{not json"
    rejected = test_db.execute(
        "SELECT source, reason FROM staging.rejected_records ORDER BY source"
    ).fetchall()
    assert [tuple(row) for row in rejected] == [
        ("delivery_events", "malformed_record"),
        ("orders", "malformed_record"),
    ]
    orders_run = [row for row in audit_rows(test_db) if row[0] == "orders"]
    assert orders_run == [("orders", 1, "succeeded", 2, 1, 1, None)]


# ----------------------------------------------------------------- duplicate batch ---


def test_duplicate_batch_is_a_no_op_and_a_changed_batch_id_is_refused(
    test_db: Connection, tmp_path: Path
) -> None:
    dataset = write_batch(tmp_path / "first")
    load_dataset(test_db, dataset, CONFIG)
    loaded = digests(test_db, all_tables(test_db))

    [again] = load_dataset(test_db, dataset, CONFIG)
    assert again.status == SKIPPED
    assert digests(test_db, all_tables(test_db)) == loaded

    changed = write_batch(tmp_path / "second", batch_id="edge-batch-1-changed")
    with pytest.raises(LoadError, match="already loaded with a different batch_id"):
        load_dataset(test_db, changed, CONFIG)
    assert digests(test_db, all_tables(test_db)) == loaded  # nothing written, not even audit


# ---------------------------------------------------------- missing required column ---


def test_missing_required_column_fails_the_batch_and_a_fixed_retry_succeeds(
    test_db: Connection, tmp_path: Path
) -> None:
    header = ORDERS_HEADER.replace(",city", "")
    row = ORDER_ROW.replace(",Delhi,", ",", 1)
    dataset = write_batch(tmp_path, orders=(header, row))
    with pytest.raises(BatchSchemaError, match="city"):
        load_dataset(test_db, dataset, CONFIG)
    runs = audit_rows(test_db)
    assert len(runs) == 4
    for _, attempt, status, rows_input, accepted, _, reason in runs:
        assert (attempt, status, rows_input, accepted) == (1, "failed", 0, 0)
        assert isinstance(reason, str) and "BatchSchemaError" in reason and "city" in reason
    for schema, table in (
        ("raw", "orders_raw"),
        ("raw", "drivers_raw"),
        ("analytics", "orders"),
        ("analytics", "drivers"),
    ):
        query = sql.SQL("SELECT count(*) FROM {}").format(sql.Identifier(schema, table))
        count = test_db.execute(query).fetchone()
        assert count is not None and count[0] == 0, table

    write_batch(tmp_path)  # the source is corrected
    [retry] = load_dataset(test_db, dataset, CONFIG)
    assert (retry.status, retry.attempt) == (LOADED, 2)
    assert scalar(test_db, "SELECT count(*) FROM analytics.orders") == 1


# -------------------------------------------------------------------- extra column ---


def test_extra_column_is_kept_in_raw_logged_and_ignored_downstream(
    test_db: Connection, tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    with test_db.transaction(force_rollback=True):
        load_dataset(test_db, write_batch(tmp_path / "plain"), CONFIG)
        plain = digests(test_db, BUSINESS_TABLES)

    extra_event = json.dumps({**EVENT, "speed": 12})
    dataset = write_batch(
        tmp_path / "extra",
        orders=(ORDERS_HEADER + ",promo_code", ORDER_ROW + ",SPRING"),
        events=(extra_event,),
    )
    with caplog.at_level("WARNING", logger="src.transformations.records"):
        load_dataset(test_db, dataset, CONFIG)
    assert "orders: ignoring extra column(s): promo_code" in caplog.text
    assert "delivery_events: ignoring extra field(s): speed" in caplog.text
    assert scalar(test_db, "SELECT extra_columns FROM raw.orders_raw") == {"promo_code": "SPRING"}
    assert scalar(test_db, "SELECT extra_columns FROM raw.events_raw") == {"speed": 12}
    assert digests(test_db, BUSINESS_TABLES) == plain  # analytics and state unchanged
