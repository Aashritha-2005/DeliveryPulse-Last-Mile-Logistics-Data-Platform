"""Integration tests of the data-quality checks (test database, rolled back).

* Every stored check has a handmade case that must make it fail or warn, and the set of cases
  must equal the registry, so no check can be vacuous.
* On clean pipeline output every staging, audit and analytics check passes.
* Every manifest defect type is detected at load time (the same rule as `make quality-report`).
* A blocking failure stops the batch: nothing of it is committed, metrics are unchanged, the
  failure and its evidence are recorded, and a retry matches a clean reference load.
"""

from collections.abc import Callable
from datetime import date
from pathlib import Path
from typing import Any

import psycopg
import pytest
from psycopg import sql
from psycopg.rows import TupleRow

from src.database.load_postgres import (
    LOADED,
    SKIPPED,
    SOURCES,
    load_batch_folder,
    load_dataset,
    read_batch_identity,
)
from src.groundtruth.manifest import MANIFEST_FILE_NAME, case_counts, load_manifest
from src.groundtruth.quality_coverage import coverage_problems, coverage_rows, load_time_results
from src.quality.checks import (
    CHECKS_BY_NAME,
    FAILED,
    LAYER_RAW,
    PASSED,
    QualityConfig,
    QualityGateError,
    Scope,
    result_names,
    run_check,
)
from src.quality.run import run_database_quality
from src.transformations.pipeline import TransformConfig
from src.transformations.records import batch_folders
from tests.integration.test_load import BUSINESS_TABLES, digests, rows_for_batch

pytestmark = pytest.mark.integration

Connection = psycopg.Connection[TupleRow]
CONFIG = TransformConfig()
DATASET = "quality-test"
B1 = "batch-q1"
B2 = "batch-q2"
DAY = date(2026, 9, 10)
RAW_TABLES = {
    "drivers": "drivers_raw",
    "orders": "orders_raw",
    "delivery_events": "events_raw",
    "shift_events": "shift_events_raw",
}
BASE: dict[str, dict[str, str]] = {
    "drivers": {
        "driver_id": "DRV-1",
        "city": "Delhi",
        "vehicle_type": "car",
        "joined_date": "2025-01-01",
        "rating": "4.50",
    },
    "orders": {
        "order_id": "ORD-1",
        "customer_id": "CUST-1",
        "driver_id": "DRV-1",
        "order_created_ts": "2026-09-10T05:00:00Z",
        "pickup_ts": "2026-09-10T05:10:00Z",
        "delivery_ts": "2026-09-10T05:40:00Z",
        "promised_ts": "2026-09-10T06:00:00Z",
        "distance_km": "1.000",
        "status": "delivered",
        "city": "Delhi",
        "fare_amount": "10.00",
        "source_updated_at": "2026-09-10T05:40:00Z",
    },
    "delivery_events": {
        "event_id": "EVT-1",
        "order_id": "ORD-1",
        "driver_id": "DRV-1",
        "event_type": "gps_ping",
        "event_ts": "2026-09-10T05:15:00Z",
        "lat": "28.600000",
        "lon": "77.200000",
    },
    "shift_events": {
        "event_id": "SHF-1",
        "driver_id": "DRV-1",
        "event_type": "online",
        "event_ts": "2026-09-10T05:00:00Z",
    },
}
# The handmade batches have a few rows, so failing cases judge shares from 1 row checked.
TINY_SAMPLES = QualityConfig(min_share_rows=1)
KEYS = {
    "drivers": "driver_id",
    "orders": "order_id",
    "delivery_events": "event_id",
    "shift_events": "event_id",
}


# ------------------------------------------------------------------- builders ---


def add_batch(
    conn: Connection,
    batch_id: str,
    sequence: int,
    window: tuple[str, str] = ("2026-09-10T00:00:00Z", "2026-09-11T00:00:00Z"),
    status: str = "started",
) -> None:
    """The audit rows the checks use to find a batch's sequence and window."""
    for source in SOURCES:
        conn.execute(
            "INSERT INTO audit.pipeline_runs (dataset, batch_id, batch_sequence, attempt, "
            "source, status, window_start, window_end) VALUES (%s, %s, %s, 1, %s, %s, %s, %s)",
            (DATASET, batch_id, sequence, source, status, window[0], window[1]),
        )


def raw(conn: Connection, source: str, batch_id: str, row: int, **changes: str) -> None:
    """One raw row: the valid base row of ``source`` with ``changes``."""
    values = {**BASE[source], **changes}
    columns = ["batch_id", "source_file", "source_row_number", *values]
    query = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
        sql.Identifier("raw", RAW_TABLES[source]),
        sql.SQL(", ").join(sql.Identifier(c) for c in columns),
        sql.SQL(", ").join(sql.Placeholder() for _ in columns),
    )
    conn.execute(query, [batch_id, f"{source}.file", row, *values.values()])


def driver(conn: Connection, driver_id: str = "DRV-1", city: str = "Delhi") -> None:
    conn.execute(
        "INSERT INTO analytics.drivers (driver_id, city, vehicle_type, joined_date, rating) "
        "VALUES (%s, %s, 'car', '2025-01-01', 4.5)",
        (driver_id, city),
    )


def order(conn: Connection, **changes: Any) -> None:
    """A delivered order satisfying every analytics constraint, with ``changes``."""
    values: dict[str, Any] = {
        "order_id": "ORD-1",
        "customer_id": "CUST-1",
        "driver_id": "DRV-1",
        "order_created_ts": "2026-09-10T05:00:00Z",
        "pickup_ts": "2026-09-10T05:10:00Z",
        "delivery_ts": "2026-09-10T05:40:00Z",
        "promised_ts": "2026-09-10T06:00:00Z",
        "distance_km": 1,
        "status": "delivered",
        "city": "Delhi",
        "fare_amount": 10,
        "source_updated_at": "2026-09-10T05:40:00Z",
        **changes,
    }
    query = sql.SQL("INSERT INTO analytics.orders ({}) VALUES ({})").format(
        sql.SQL(", ").join(sql.Identifier(c) for c in values),
        sql.SQL(", ").join(sql.Placeholder() for _ in values),
    )
    conn.execute(query, list(values.values()))


def ping(conn: Connection, event_id: str, ts: str, lat: float = 28.6, lon: float = 77.2) -> None:
    conn.execute(
        "INSERT INTO analytics.delivery_events "
        "(event_id, order_id, driver_id, event_type, event_ts, lat, lon) "
        "VALUES (%s, 'ORD-1', 'DRV-1', 'gps_ping', %s, %s, %s)",
        (event_id, ts, lat, lon),
    )


def disposition(
    conn: Connection, source: str, row: int, key: str, value: str, reason: str = ""
) -> None:
    conn.execute(
        "INSERT INTO audit.record_dispositions "
        "(batch_id, source, row_number, record_key, disposition, reason) "
        "VALUES (%s, %s, %s, %s, %s, %s)",
        (B1, source, row, key, value, reason),
    )


def scope(*batch_ids: str, dates: tuple[date, ...] = ()) -> Scope:
    return Scope(DATASET, batch_ids or (B1,), dates)


# --------------------------------------------------- one failing case per check ---

Case = Callable[[Connection], tuple[Scope, str]]


def duplicate_rows(source: str) -> Case:
    def setup(conn: Connection) -> tuple[Scope, str]:
        add_batch(conn, B1, 1)
        raw(conn, source, B1, 1)
        raw(conn, source, B1, 2)
        return scope(), BASE[source][KEYS[source]]

    return setup


MISSING_FIELD = {
    "drivers": "city",
    "orders": "customer_id",
    "delivery_events": "driver_id",
    "shift_events": "event_type",
}
BAD_TIMESTAMP = {
    "drivers": ("joined_date", "2025-02-30"),
    "orders": ("pickup_ts", "yesterday"),
    "delivery_events": ("event_ts", "2026-02-30T25:61:00Z"),
    "shift_events": ("event_ts", "30/09/2026 18:30"),
}


def missing_required(source: str) -> Case:
    def setup(conn: Connection) -> tuple[Scope, str]:
        add_batch(conn, B1, 1)
        raw(conn, source, B1, 1, **{MISSING_FIELD[source]: ""})
        return scope(), BASE[source][KEYS[source]]

    return setup


def unparseable_timestamp(source: str) -> Case:
    def setup(conn: Connection) -> tuple[Scope, str]:
        add_batch(conn, B1, 1)
        field, value = BAD_TIMESTAMP[source]
        raw(conn, source, B1, 1, **{field: value})
        return scope(), BASE[source][KEYS[source]]

    return setup


def one_raw_row(source: str, **changes: str) -> Case:
    def setup(conn: Connection) -> tuple[Scope, str]:
        add_batch(conn, B1, 1)
        raw(conn, source, B1, 1, **changes)
        return scope(), BASE[source][KEYS[source]]

    return setup


def two_batches(first: dict[str, str], second: dict[str, str], source: str = "orders") -> Case:
    """The same record in batch 1 and (changed by ``second``) in batch 2; checks batch 2."""

    def setup(conn: Connection) -> tuple[Scope, str]:
        add_batch(conn, B1, 1, status="succeeded")
        add_batch(conn, B2, 2, ("2026-09-11T00:00:00Z", "2026-09-12T00:00:00Z"))
        raw(conn, source, B1, 1, **first)
        raw(conn, source, B2, 1, **second)
        return scope(B2), BASE[source][KEYS[source]]

    return setup


def impossible_speed(conn: Connection) -> tuple[Scope, str]:
    add_batch(conn, B1, 1)
    raw(conn, "delivery_events", B1, 1, event_id="EVT-1", event_ts="2026-09-10T05:00:00Z")
    raw(
        conn,
        "delivery_events",
        B1,
        2,
        event_id="EVT-2",
        event_ts="2026-09-10T05:04:00Z",
        lat="29.200000",
    )  # ~67 km in 4 minutes, and back
    raw(conn, "delivery_events", B1, 3, event_id="EVT-3", event_ts="2026-09-10T05:08:00Z")
    return scope(), "EVT-2"


def duplicate_timestamp(conn: Connection) -> tuple[Scope, str]:
    add_batch(conn, B1, 1)
    raw(conn, "delivery_events", B1, 1, event_id="EVT-1")
    raw(conn, "delivery_events", B1, 2, event_id="EVT-2", lat="28.601000")
    return scope(), "EVT-2"


def out_of_file_order(conn: Connection) -> tuple[Scope, str]:
    add_batch(conn, B1, 1)
    raw(conn, "delivery_events", B1, 1, event_id="EVT-1", event_ts="2026-09-10T05:20:00Z")
    raw(conn, "delivery_events", B1, 2, event_id="EVT-2", event_ts="2026-09-10T05:10:00Z")
    return scope(), "EVT-2"


def long_gap(conn: Connection) -> tuple[Scope, str]:
    add_batch(conn, B1, 1)
    raw(conn, "delivery_events", B1, 1, event_id="EVT-1", event_ts="2026-09-10T05:00:00Z")
    raw(conn, "delivery_events", B1, 2, event_id="EVT-2", event_ts="2026-09-10T05:25:00Z")
    return scope(), "EVT-2"


def late_event(conn: Connection) -> tuple[Scope, str]:
    add_batch(conn, B1, 1)
    raw(conn, "delivery_events", B1, 1, event_ts="2026-09-09T20:00:00Z")
    return scope(), "EVT-1"


def orphan(source: str) -> Case:
    def setup(conn: Connection) -> tuple[Scope, str]:
        add_batch(conn, B1, 1)
        raw(conn, source, B1, 1, driver_id="DRV-404")  # no driver row anywhere
        return scope(), BASE[source][KEYS[source]]

    return setup


def parent_after_children(source: str) -> Case:
    child = "delivery_events" if source == "orders" else "shift_events"

    def setup(conn: Connection) -> tuple[Scope, str]:
        add_batch(conn, B1, 1, status="succeeded")
        add_batch(conn, B2, 2, ("2026-09-11T00:00:00Z", "2026-09-12T00:00:00Z"))
        raw(conn, child, B1, 1)
        raw(conn, source, B2, 1)
        return scope(B2), BASE[source][KEYS[source]]

    return setup


def unreconciled(source: str) -> Case:
    def setup(conn: Connection) -> tuple[Scope, str]:
        add_batch(conn, B1, 1)
        raw(conn, source, B1, 1)  # a raw row without a disposition
        return scope(), f"{B1}:row-1"

    return setup


def bad_disposition(conn: Connection) -> tuple[Scope, str]:
    disposition(conn, "orders", 1, "ORD-1", "weird")
    return scope(), f"{B1}:orders:row-1"


def rejected_not_quarantined(conn: Connection) -> tuple[Scope, str]:
    disposition(conn, "orders", 1, "ORD-1", "rejected", "invalid_status")
    return scope(), f"{B1}:orders"


def accepted_not_loaded(conn: Connection) -> tuple[Scope, str]:
    disposition(conn, "orders", 1, "ORD-404", "accepted")
    return scope(), "orders:ORD-404"


def stale_pending(conn: Connection) -> tuple[Scope, str]:
    driver(conn)
    order(conn)
    conn.execute(
        "INSERT INTO staging.pending_events (source, record_key, batch_id, batch_sequence, "
        "row_number, missing_reference, fields, record_payload) "
        "VALUES ('delivery_events', 'EVT-1', %s, 1, 1, 'order', %s, '{}')",
        (B1, '{"order_id": "ORD-1", "driver_id": "DRV-1"}'),
    )
    return scope(), "delivery_events:EVT-1"


def missing_driver_for_status(conn: Connection) -> tuple[Scope, str]:
    order(conn, driver_id=None, status="assigned", pickup_ts=None, delivery_ts=None)
    return scope(), "ORD-1"


def zero_fare(conn: Connection) -> tuple[Scope, str]:
    driver(conn)
    order(conn, fare_amount=0)
    return scope(), "ORD-1"


def duplicate_analytics_ping(conn: Connection) -> tuple[Scope, str]:
    driver(conn)
    order(conn)
    ping(conn, "EVT-1", "2026-09-10T05:15:00Z")
    ping(conn, "EVT-2", "2026-09-10T05:15:00Z", lat=28.601)
    return scope(), "EVT-2"


def metrics_out_of_date(conn: Connection) -> tuple[Scope, str]:
    driver(conn)
    order(conn)  # no daily_delivery_metrics row was refreshed for it
    return scope(dates=(DAY,)), "2026-09-10:Delhi"


def future_timestamp(conn: Connection) -> tuple[Scope, str]:
    add_batch(conn, B1, 1)
    driver(conn)
    order(conn, source_updated_at="2026-09-11T01:00:00Z")  # window ends 2026-09-11T00:00Z
    disposition(conn, "orders", 1, "ORD-1", "accepted")
    return scope(), "orders:ORD-1"


def stale_feed(conn: Connection) -> tuple[Scope, str]:
    add_batch(conn, B1, 1)
    driver(conn)
    order(conn)
    ping(conn, "EVT-1", "2026-09-10T05:15:00Z")  # 18.75 h before the window end
    disposition(conn, "delivery_events", 1, "EVT-1", "accepted")
    return scope(), B1


def unknown_city(conn: Connection) -> tuple[Scope, str]:
    driver(conn)
    order(conn, city="Atlantis")
    return scope(), "orders:ORD-1"


def over_utilised(conn: Connection) -> tuple[Scope, str]:
    driver(conn)
    conn.execute(
        "INSERT INTO analytics.driver_performance (driver_id, metric_date, deliveries, "
        "avg_delivery_minutes, utilisation_pct, busy_minutes, online_minutes) "
        "VALUES ('DRV-1', %s, 1, 30, 150, 90, 60)",
        (DAY,),
    )
    return scope(), "DRV-1@2026-09-10"


def fast_analytics_pings(conn: Connection) -> tuple[Scope, str]:
    driver(conn)
    order(conn)
    ping(conn, "EVT-1", "2026-09-10T05:15:00Z")
    ping(conn, "EVT-2", "2026-09-10T05:19:00Z", lat=29.2)
    return scope(), "EVT-2"


FAILING_CASES: dict[str, Case] = {
    **{f"raw_duplicate_rows.{s}": duplicate_rows(s) for s in RAW_TABLES},
    **{f"raw_missing_required.{s}": missing_required(s) for s in RAW_TABLES},
    **{f"raw_unparseable_timestamps.{s}": unparseable_timestamp(s) for s in RAW_TABLES},
    "raw_invalid_status.orders": one_raw_row("orders", status="lost"),
    "raw_status_not_canonical.orders": one_raw_row("orders", status=" Delivered"),
    "raw_numeric_out_of_range.orders": one_raw_row("orders", fare_amount="0.00"),
    "raw_driver_invalid_values.drivers": one_raw_row("drivers", vehicle_type="truck"),
    "raw_delivery_before_pickup.orders": one_raw_row("orders", delivery_ts="2026-09-10T05:05:00Z"),
    "raw_resent_unchanged.orders": two_batches({}, {}),
    "raw_stale_versions.orders": two_batches({}, {"source_updated_at": "2026-09-10T05:20:00Z"}),
    "raw_version_updates.orders": two_batches({}, {"source_updated_at": "2026-09-10T06:30:00Z"}),
    "raw_replayed_from_earlier_batch.delivery_events": two_batches({}, {}, "delivery_events"),
    "raw_replayed_from_earlier_batch.shift_events": two_batches({}, {}, "shift_events"),
    "raw_gps_missing_coordinates.delivery_events": one_raw_row("delivery_events", lat=""),
    "raw_gps_invalid_coordinates.delivery_events": one_raw_row("delivery_events", lat="95.000000"),
    "raw_gps_impossible_speed.delivery_events": impossible_speed,
    "raw_gps_duplicate_timestamps.delivery_events": duplicate_timestamp,
    "raw_events_out_of_file_order.delivery_events": out_of_file_order,
    "raw_gps_long_gaps.delivery_events": long_gap,
    "raw_late_events.delivery_events": late_event,
    "raw_orphans.delivery_events": orphan("delivery_events"),
    "raw_orphans.shift_events": orphan("shift_events"),
    "raw_parent_after_children.orders": parent_after_children("orders"),
    "raw_parent_after_children.drivers": parent_after_children("drivers"),
    **{f"reconciliation_identity.{s}": unreconciled(s) for s in RAW_TABLES},
    "disposition_values": bad_disposition,
    "rejected_quarantined": rejected_not_quarantined,
    "accepted_loaded": accepted_not_loaded,
    "pending_parent_missing": stale_pending,
    "orders_required_by_status": missing_driver_for_status,
    "orders_positive_amounts": zero_fare,
    "gps_ping_unique": duplicate_analytics_ping,
    "metrics_match_facts": metrics_out_of_date,
    "no_future_timestamps": future_timestamp,
    "freshness_lag": stale_feed,
    "known_cities": unknown_city,
    "utilisation_above_100": over_utilised,
    "gps_speed_in_analytics": fast_analytics_pings,
}


def test_every_stored_check_has_a_failing_case() -> None:
    assert set(FAILING_CASES) == set(result_names())


@pytest.mark.parametrize("name", sorted(FAILING_CASES))
def test_check_fails_on_broken_data(test_db: Connection, name: str) -> None:
    check_scope, bad_key = FAILING_CASES[name](test_db)
    check = CHECKS_BY_NAME[name.split(".")[0]]
    with test_db.cursor() as cur:
        # The handmade batches are tiny, so the share rule applies from 1 row here.
        results = {r.check_name: r for r in run_check(cur, check, check_scope, TINY_SAMPLES)}
    found = results[name]
    assert found.status != PASSED, found
    assert found.status == (FAILED if check.severity == "blocking" else "warned")
    assert bad_key in found.sample_keys, found


def test_promised_ts_after_the_window_end_is_not_a_future_timestamp(test_db: Connection) -> None:
    add_batch(test_db, B1, 1)
    driver(test_db)
    order(test_db, promised_ts="2026-09-11T03:00:00Z")  # planned time after the window end
    disposition(test_db, "orders", 1, "ORD-1", "accepted")
    with test_db.cursor() as cur:
        [found] = run_check(cur, CHECKS_BY_NAME["no_future_timestamps"], scope(), QualityConfig())
    assert (found.status, found.failing_rows, found.rows_checked) == (PASSED, 0, 1)


def test_impossible_speed_counts_a_jump_that_is_the_last_ping_once(test_db: Connection) -> None:
    add_batch(test_db, B1, 1)
    raw(test_db, "delivery_events", B1, 1, event_id="EVT-1", event_ts="2026-09-10T05:00:00Z")
    raw(
        test_db,
        "delivery_events",
        B1,
        2,
        event_id="EVT-2",
        event_ts="2026-09-10T05:04:00Z",
        lat="29.200000",
    )  # jump; no ping follows (no return leg)
    raw(
        test_db,
        "delivery_events",
        B1,
        3,
        event_id="EVT-3",
        order_id="ORD-2",
        event_ts="2026-09-10T09:00:00Z",
    )  # the driver's next order, hours later
    with test_db.cursor() as cur:
        [found] = run_check(
            cur, CHECKS_BY_NAME["raw_gps_impossible_speed"], scope(), QualityConfig()
        )
    assert (found.failing_rows, found.sample_keys) == (1, ("EVT-2",))


def test_out_of_file_order_counts_one_swap_once(test_db: Connection) -> None:
    add_batch(test_db, B1, 1)
    raw(test_db, "delivery_events", B1, 1, event_id="EVT-1", event_ts="2026-09-10T05:08:00Z")
    raw(
        test_db,
        "delivery_events",
        B1,
        2,
        event_id="EVT-2",
        event_type="picked_up",
        event_ts="2026-09-10T05:06:00Z",
    )  # a status event between the two swapped pings
    raw(test_db, "delivery_events", B1, 3, event_id="EVT-3", event_ts="2026-09-10T05:04:00Z")
    raw(test_db, "delivery_events", B1, 4, event_id="EVT-4", event_ts="2026-09-10T05:12:00Z")
    raw(
        test_db,
        "delivery_events",
        B1,
        5,
        event_id="EVT-5",
        event_ts="2026-09-10T05:04:00Z",
        lat="28.601000",
    )  # duplicate timestamp, reported by its own check
    with test_db.cursor() as cur:
        [found] = run_check(
            cur, CHECKS_BY_NAME["raw_events_out_of_file_order"], scope(), QualityConfig()
        )
    assert (found.failing_rows, found.sample_keys) == (1, ("EVT-3",))


def shift(conn: Connection, event_id: str, kind: str, ts: str) -> None:
    conn.execute(
        "INSERT INTO analytics.driver_shift_events (event_id, driver_id, event_type, event_ts) "
        "VALUES (%s, 'DRV-1', %s, %s)",
        (event_id, kind, ts),
    )


def test_utilisation_above_100_ignores_a_session_that_is_still_open(test_db: Connection) -> None:
    over_utilised(test_db)
    shift(test_db, "SHF-1", "online", "2026-09-10T03:00:00Z")
    shift(test_db, "SHF-2", "offline", "2026-09-10T04:00:00Z")
    shift(test_db, "SHF-3", "online", "2026-09-10T16:00:00Z")  # its offline has not arrived
    check = CHECKS_BY_NAME["utilisation_above_100"]
    with test_db.cursor() as cur:
        [still_open] = run_check(cur, check, scope(), QualityConfig())
        shift(test_db, "SHF-4", "offline", "2026-09-10T17:00:00Z")  # the session closes
        [closed] = run_check(cur, check, scope(), QualityConfig())
    assert (still_open.status, still_open.failing_rows) == (PASSED, 0)
    assert (closed.status, closed.sample_keys) == ("warned", ("DRV-1@2026-09-10",))


# ------------------------------------------------------------- the real pipeline ---


def test_checks_pass_on_clean_pipeline_output(test_db: Connection, fixture_dataset: Path) -> None:
    loads = load_dataset(test_db, fixture_dataset, CONFIG)
    for load in loads:
        assert load.quality is not None
        assert load.quality.blocking_failures == []
        integrity = [r for r in load.quality.results if r.layer != LAYER_RAW]
        assert [r for r in integrity if r.status != PASSED] == []
    manual, _ = run_database_quality(test_db, QualityConfig())
    assert manual.blocking_failures == []
    assert [r for r in manual.results if r.layer != LAYER_RAW and r.status != PASSED] == []
    stored = test_db.execute(
        "SELECT triggered_by, count(*) FROM audit.quality_runs GROUP BY triggered_by"
    ).fetchall()
    assert sorted(tuple(row) for row in stored) == [("load", len(loads)), ("manual", 1)]


def test_every_manifest_defect_type_is_detected_at_load_time(
    test_db: Connection, fixture_dataset: Path
) -> None:
    load_dataset(test_db, fixture_dataset, CONFIG)
    root = fixture_dataset.parent.parent
    manifest = load_manifest(root / "ground_truth" / fixture_dataset.name / MANIFEST_FILE_NAME)
    observed = load_time_results(test_db, fixture_dataset.name)
    batches = list(range(1, len(manifest.batch_ids) + 1))
    assert sorted(observed) == batches
    rows = coverage_rows(case_counts(manifest.cases), observed, batches)
    assert coverage_problems(rows) == []


METRIC_TABLES = (
    ("analytics", "daily_delivery_metrics"),
    ("analytics", "driver_performance"),
    ("analytics", "dim_date"),
    ("analytics", "dim_city"),
)


def test_blocking_failure_stops_the_batch_and_a_retry_matches_a_clean_load(
    test_db: Connection, fixture_dataset: Path
) -> None:
    folders = batch_folders(fixture_dataset)
    dataset = fixture_dataset.name
    with test_db.transaction(force_rollback=True):
        load_dataset(test_db, fixture_dataset, CONFIG)
        reference = digests(test_db, BUSINESS_TABLES)

    load_batch_folder(test_db, dataset, folders[0], CONFIG)
    row = test_db.execute("SELECT min(driver_id) FROM analytics.drivers").fetchone()
    assert row is not None
    # A corrupt row that satisfies every constraint but breaks orders_positive_amounts.
    order(test_db, order_id="ORD-BAD", driver_id=row[0], fare_amount=0)
    before = digests(test_db, BUSINESS_TABLES)
    metrics_before = digests(test_db, METRIC_TABLES)
    _, blocked_batch = read_batch_identity(folders[1])

    with pytest.raises(QualityGateError, match="quality gate: orders_positive_amounts"):
        load_batch_folder(test_db, dataset, folders[1], CONFIG)

    assert digests(test_db, METRIC_TABLES) == metrics_before
    assert digests(test_db, BUSINESS_TABLES) == before
    assert set(rows_for_batch(test_db, blocked_batch).values()) == {0}
    runs = test_db.execute(
        "SELECT DISTINCT status, failure_reason FROM audit.pipeline_runs WHERE batch_id = %s",
        (blocked_batch,),
    ).fetchall()
    assert runs == [("failed", "quality gate: orders_positive_amounts")]
    stored = test_db.execute(
        "SELECT q.status, q.attempt, r.status, r.sample_keys FROM audit.quality_runs q "
        "JOIN audit.quality_results r USING (quality_run_id) "
        "WHERE q.batch_id = %s AND r.check_name = 'orders_positive_amounts'",
        (blocked_batch,),
    ).fetchall()
    assert len(stored) == 1
    run_status, attempt, check_status, sample = stored[0]
    assert (run_status, attempt, check_status) == ("failed", 1, "failed")
    assert "ORD-BAD" in sample

    test_db.execute("DELETE FROM analytics.orders WHERE order_id = 'ORD-BAD'")
    loads = load_dataset(test_db, fixture_dataset, CONFIG)
    assert [(load.status, load.attempt) for load in loads] == [
        (SKIPPED, 0),
        (LOADED, 2),
        (LOADED, 1),
    ]
    assert digests(test_db, BUSINESS_TABLES) == reference


def test_thresholds_are_configurable(test_db: Connection) -> None:
    impossible_speed(test_db)
    check = CHECKS_BY_NAME["raw_gps_impossible_speed"]
    with test_db.cursor() as cur:
        [default] = run_check(cur, check, scope(), TINY_SAMPLES)
        [tolerant] = run_check(
            cur, check, scope(), QualityConfig(min_share_rows=1, thresholds={check.name: 0.5})
        )
        [slow_limit] = run_check(cur, check, scope(), QualityConfig(max_speed_kmh=2000))
    assert default.status == "warned"  # 1 of 3 pings = 33 % > 2 %
    assert tolerant.status == PASSED and tolerant.failing_rows == 1
    assert slow_limit.failing_rows == 0


def test_a_share_threshold_needs_a_minimum_sample(test_db: Connection) -> None:
    """Default minimum 50 rows: 1 duplicate in 3 rows passes, 2 in 60 rows still warns."""
    add_batch(test_db, B1, 1)
    add_batch(test_db, B2, 2, ("2026-09-11T00:00:00Z", "2026-09-12T00:00:00Z"))
    for row in range(1, 3):
        raw(test_db, "drivers", B1, row, driver_id="DRV-1")  # 2 identical rows
    raw(test_db, "drivers", B1, 3, driver_id="DRV-2")
    for row in range(1, 59):
        raw(test_db, "drivers", B2, row, driver_id=f"DRV-{row:03d}")
    raw(test_db, "drivers", B2, 59, driver_id="DRV-001")  # copies of rows 1 and 2
    raw(test_db, "drivers", B2, 60, driver_id="DRV-002")
    check = CHECKS_BY_NAME["raw_duplicate_rows"]
    with test_db.cursor() as cur:
        small = {r.check_name: r for r in run_check(cur, check, scope(B1), QualityConfig())}
        large = {r.check_name: r for r in run_check(cur, check, scope(B2), QualityConfig())}
    tiny = small["raw_duplicate_rows.drivers"]
    assert (tiny.status, tiny.failing_rows, tiny.rows_checked) == (PASSED, 1, 3)
    assert tiny.sample_keys == ("DRV-1",)  # counts and samples are still stored
    big = large["raw_duplicate_rows.drivers"]
    assert (big.status, big.failing_rows, big.rows_checked) == ("warned", 2, 60)  # 3.3 % > 2 %
