"""Integration tests of dimensions, aggregates and analyst queries (test database, rolled back).

Correctness is proven independently: tests/reference_metrics.py computes every metric in plain
Python from the accepted records of LocalState (not from the database), and the SQL results
must match it exactly.
"""

from collections.abc import Sequence
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, LiteralString

import psycopg
import pytest
from psycopg.rows import TupleRow

from src.contract import CITY_STATES, parse_utc, reporting_date
from src.database.analytics_queries import (
    data_period,
    default_parameters,
    load_queries,
    query_text,
)
from src.database.load_postgres import load_dataset, refresh_batch_metrics, write_analytics
from src.database.metrics import metric_parameters, rebuild_all, refresh_metrics
from src.database.postgres_state import PostgresState
from src.transformations.pipeline import TransformConfig, process_batch
from src.transformations.records import batch_folders, load_batch
from src.transformations.rules import DriverRecord, OrderRecord, ShiftRecord
from src.transformations.state import LocalState
from tests.reference_metrics import Order, Shift, daily_metrics, driver_performance

pytestmark = pytest.mark.integration

Connection = psycopg.Connection[TupleRow]
CONFIG = TransformConfig()
SEP_10 = date(2026, 9, 10)
SEP_11 = date(2026, 9, 11)
DRIVERS = (
    DriverRecord("DRV-1", "Delhi", "car", "2025-01-01", None),
    DriverRecord("DRV-2", "Delhi", "scooter", "2025-01-01", None),
)


def ts(text: str) -> int:
    epoch = parse_utc(text)
    assert epoch is not None, text
    return epoch


def make_order(
    order_id: str,
    created: str,
    status: str = "delivered",
    pickup: str | None = None,
    delivery: str | None = None,
    promised: str = "2026-09-10T23:00:00Z",
    driver_id: str = "DRV-1",
    updated: str | None = None,
) -> OrderRecord:
    """A valid accepted order (satisfies every analytics CHECK constraint)."""
    times = [ts(t) for t in (created, pickup, delivery) if t is not None]
    return OrderRecord(
        order_id=order_id,
        customer_id="CUST-1",
        driver_id=None if status == "cancelled" else driver_id,
        order_created_ts=ts(created),
        pickup_ts=None if pickup is None else ts(pickup),
        delivery_ts=None if delivery is None else ts(delivery),
        promised_ts=ts(promised),
        distance_km=Decimal("1.000"),
        status=status,
        city="Delhi",
        fare_amount=Decimal("10.00"),
        source_updated_at=ts(updated) if updated is not None else max(times),
    )


def make_shift(event_id: str, kind: str, at: str, driver_id: str = "DRV-1") -> ShiftRecord:
    return ShiftRecord(event_id, driver_id, kind, ts(at))


def to_reference(orders: Sequence[OrderRecord], shifts: Sequence[ShiftRecord]) -> tuple[Any, Any]:
    """Both aggregate tables computed by the independent Python reference."""
    ref_orders = [
        Order(
            o.order_id,
            o.driver_id,
            o.city,
            o.status,
            o.order_created_ts,
            o.pickup_ts,
            o.delivery_ts,
            o.promised_ts,
        )
        for o in orders
    ]
    ref_shifts = [Shift(s.event_id, s.driver_id, s.event_type, s.event_ts) for s in shifts]
    return daily_metrics(ref_orders), driver_performance(ref_orders, ref_shifts)


def apply_batch(
    conn: Connection,
    orders: Sequence[OrderRecord] = (),
    shifts: Sequence[ShiftRecord] = (),
    drivers: Sequence[DriverRecord] = (),
) -> list[date]:
    """Write accepted records like one batch would, then refresh the affected dates."""
    state = PostgresState(conn, "metrics-test")
    state.new_drivers = {d.driver_id: d for d in drivers}
    state.new_orders = {o.order_id: o for o in orders}
    state.new_shift_events = {s.event_id: s for s in shifts}
    with conn.cursor() as cur:
        written = write_analytics(cur, state)
        return refresh_batch_metrics(cur, state, written)


def sql_daily(conn: Connection) -> dict[tuple[date, str], tuple[Any, ...]]:
    rows = conn.execute(
        "SELECT metric_date, city, total_orders, avg_delivery_minutes, late_rate, "
        "cancelled_rate, delivered_orders, late_orders, cancelled_orders "
        "FROM analytics.daily_delivery_metrics"
    ).fetchall()
    return {(row[0], row[1]): tuple(row[2:]) for row in rows}


def sql_drivers(conn: Connection) -> dict[tuple[str, date], tuple[Any, ...]]:
    rows = conn.execute(
        "SELECT driver_id, metric_date, deliveries, avg_delivery_minutes, utilisation_pct, "
        "busy_minutes, online_minutes FROM analytics.driver_performance"
    ).fetchall()
    return {(row[0], row[1]): tuple(row[2:]) for row in rows}


def sql_dim_date(conn: Connection) -> dict[date, tuple[Any, ...]]:
    rows = conn.execute("SELECT * FROM analytics.dim_date").fetchall()
    return {row[0]: tuple(row[1:]) for row in rows}


def differences(actual: dict[Any, Any], expected: dict[Any, Any]) -> list[str]:
    """Readable differences between two keyed result sets (first 20)."""
    found = [
        f"{key}: sql={actual.get(key)} python={expected.get(key)}"
        for key in sorted(set(actual) | set(expected), key=str)
        if actual.get(key) != expected.get(key)
    ]
    return found[:20]


def accepted_records(dataset_dir: Path) -> tuple[list[OrderRecord], list[ShiftRecord]]:
    """Accepted orders (latest versions) and shift events, computed without the database."""
    state = LocalState()
    for folder in batch_folders(dataset_dir):
        process_batch(load_batch(folder), state, CONFIG)
    return list(state.orders.values()), list(state.shift_events.values())


# ------------------------------------------------------------ independent check ---


def test_sql_metrics_equal_the_python_reference_on_the_fixture(
    test_db: Connection, fixture_dataset: Path
) -> None:
    load_dataset(test_db, fixture_dataset, CONFIG)
    expected_daily, expected_drivers = to_reference(*accepted_records(fixture_dataset))
    assert expected_daily and expected_drivers
    assert differences(sql_daily(test_db), expected_daily) == []
    assert differences(sql_drivers(test_db), expected_drivers) == []


def test_incremental_refresh_equals_a_full_rebuild(
    test_db: Connection, fixture_dataset: Path
) -> None:
    load_dataset(test_db, fixture_dataset, CONFIG)
    incremental = (sql_daily(test_db), sql_drivers(test_db), sql_dim_date(test_db))
    rebuild_all(test_db)
    assert (sql_daily(test_db), sql_drivers(test_db), sql_dim_date(test_db)) == incremental
    # dim_date is exactly the continuous range of dates with data, on either path.
    dim_dates = sorted(incremental[2])
    assert dim_dates == [
        dim_dates[0] + timedelta(days=n) for n in range((dim_dates[-1] - dim_dates[0]).days + 1)
    ]
    data_range = test_db.execute(
        "SELECT min(order_created_ts), max(coalesce(delivery_ts, order_created_ts)) "
        "FROM analytics.orders"
    ).fetchone()
    assert data_range is not None
    shift_range = test_db.execute(
        "SELECT min(event_ts), max(event_ts) FROM analytics.driver_shift_events"
    ).fetchone()
    assert shift_range is not None
    first = min(reporting_date(int(v.timestamp())) for v in (data_range[0], shift_range[0]))
    last = max(reporting_date(int(v.timestamp())) for v in (data_range[1], shift_range[1]))
    assert (dim_dates[0], dim_dates[-1]) == (first, last)


def test_dimensions_cover_every_metric_date_and_city(
    test_db: Connection, fixture_dataset: Path
) -> None:
    load_dataset(test_db, fixture_dataset, CONFIG)
    rows = test_db.execute("SELECT city, state FROM analytics.dim_city").fetchall()
    cities = {row[0]: row[1] for row in rows}
    order_cities = {row[0] for row in test_db.execute("SELECT city FROM analytics.orders")}
    assert order_cities <= set(cities)
    assert all(CITY_STATES[city] == state for city, state in cities.items())
    dim_dates = {row[0] for row in test_db.execute("SELECT date_key FROM analytics.dim_date")}
    assert {key[0] for key in sql_daily(test_db)} <= dim_dates
    assert {key[1] for key in sql_drivers(test_db)} <= dim_dates


# -------------------------------------------------------------------- edge cases ---


def test_ist_day_boundary_and_cancelled_orders(test_db: Connection) -> None:
    orders = [
        make_order("O1", "2026-09-10T18:29:59Z", status="cancelled"),  # 23:59:59 IST, 10 Sep
        make_order("O2", "2026-09-10T18:30:00Z", status="cancelled"),  # 00:00:00 IST, 11 Sep
    ]
    apply_batch(test_db, orders, drivers=DRIVERS)
    expected_row = (1, None, None, Decimal("1.0000"), 0, 0, 1)
    assert sql_daily(test_db) == {(SEP_10, "Delhi"): expected_row, (SEP_11, "Delhi"): expected_row}
    assert sql_daily(test_db) == to_reference(orders, [])[0]


def test_cancelled_order_is_excluded_from_late_rate_and_average(test_db: Connection) -> None:
    orders = [
        make_order(
            "A",
            "2026-09-10T05:00:00Z",
            pickup="2026-09-10T05:10:00Z",
            delivery="2026-09-10T05:40:00Z",
            promised="2026-09-10T05:45:00Z",
        ),
        make_order(
            "B",
            "2026-09-10T06:00:00Z",
            pickup="2026-09-10T06:10:00Z",
            delivery="2026-09-10T06:55:00Z",
            promised="2026-09-10T06:45:00Z",
        ),
        make_order("C", "2026-09-10T07:00:00Z", status="cancelled"),
    ]
    apply_batch(test_db, orders, drivers=DRIVERS)
    expected = (3, Decimal("37.50"), Decimal("0.5000"), Decimal("0.3333"), 2, 1, 1)
    assert sql_daily(test_db) == {(SEP_10, "Delhi"): expected}
    assert sql_daily(test_db) == to_reference(orders, [])[0]


def test_order_updated_in_a_later_batch_moves_between_dates(test_db: Connection) -> None:
    first = make_order(
        "U",
        "2026-09-10T18:00:00Z",
        pickup="2026-09-10T18:05:00Z",
        delivery="2026-09-10T18:15:00Z",  # 23:45 IST, 10 Sep
        promised="2026-09-10T19:00:00Z",
    )
    apply_batch(test_db, [first], drivers=DRIVERS)
    assert sql_daily(test_db)[(SEP_10, "Delhi")][4] == 1  # delivered on 10 Sep

    corrected = make_order(
        "U",
        "2026-09-10T18:00:00Z",
        pickup="2026-09-10T18:05:00Z",
        delivery="2026-09-10T18:50:00Z",  # 00:20 IST, 11 Sep
        promised="2026-09-10T19:00:00Z",
    )
    dates = apply_batch(test_db, [corrected])
    assert dates == [SEP_10, SEP_11]  # the old date is recomputed too (RETURNING old.*)
    daily = sql_daily(test_db)
    assert daily[(SEP_10, "Delhi")] == (1, None, None, Decimal("0.0000"), 0, 0, 0)
    assert daily[(SEP_11, "Delhi")] == (0, Decimal("45.00"), Decimal("0.0000"), None, 1, 0, 0)
    drivers = sql_drivers(test_db)
    assert drivers[("DRV-1", SEP_10)] == (0, None, None, Decimal("25.00"), None)
    assert drivers[("DRV-1", SEP_11)] == (1, Decimal("45.00"), None, Decimal("20.00"), None)
    assert (daily, drivers) == to_reference([corrected], [])


def test_late_shift_event_changes_utilisation_exactly_once(test_db: Connection) -> None:
    delivered = make_order(
        "L",
        "2026-09-10T05:50:00Z",
        pickup="2026-09-10T06:00:00Z",
        delivery="2026-09-10T06:30:00Z",
        promised="2026-09-10T07:00:00Z",
    )
    online = make_shift("S1", "online", "2026-09-10T03:00:00Z")
    apply_batch(test_db, [delivered], [online], drivers=DRIVERS)
    assert sql_drivers(test_db)[("DRV-1", SEP_10)][2] is None  # unpaired online

    offline = make_shift("S2", "offline", "2026-09-10T11:00:00Z")
    dates = apply_batch(test_db, shifts=[offline])
    assert dates == [date(2026, 9, 9), SEP_10, SEP_11]  # refreshed: the shift date +-1 day
    assert list(sql_dim_date(test_db)) == [SEP_10]  # dim_date: only the date with data
    after_late_event = sql_drivers(test_db)
    assert after_late_event[("DRV-1", SEP_10)] == (
        1,
        Decimal("30.00"),
        Decimal("6.25"),
        Decimal("30.00"),
        Decimal("480.00"),
    )
    with test_db.cursor() as cur:
        refresh_metrics(cur, dates)  # replaying the refresh changes nothing
    assert sql_drivers(test_db) == after_late_event
    assert after_late_event == to_reference([delivered], [online, offline])[1]


def test_driver_without_deliveries_and_utilisation_with_breaks(test_db: Connection) -> None:
    orders = [
        make_order(
            "B1",
            "2026-09-10T03:50:00Z",
            pickup="2026-09-10T04:00:00Z",
            delivery="2026-09-10T07:30:00Z",
            promised="2026-09-10T08:00:00Z",
        )
    ]
    shifts = [
        make_shift("S1", "online", "2026-09-10T03:30:00Z"),
        make_shift("S2", "break_start", "2026-09-10T07:30:00Z"),
        make_shift("S3", "break_end", "2026-09-10T08:00:00Z"),
        make_shift("S4", "offline", "2026-09-10T11:30:00Z"),
        make_shift("S5", "online", "2026-09-10T03:00:00Z", driver_id="DRV-2"),
        make_shift("S6", "offline", "2026-09-10T11:00:00Z", driver_id="DRV-2"),
    ]
    apply_batch(test_db, orders, shifts, drivers=DRIVERS)
    drivers = sql_drivers(test_db)
    assert drivers[("DRV-1", SEP_10)] == (
        1,
        Decimal("210.00"),
        Decimal("46.67"),
        Decimal("210.00"),
        Decimal("450.00"),
    )
    assert drivers[("DRV-2", SEP_10)] == (
        0,
        None,
        Decimal("0.00"),
        Decimal("0.00"),
        Decimal("480.00"),
    )
    assert drivers == to_reference(orders, shifts)[1]


def test_overlapping_deliveries_count_once(test_db: Connection) -> None:
    orders = [
        make_order(
            "P1",
            "2026-09-10T03:50:00Z",
            pickup="2026-09-10T04:00:00Z",
            delivery="2026-09-10T05:00:00Z",
        ),
        make_order(
            "P2",
            "2026-09-10T04:20:00Z",
            pickup="2026-09-10T04:30:00Z",
            delivery="2026-09-10T05:30:00Z",
        ),
    ]
    apply_batch(test_db, orders, drivers=DRIVERS)
    expected = (2, Decimal("60.00"), None, Decimal("90.00"), None)
    assert sql_drivers(test_db) == {("DRV-1", SEP_10): expected}
    assert sql_drivers(test_db) == to_reference(orders, [])[1]


# ---------------------------------------------------------------- analyst queries ---

# Orders delivered on %(day)s: counted directly, and through a naive join with their events.
DELIVERED_ON_DAY: LiteralString = (
    "SELECT count(*) FROM analytics.orders o "
    "WHERE o.status = 'delivered' "
    "AND o.delivery_ts >= (%(day)s::date::timestamp AT TIME ZONE %(tz)s) "
    "AND o.delivery_ts < ((%(day)s::date + 1)::timestamp AT TIME ZONE %(tz)s)"
)
DELIVERED_ON_DAY_NAIVE_JOIN: LiteralString = (
    "SELECT count(*) FROM analytics.orders o "
    "JOIN analytics.delivery_events e ON e.order_id = o.order_id "
    "WHERE o.status = 'delivered' "
    "AND o.delivery_ts >= (%(day)s::date::timestamp AT TIME ZONE %(tz)s) "
    "AND o.delivery_ts < ((%(day)s::date + 1)::timestamp AT TIME ZONE %(tz)s)"
)


def test_events_are_aggregated_before_joining_so_order_counts_never_inflate(
    test_db: Connection, fixture_dataset: Path
) -> None:
    load_dataset(test_db, fixture_dataset, CONFIG)
    first, last = data_period(test_db)
    parameters = default_parameters(first, last)["order_event_summary"]
    rows = test_db.execute(query_text("order_event_summary"), parameters).fetchall()
    summarised = sum(int(row[1]) for row in rows)
    direct = test_db.execute(DELIVERED_ON_DAY, parameters).fetchone()
    naive = test_db.execute(DELIVERED_ON_DAY_NAIVE_JOIN, parameters).fetchone()
    assert direct is not None and naive is not None
    assert summarised == direct[0] > 0
    assert naive[0] > direct[0]  # the naive join would inflate order counts


def test_every_analyst_query_runs_on_the_fixture(
    test_db: Connection, fixture_dataset: Path
) -> None:
    load_dataset(test_db, fixture_dataset, CONFIG)
    first, last = data_period(test_db)
    parameters = default_parameters(first, last)
    for name in load_queries():
        rows = test_db.execute(query_text(name), parameters[name]).fetchall()
        assert rows, name
    peak = test_db.execute(query_text("peak_hours"), parameters["peak_hours"]).fetchall()
    assert abs(sum(row[2] for row in peak) - Decimal(100)) < Decimal("0.5")


def test_refresh_select_measured_by_explain_runs(
    test_db: Connection, fixture_dataset: Path
) -> None:
    load_dataset(test_db, fixture_dataset, CONFIG)
    _, last = data_period(test_db)
    rows = test_db.execute(query_text("refresh_driver_performance"), metric_parameters([last]))
    assert rows.fetchall()
