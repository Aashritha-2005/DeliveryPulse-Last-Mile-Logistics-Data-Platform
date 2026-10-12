"""Integration tests of the dashboard's data (PostgreSQL).

Each view is compared with an independent computation from the accepted records of the
1,500-order fixture (tests/reference_metrics.py and plain Python), loaded inside a rolled-back
transaction. A separate test runs every dashboard query as the read-only role.
"""

from collections import Counter
from datetime import date, datetime
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path

import psycopg
import pytest
from psycopg.rows import TupleRow

from src.dashboard.data import (
    MIN_DELIVERIES,
    QUERY_NAMES,
    TOP_DRIVERS,
    Filters,
    check,
    cities,
    data_period,
    default_filters,
    fetch,
    ratio,
)
from src.database.load_postgres import load_dataset
from src.transformations.pipeline import TransformConfig
from tests.integration.test_metrics import accepted_records
from tests.reference_metrics import TZ, Order, daily_metrics, local_date

pytestmark = pytest.mark.integration

Connection = psycopg.Connection[TupleRow]
CONFIG = TransformConfig()


def reference_orders(fixture_dataset: Path) -> list[Order]:
    orders, _ = accepted_records(fixture_dataset)
    return [
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


def within(day: date, filters: Filters) -> bool:
    return filters.start <= day <= filters.end


def delivered_in(orders: list[Order], filters: Filters) -> list[tuple[Order, int, int]]:
    """(order, pickup, delivery) of the orders delivered in the range and cities."""
    found: list[tuple[Order, int, int]] = []
    for o in orders:
        if o.status != "delivered" or o.pickup is None or o.delivery is None:
            continue
        if o.city in filters.cities and within(local_date(o.delivery), filters):
            found.append((o, o.pickup, o.delivery))
    return found


def created_in(orders: list[Order], filters: Filters) -> list[Order]:
    return [
        o for o in orders if o.city in filters.cities and within(local_date(o.created), filters)
    ]


def assert_matches_reference(test_db: Connection, orders: list[Order], filters: Filters) -> None:
    data = fetch(test_db, filters)
    daily = {
        key: row
        for key, row in daily_metrics(orders).items()
        if within(key[0], filters) and key[1] in filters.cities
    }
    delivered = delivered_in(orders, filters)
    created = created_in(orders, filters)

    # KPIs: counts, and rates from counts
    assert data.kpis.orders_created == len(created)
    assert data.kpis.cancelled_orders == sum(o.status == "cancelled" for o in created)
    assert data.kpis.delivered_orders == len(delivered)
    assert data.kpis.late_orders == sum(end > o.promised for o, _, end in delivered)
    seconds = sum(end - start for _, start, end in delivered)
    assert data.kpis.avg_delivery_minutes == ratio(seconds, 60 * len(delivered), 2)

    # delivery-time trend: the stored daily averages per city
    expected_trend = sorted(
        (day, city, row[1], row[4]) for (day, city), row in daily.items() if row[4] > 0
    )
    actual_trend = [
        (p.metric_date, p.city, p.avg_delivery_minutes, p.delivered_orders) for p in data.trend
    ]
    assert actual_trend == expected_trend

    # late-delivery rate by city, from counts
    per_city: dict[str, list[int]] = {}
    for (_, city), row in daily.items():
        counts = per_city.setdefault(city, [0, 0])
        counts[0] += row[4]
        counts[1] += row[5]
    assert [(r.city, r.delivered_orders, r.late_orders) for r in data.late_by_city] == sorted(
        (city, counts[0], counts[1]) for city, counts in per_city.items()
    )

    # peak hours: orders created per local hour
    hours = Counter(datetime.fromtimestamp(o.created, TZ).hour for o in created)
    assert {row.hour_of_day: row.orders for row in data.peak_hours} == dict(hours)

    # top drivers: fastest average among drivers with enough deliveries
    by_driver: dict[str, list[int]] = {}
    for o, start, end in delivered:
        assert o.driver_id is not None
        totals = by_driver.setdefault(o.driver_id, [0, 0])
        totals[0] += 1
        totals[1] += end - start
    ranked = sorted(
        (ratio(total, 60 * count, 2), driver, count)
        for driver, (count, total) in by_driver.items()
        if count >= MIN_DELIVERIES
    )[:TOP_DRIVERS]
    assert [(r.avg_delivery_minutes, r.driver_id, r.deliveries) for r in data.top_drivers] == [
        (avg, driver, count) for avg, driver, count in ranked
    ]


def test_every_view_equals_an_independent_computation(
    test_db: Connection, fixture_dataset: Path
) -> None:
    load_dataset(test_db, fixture_dataset, CONFIG)
    period = data_period(test_db)
    assert period is not None
    filters = default_filters(period, cities(test_db))
    orders = reference_orders(fixture_dataset)
    assert_matches_reference(test_db, orders, filters)
    data = fetch(test_db, filters)
    assert data.trend and data.late_by_city and data.peak_hours and data.top_drivers
    assert sum(row.orders for row in data.peak_hours) == data.kpis.orders_created


def test_city_and_date_filters_restrict_every_view(
    test_db: Connection, fixture_dataset: Path
) -> None:
    load_dataset(test_db, fixture_dataset, CONFIG)
    period = data_period(test_db)
    assert period is not None
    one_week = Filters(period[0], date.fromordinal(period[0].toordinal() + 6), ("Delhi",))
    orders = reference_orders(fixture_dataset)
    assert_matches_reference(test_db, orders, one_week)
    data = fetch(test_db, one_week)
    assert {p.city for p in data.trend} == {"Delhi"}
    assert [r.city for r in data.late_by_city] == ["Delhi"]
    assert all(one_week.start <= p.metric_date <= one_week.end for p in data.trend)
    everything = fetch(test_db, default_filters(period, cities(test_db)))
    assert data.kpis.orders_created < everything.kpis.orders_created


def test_utilisation_of_top_drivers_is_weighted_over_the_range(
    test_db: Connection, fixture_dataset: Path
) -> None:
    load_dataset(test_db, fixture_dataset, CONFIG)
    period = data_period(test_db)
    assert period is not None
    data = fetch(test_db, default_filters(period, cities(test_db)))
    for row in data.top_drivers:
        sums = test_db.execute(
            "SELECT sum(busy_minutes), sum(online_minutes) FROM analytics.driver_performance "
            "WHERE driver_id = %s AND metric_date BETWEEN %s AND %s "
            "AND online_minutes IS NOT NULL",
            (row.driver_id, period[0], period[1]),
        ).fetchone()
        assert sums is not None
        busy, online = sums
        expected = (
            None
            if online is None or online == 0
            else (Decimal(busy) * 100 / Decimal(online)).quantize(
                Decimal("0.01"), rounding=ROUND_HALF_UP
            )
        )
        assert row.utilisation_pct == expected, row.driver_id


def test_check_runs_every_query_with_rows(test_db: Connection, fixture_dataset: Path) -> None:
    load_dataset(test_db, fixture_dataset, CONFIG)
    timings = check(test_db)
    assert [name for name, _, _ in timings] == list(QUERY_NAMES)
    assert all(rows > 0 for _, rows, _ in timings), timings


def test_every_dashboard_query_runs_as_the_read_only_role(readonly_conn: Connection) -> None:
    """Privileges: the dashboard role can run every query (the main database may be empty)."""
    period = data_period(readonly_conn) or (date(2026, 9, 10), date(2026, 9, 30))
    known = cities(readonly_conn) or ["Delhi"]
    data = fetch(readonly_conn, default_filters(period, known))
    assert data.kpis.orders_created >= 0
    role = readonly_conn.execute("SELECT current_user").fetchone()
    assert role is not None and role[0] == "deliverypulse_readonly"
