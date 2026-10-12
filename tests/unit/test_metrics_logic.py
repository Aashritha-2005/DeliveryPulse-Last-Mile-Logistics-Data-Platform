"""Unit tests (no database): affected dates, the Python metric reference, reference data and
the analyst query file."""

import re
from datetime import date
from decimal import Decimal

import pytest

from src.contract import CITY_STATES, parse_utc
from src.database.analytics_queries import (
    FULL_PERIOD_QUERIES,
    REFRESH_QUERY,
    default_parameters,
    load_queries,
)
from src.database.metrics import affected_dates
from src.ingestion.generator_config import CITIES
from tests.reference_metrics import Order, Shift, daily_metrics, driver_performance

pytestmark = pytest.mark.unit

KNOWN_PARAMETERS = {"tz", "start_date", "end_date", "day", "min_deliveries", "limit"}
ANALYST_QUERIES = [
    "avg_delivery_time_by_city",
    "late_rate_by_day",
    "peak_hours",
    "top_bottom_drivers",
    "driver_utilisation",
    "rolling_7_day",
    "order_event_summary",
]
SEP_10 = date(2026, 9, 10)
SEP_11 = date(2026, 9, 11)


def ts(text: str) -> int:
    epoch = parse_utc(text)
    assert epoch is not None, text
    return epoch


def order(
    order_id: str,
    created: str,
    status: str = "delivered",
    pickup: str | None = None,
    delivery: str | None = None,
    promised: str | None = None,
    driver_id: str | None = "DRV-1",
    city: str = "Delhi",
) -> Order:
    return Order(
        order_id=order_id,
        driver_id=driver_id if status != "cancelled" else None,
        city=city,
        status=status,
        created=ts(created),
        pickup=None if pickup is None else ts(pickup),
        delivery=None if delivery is None else ts(delivery),
        promised=ts(promised or "2026-09-10T23:00:00Z"),
    )


def shift(event_id: str, kind: str, at: str, driver_id: str = "DRV-1") -> Shift:
    return Shift(event_id, driver_id, kind, ts(at))


# ------------------------------------------------------------- affected dates ---


def test_affected_dates_split_at_ist_midnight() -> None:
    before = (ts("2026-09-10T18:29:59Z"), None, None)  # 23:59:59 IST, 10 Sep
    after = (ts("2026-09-10T18:30:00Z"), None, None)  # 00:00:00 IST, 11 Sep
    assert affected_dates([before], []) == [SEP_10]
    assert affected_dates([after], []) == [SEP_11]


def test_affected_dates_cover_every_day_between_pickup_and_delivery() -> None:
    version = (ts("2026-09-10T18:00:00Z"), ts("2026-09-10T18:20:00Z"), ts("2026-09-10T18:40:00Z"))
    assert affected_dates([version], []) == [SEP_10, SEP_11]


def test_affected_dates_include_old_and_new_versions() -> None:
    old = (ts("2026-09-10T10:00:00Z"), ts("2026-09-10T10:15:00Z"), ts("2026-09-10T10:40:00Z"))
    new = (ts("2026-09-10T10:00:00Z"), ts("2026-09-11T19:00:00Z"), ts("2026-09-11T19:20:00Z"))
    assert affected_dates([old, new], []) == [SEP_10, date(2026, 9, 12)]


def test_affected_dates_widen_shift_events_by_one_day() -> None:
    assert affected_dates([], [ts("2026-09-10T04:30:00Z")]) == [
        date(2026, 9, 9),
        SEP_10,
        SEP_11,
    ]


# ------------------------------------------------------- the Python reference ---


def test_reference_daily_metrics_by_hand() -> None:
    orders = [
        order(
            "A",
            "2026-09-10T05:00:00Z",
            pickup="2026-09-10T05:10:00Z",
            delivery="2026-09-10T05:40:00Z",
            promised="2026-09-10T05:45:00Z",
        ),
        order(
            "B",
            "2026-09-10T06:00:00Z",
            pickup="2026-09-10T06:10:00Z",
            delivery="2026-09-10T06:55:00Z",
            promised="2026-09-10T06:45:00Z",
        ),
        order("C", "2026-09-10T07:00:00Z", status="cancelled"),
    ]
    rows = daily_metrics(orders)
    # 3 created, 1 cancelled; 2 delivered (30 and 45 minutes), 1 late.
    assert rows == {
        (SEP_10, "Delhi"): (3, Decimal("37.50"), Decimal("0.5000"), Decimal("0.3333"), 2, 1, 1)
    }


def test_reference_utilisation_with_a_break() -> None:
    orders = [
        order(
            "A",
            "2026-09-10T03:50:00Z",
            pickup="2026-09-10T04:00:00Z",
            delivery="2026-09-10T07:30:00Z",
            promised="2026-09-10T08:00:00Z",
        )
    ]
    shifts = [
        shift("S1", "online", "2026-09-10T03:30:00Z"),
        shift("S2", "break_start", "2026-09-10T07:30:00Z"),
        shift("S3", "break_end", "2026-09-10T08:00:00Z"),
        shift("S4", "offline", "2026-09-10T11:30:00Z"),
    ]
    # Busy 210 min; online 480 - 30 = 450 min; 210 / 450 = 46.67 %.
    assert driver_performance(orders, shifts) == {
        ("DRV-1", SEP_10): (
            1,
            Decimal("210.00"),
            Decimal("46.67"),
            Decimal("210.00"),
            Decimal("450.00"),
        )
    }


def test_reference_counts_overlapping_deliveries_once_and_unpaired_online_is_null() -> None:
    orders = [
        order(
            "A",
            "2026-09-10T03:50:00Z",
            pickup="2026-09-10T04:00:00Z",
            delivery="2026-09-10T05:00:00Z",
        ),
        order(
            "B",
            "2026-09-10T04:20:00Z",
            pickup="2026-09-10T04:30:00Z",
            delivery="2026-09-10T05:30:00Z",
        ),
    ]
    shifts = [shift("S1", "online", "2026-09-10T03:00:00Z")]  # no offline: unpaired
    row = driver_performance(orders, shifts)[("DRV-1", SEP_10)]
    assert row == (2, Decimal("60.00"), None, Decimal("90.00"), None)


def test_reference_driver_without_deliveries() -> None:
    shifts = [
        shift("S1", "online", "2026-09-10T03:00:00Z", driver_id="DRV-2"),
        shift("S2", "offline", "2026-09-10T11:00:00Z", driver_id="DRV-2"),
    ]
    assert driver_performance([], shifts) == {
        ("DRV-2", SEP_10): (0, None, Decimal("0.00"), Decimal("0.00"), Decimal("480.00"))
    }


def test_reference_busy_time_is_split_at_ist_midnight() -> None:
    orders = [
        order(
            "A",
            "2026-09-10T18:00:00Z",
            pickup="2026-09-10T18:20:00Z",  # 23:50 IST, 10 Sep
            delivery="2026-09-10T18:40:00Z",  # 00:10 IST, 11 Sep
            promised="2026-09-10T19:00:00Z",
        )
    ]
    rows = driver_performance(orders, [])
    assert rows[("DRV-1", SEP_10)] == (0, None, None, Decimal("10.00"), None)
    assert rows[("DRV-1", SEP_11)] == (1, Decimal("20.00"), None, Decimal("10.00"), None)


# ------------------------------------------------------------- reference data ---


def test_city_states_match_the_generator_cities() -> None:
    assert {city.name: city.state for city in CITIES} == CITY_STATES


# ---------------------------------------------------------- analyst queries ---


def test_analytics_sql_has_every_named_query_with_known_parameters() -> None:
    queries = load_queries()
    assert list(queries) == ANALYST_QUERIES
    for name, text in queries.items():
        assert text.lstrip().upper().startswith(("SELECT", "WITH", "--")), name
        assert not text.rstrip().endswith(";"), name
        used = set(re.findall(r"%\((\w+)\)s", text))
        assert used and used <= KNOWN_PARAMETERS, (name, used)


def test_default_parameters_derive_ranges_from_the_data_period() -> None:
    parameters = default_parameters(date(2026, 9, 10), date(2026, 9, 30))
    assert set(parameters) == {*ANALYST_QUERIES, REFRESH_QUERY}
    for name in FULL_PERIOD_QUERIES:
        assert parameters[name]["start_date"] == date(2026, 9, 10)
    assert parameters["avg_delivery_time_by_city"]["start_date"] == date(2026, 9, 24)
    assert parameters["order_event_summary"]["day"] == date(2026, 9, 30)
    assert parameters[REFRESH_QUERY]["dates"] == [date(2026, 9, 30)]
