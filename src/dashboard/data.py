"""Data access for the dashboard: read-only queries, typed results and pure helpers.

Every query lives in ``sql/dashboard.sql`` and reads the ``analytics`` schema only. The dashboard
always connects as ``deliverypulse_readonly``. Rates are computed here from counts (one
division, rounded half up), so a period or a set of cities never averages daily rates.

`make dashboard-check` runs every query once without a browser and prints row counts and
timings:

    uv run python -m src.dashboard.data
"""

import logging
import sys
import time
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path
from typing import Any, Final

import psycopg

from src.config import PROJECT_ROOT, ConfigError, load_database_settings
from src.contract import REPORTING_TIMEZONE
from src.database.analytics_queries import load_queries
from src.database.postgres_state import Connection
from src.utils.logging import configure_logging

logger = logging.getLogger("deliverypulse.dashboard")

DASHBOARD_SQL: Path = PROJECT_ROOT / "sql" / "dashboard.sql"
DASHBOARD_ROLE: Final = "readonly"
QUERY_NAMES = (
    "data_period",
    "cities",
    "kpis",
    "delivery_time_trend",
    "late_rate_by_city",
    "peak_hours",
    "top_drivers",
)
MIN_DELIVERIES = 5
TOP_DRIVERS = 10


class FilterError(ValueError):
    """The requested filters cannot be applied."""


@dataclass(frozen=True)
class Filters:
    """Reporting-date range (inclusive) and the cities to include."""

    start: date
    end: date
    cities: tuple[str, ...]


@dataclass(frozen=True)
class Kpis:
    """Headline counts for the filters, plus rates computed from them."""

    orders_created: int
    cancelled_orders: int
    delivered_orders: int
    late_orders: int
    delivery_seconds: Decimal

    @property
    def late_rate(self) -> Decimal | None:
        """Late delivered / delivered orders (cancelled excluded); None without deliveries."""
        return ratio(self.late_orders, self.delivered_orders, 4)

    @property
    def cancelled_rate(self) -> Decimal | None:
        """Cancelled / created orders; None without orders."""
        return ratio(self.cancelled_orders, self.orders_created, 4)

    @property
    def avg_delivery_minutes(self) -> Decimal | None:
        """Mean pickup-to-delivery minutes of delivered orders; None without deliveries."""
        return ratio(self.delivery_seconds, 60 * self.delivered_orders, 2)


@dataclass(frozen=True)
class TrendPoint:
    """One reporting day and city of the delivery-time trend."""

    metric_date: date
    city: str
    avg_delivery_minutes: Decimal
    delivered_orders: int


@dataclass(frozen=True)
class CityLateRate:
    """Late-delivery counts of one city over the range."""

    city: str
    delivered_orders: int
    late_orders: int

    @property
    def late_rate(self) -> Decimal | None:
        return ratio(self.late_orders, self.delivered_orders, 4)


@dataclass(frozen=True)
class HourCount:
    """Orders created in one hour of day (reporting timezone)."""

    hour_of_day: int
    orders: int


@dataclass(frozen=True)
class DriverRow:
    """One of the fastest drivers over the range."""

    driver_id: str
    city: str
    deliveries: int
    avg_delivery_minutes: Decimal
    utilisation_pct: Decimal | None


@dataclass(frozen=True)
class DashboardData:
    """Everything the dashboard shows for one set of filters."""

    filters: Filters
    kpis: Kpis
    trend: tuple[TrendPoint, ...]
    late_by_city: tuple[CityLateRate, ...]
    peak_hours: tuple[HourCount, ...]
    top_drivers: tuple[DriverRow, ...]

    @property
    def is_empty(self) -> bool:
        """True when the filters match no created and no delivered order."""
        return self.kpis.orders_created == 0 and self.kpis.delivered_orders == 0


# --------------------------------------------------------------------- helpers ---


def ratio(numerator: int | Decimal, denominator: int, places: int) -> Decimal | None:
    """``numerator / denominator`` rounded half up to ``places``; None if the denominator is 0."""
    if denominator == 0:
        return None
    exponent = Decimal(1).scaleb(-places)
    return (Decimal(numerator) / Decimal(denominator)).quantize(exponent, rounding=ROUND_HALF_UP)


def default_filters(period: tuple[date, date], cities: Sequence[str]) -> Filters:
    """The whole loaded period and every city."""
    return Filters(period[0], period[1], tuple(cities))


def validate_filters(filters: Filters, period: tuple[date, date], cities: Sequence[str]) -> None:
    """Reject a reversed range, a range outside the loaded data, or an unknown city."""
    if filters.start > filters.end:
        raise FilterError(f"start date {filters.start} is after end date {filters.end}")
    if filters.end < period[0] or filters.start > period[1]:
        raise FilterError(
            f"{filters.start} to {filters.end} is outside the loaded data "
            f"({period[0]} to {period[1]})"
        )
    unknown = sorted(set(filters.cities) - set(cities))
    if unknown:
        raise FilterError(f"unknown cities: {', '.join(unknown)}")


def parameters(filters: Filters) -> dict[str, Any]:
    """Bound parameters shared by the dashboard queries."""
    return {
        "tz": REPORTING_TIMEZONE,
        "start_date": filters.start,
        "end_date": filters.end,
        "cities": list(filters.cities),
        "min_deliveries": MIN_DELIVERIES,
        "limit": TOP_DRIVERS,
    }


def percent(value: Decimal | None) -> float | None:
    """A 0–1 rate as a percentage for display; None stays None."""
    return None if value is None else float(value * 100)


def trend_series(trend: Sequence[TrendPoint]) -> dict[str, list[Any]]:
    """Long-format columns for a line chart: one line per city."""
    return {
        "date": [point.metric_date for point in trend],
        "city": [point.city for point in trend],
        "avg delivery minutes": [float(point.avg_delivery_minutes) for point in trend],
    }


def late_rate_series(rows: Sequence[CityLateRate]) -> dict[str, list[Any]]:
    """Columns for the late-rate bar chart (percent); cities without deliveries are left out."""
    shown = [row for row in rows if row.late_rate is not None]
    return {
        "city": [row.city for row in shown],
        "late rate %": [percent(row.late_rate) for row in shown],
    }


def peak_hour_series(rows: Sequence[HourCount]) -> dict[str, list[int]]:
    """Orders for all 24 hours, zero where no order was created."""
    by_hour = {row.hour_of_day: row.orders for row in rows}
    hours = list(range(24))
    return {"hour": hours, "orders": [by_hour.get(hour, 0) for hour in hours]}


def driver_table(rows: Sequence[DriverRow]) -> dict[str, list[Any]]:
    """Columns for the top-drivers table."""
    return {
        "driver": [row.driver_id for row in rows],
        "city": [row.city for row in rows],
        "deliveries": [row.deliveries for row in rows],
        "avg delivery minutes": [float(row.avg_delivery_minutes) for row in rows],
        "utilisation %": [
            None if row.utilisation_pct is None else float(row.utilisation_pct) for row in rows
        ],
    }


# ---------------------------------------------------------------------- queries ---


def queries() -> dict[str, str]:
    """The named queries of ``sql/dashboard.sql``."""
    return load_queries(DASHBOARD_SQL)


def connect() -> Connection:
    """An autocommit connection as the dashboard's read-only role."""
    settings = load_database_settings(DASHBOARD_ROLE)
    return psycopg.connect(settings.conninfo(), autocommit=True)


def data_period(conn: Connection) -> tuple[date, date] | None:
    """First and last reporting date with created orders; None when nothing is loaded."""
    row = conn.execute(queries()["data_period"], {"tz": REPORTING_TIMEZONE}).fetchone()
    if row is None or row[0] is None:
        return None
    return row[0], row[1]


def cities(conn: Connection) -> list[str]:
    """Every city of the city dimension."""
    return [str(row[0]) for row in conn.execute(queries()["cities"]).fetchall()]


def fetch(conn: Connection, filters: Filters) -> DashboardData:
    """Run the dashboard queries for ``filters``."""
    sql = queries()
    params = parameters(filters)
    kpi_row = conn.execute(sql["kpis"], params).fetchone()
    if kpi_row is None:
        raise RuntimeError("the kpis query returned no row")
    kpis = Kpis(
        orders_created=int(kpi_row[0]),
        cancelled_orders=int(kpi_row[1]),
        delivered_orders=int(kpi_row[2]),
        late_orders=int(kpi_row[3]),
        delivery_seconds=Decimal(kpi_row[4]),
    )
    trend = tuple(
        TrendPoint(row[0], str(row[1]), Decimal(row[2]), int(row[3]))
        for row in conn.execute(sql["delivery_time_trend"], params).fetchall()
    )
    late = tuple(
        CityLateRate(str(row[0]), int(row[1]), int(row[2]))
        for row in conn.execute(sql["late_rate_by_city"], params).fetchall()
    )
    hours = tuple(
        HourCount(int(row[0]), int(row[1]))
        for row in conn.execute(sql["peak_hours"], params).fetchall()
    )
    drivers = tuple(
        DriverRow(
            str(row[0]),
            str(row[1]),
            int(row[2]),
            Decimal(row[3]),
            None if row[4] is None else Decimal(row[4]),
        )
        for row in conn.execute(sql["top_drivers"], params).fetchall()
    )
    return DashboardData(filters, kpis, trend, late, hours, drivers)


def check(conn: Connection) -> list[tuple[str, int, float]]:
    """(query, rows, seconds) for every dashboard query, with the default filters."""
    sql = queries()
    period = data_period(conn)
    if period is None:
        raise RuntimeError("analytics.orders is empty; run `make load` first")
    params = parameters(default_filters(period, cities(conn)))
    timings: list[tuple[str, int, float]] = []
    for name in QUERY_NAMES:
        started = time.perf_counter()
        if name == "cities":
            rows = conn.execute(sql[name]).fetchall()
        else:
            rows = conn.execute(sql[name], params).fetchall()
        timings.append((name, len(rows), time.perf_counter() - started))
    return timings


def main(argv: Sequence[str] | None = None) -> int:
    """`make dashboard-check`: run every query as the read-only role; returns the exit code."""
    del argv
    configure_logging()
    started = time.perf_counter()
    try:
        with connect() as conn:
            timings = check(conn)
    except (ConfigError, RuntimeError, psycopg.Error) as exc:
        logger.error("%s", exc)
        return 1
    for name, rows, seconds in timings:
        print(f"{name:22} {rows:6d} rows {seconds * 1000:9.1f} ms")
    logger.info(
        "dashboard check passed as %s: %d queries in %.2f s",
        DASHBOARD_ROLE,
        len(timings),
        time.perf_counter() - started,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
