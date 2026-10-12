"""DeliveryPulse dashboard (Streamlit). Launch with `make dashboard`.

Shows, for a reporting-date range and a set of cities: headline KPIs, the delivery-time trend,
the late-delivery rate by city, peak hours and the fastest drivers. Data comes from
``src/dashboard/data.py`` as the read-only role; this module only lays it out, so ``render``
can be tested with handmade data.
"""

from datetime import date
from decimal import Decimal

import psycopg
import streamlit as st

from src.config import ConfigError
from src.contract import REPORTING_TIMEZONE
from src.dashboard.data import (
    MIN_DELIVERIES,
    DashboardData,
    FilterError,
    Filters,
    cities,
    connect,
    data_period,
    default_filters,
    driver_table,
    fetch,
    late_rate_series,
    peak_hour_series,
    percent,
    trend_series,
    validate_filters,
)

TITLE = "DeliveryPulse: last-mile delivery operations"
NO_DATA = "No orders match these filters. Widen the date range or select more cities."
CACHE_SECONDS = 60


def _number(value: int) -> str:
    return f"{value:,}"


def _rate(value: Decimal | None) -> str:
    shown = percent(value)
    return "n/a" if shown is None else f"{shown:.2f} %"


def render(data: DashboardData) -> None:
    """Lay out one set of results."""
    filters = data.filters
    st.title(TITLE)
    st.caption(
        f"Synthetic data · {filters.start} to {filters.end} · reporting timezone "
        f"{REPORTING_TIMEZONE} · {len(filters.cities)} cities"
    )
    if data.is_empty:
        st.info(NO_DATA)
        return

    kpis = data.kpis
    created, delivered, late, minutes, cancelled = st.columns(5)
    created.metric("Orders created", _number(kpis.orders_created))
    delivered.metric("Orders delivered", _number(kpis.delivered_orders))
    late.metric("Late-delivery rate", _rate(kpis.late_rate))
    avg = kpis.avg_delivery_minutes
    minutes.metric("Avg delivery time", "n/a" if avg is None else f"{avg} min")
    cancelled.metric("Cancellation rate", _rate(kpis.cancelled_rate))

    st.subheader("Delivery time trend")
    st.caption("Average pickup-to-delivery minutes per reporting day (delivery date), by city.")
    st.line_chart(trend_series(data.trend), x="date", y="avg delivery minutes", color="city")

    left, right = st.columns(2)
    with left:
        st.subheader("Late-delivery rate by city")
        st.caption("Late delivered / delivered orders; cancelled orders excluded.")
        st.bar_chart(late_rate_series(data.late_by_city), x="city", y="late rate %")
    with right:
        st.subheader("Peak hours")
        st.caption(f"Orders created per hour of day ({REPORTING_TIMEZONE}).")
        st.bar_chart(peak_hour_series(data.peak_hours), x="hour", y="orders")

    st.subheader("Top drivers")
    st.caption(
        f"Fastest average delivery, at least {MIN_DELIVERIES} deliveries in the range; "
        "utilisation = busy / online minutes over the range."
    )
    st.dataframe(driver_table(data.top_drivers), hide_index=True)


@st.cache_data(ttl=CACHE_SECONDS, show_spinner=False)
def _options() -> tuple[tuple[date, date] | None, list[str]]:
    with connect() as conn:
        return data_period(conn), cities(conn)


@st.cache_data(ttl=CACHE_SECONDS, show_spinner="Loading…")
def _load(start: date, end: date, selected: tuple[str, ...]) -> DashboardData:
    with connect() as conn:
        return fetch(conn, Filters(start, end, selected))


def _filters(period: tuple[date, date], all_cities: list[str]) -> Filters:
    defaults = default_filters(period, all_cities)
    st.sidebar.header("Filters")
    picked = st.sidebar.date_input(
        "Reporting dates",
        value=(defaults.start, defaults.end),
        min_value=period[0],
        max_value=period[1],
    )
    if isinstance(picked, tuple) and len(picked) == 2:
        start, end = picked
    else:  # a single date while the range is being picked
        start = end = picked if isinstance(picked, date) else defaults.start
    selected = st.sidebar.multiselect("Cities", all_cities, default=all_cities)
    return Filters(start, end, tuple(selected))


def main() -> None:
    """The Streamlit page: connect as the read-only role, read the filters, render."""
    st.set_page_config(page_title="DeliveryPulse dashboard", layout="wide")
    try:
        period, all_cities = _options()
    except (ConfigError, psycopg.Error):  # never show connection details in the page
        st.error("Cannot reach the database. Is PostgreSQL running (`make up`)?")
        return
    if period is None:
        st.warning("No data loaded yet. Run `make generate` and `make load`.")
        return
    filters = _filters(period, all_cities)
    try:
        validate_filters(filters, period, all_cities)
    except FilterError as exc:
        st.warning(str(exc))
        return
    render(_load(filters.start, filters.end, filters.cities))


if __name__ == "__main__":
    main()
