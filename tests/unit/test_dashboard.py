"""Unit tests (no database, no browser) of the dashboard: its SQL file, helpers and layout."""

import re
from datetime import date
from decimal import Decimal

import pytest
from streamlit.testing.v1 import AppTest

from src.config import ConfigError
from src.dashboard import data as dashboard
from src.dashboard.data import (
    QUERY_NAMES,
    CityLateRate,
    FilterError,
    Filters,
    HourCount,
    Kpis,
    default_filters,
    late_rate_series,
    parameters,
    peak_hour_series,
    queries,
    ratio,
    validate_filters,
)
from tests.unit.dashboard_samples import FILTERS

pytestmark = pytest.mark.unit

PERIOD = (date(2026, 9, 10), date(2026, 9, 30))
CITIES = ["Bengaluru", "Delhi", "Mumbai"]
SCHEMA_REFERENCE = re.compile(r"\b(raw|staging|analytics|audit|public|pg_catalog)\.\w+")
PARAMETER = re.compile(r"%\((\w+)\)s")
# The first render imports the chart libraries; the 3 s default is too short on a cold start.
APP_TIMEOUT = 60
WRITE_KEYWORDS = re.compile(r"\b(INSERT|UPDATE|DELETE|MERGE|CREATE|ALTER|DROP|TRUNCATE|GRANT)\b")


# ----------------------------------------------------------------- the SQL file ---


def test_dashboard_sql_has_exactly_the_expected_queries() -> None:
    assert tuple(queries()) == QUERY_NAMES


@pytest.mark.parametrize("name", QUERY_NAMES)
def test_dashboard_query_is_select_only_on_analytics(name: str) -> None:
    text = queries()[name]
    statement = "\n".join(
        line for line in text.splitlines() if not line.lstrip().startswith("--")
    ).lstrip()
    assert statement.upper().startswith(("SELECT", "WITH")), name
    assert ";" not in statement  # one statement (comments may contain any text)
    assert not WRITE_KEYWORDS.search(statement.upper()), name
    schemas = {match.group(1) for match in SCHEMA_REFERENCE.finditer(text)}
    assert schemas == {"analytics"}, (name, schemas)


@pytest.mark.parametrize("name", QUERY_NAMES)
def test_dashboard_query_uses_only_supplied_parameters(name: str) -> None:
    used = set(PARAMETER.findall(queries()[name]))
    assert used <= set(parameters(FILTERS)), used


# ---------------------------------------------------------------------- helpers ---


def test_ratio_rounds_half_up_and_returns_none_for_zero_denominator() -> None:
    assert ratio(1, 3, 4) == Decimal("0.3333")
    assert ratio(1, 8, 2) == Decimal("0.13")  # 0.125 rounds half up
    assert ratio(5, 0, 4) is None


def test_kpi_rates_come_from_counts() -> None:
    kpis = Kpis(40, 4, 32, 8, Decimal(57600))
    assert (kpis.late_rate, kpis.cancelled_rate) == (Decimal("0.2500"), Decimal("0.1000"))
    assert kpis.avg_delivery_minutes == Decimal("30.00")
    nothing = Kpis(0, 0, 0, 0, Decimal(0))
    assert (nothing.late_rate, nothing.cancelled_rate, nothing.avg_delivery_minutes) == (
        None,
        None,
        None,
    )


def test_default_filters_cover_the_period_and_every_city() -> None:
    assert default_filters(PERIOD, CITIES) == Filters(PERIOD[0], PERIOD[1], tuple(CITIES))


@pytest.mark.parametrize(
    ("filters", "message"),
    [
        (Filters(date(2026, 9, 20), date(2026, 9, 12), ("Delhi",)), "after end date"),
        (Filters(date(2026, 10, 2), date(2026, 10, 5), ("Delhi",)), "outside the loaded data"),
        (Filters(date(2026, 8, 1), date(2026, 9, 9), ("Delhi",)), "outside the loaded data"),
        (Filters(date(2026, 9, 12), date(2026, 9, 14), ("Atlantis",)), "unknown cities"),
    ],
)
def test_invalid_filters_are_rejected(filters: Filters, message: str) -> None:
    with pytest.raises(FilterError, match=message):
        validate_filters(filters, PERIOD, CITIES)


def test_valid_partial_range_is_accepted() -> None:
    validate_filters(Filters(date(2026, 9, 9), date(2026, 9, 12), ("Delhi",)), PERIOD, CITIES)


def test_peak_hours_fill_every_hour_and_late_rates_skip_cities_without_deliveries() -> None:
    hours = peak_hour_series([HourCount(0, 3), HourCount(23, 1)])
    assert hours["hour"] == list(range(24))
    assert hours["orders"][0] == 3 and hours["orders"][23] == 1 and sum(hours["orders"]) == 4
    rates = late_rate_series([CityLateRate("Delhi", 4, 1), CityLateRate("Mumbai", 0, 0)])
    assert rates == {"city": ["Delhi"], "late rate %": [25.0]}


def test_dashboard_connects_as_the_read_only_role(monkeypatch: pytest.MonkeyPatch) -> None:
    requested: list[str] = []

    def capture(role: str, **_: object) -> None:
        requested.append(role)
        raise ConfigError("stop before connecting")

    monkeypatch.setattr(dashboard, "load_database_settings", capture)
    with pytest.raises(ConfigError):
        dashboard.connect()
    assert requested == ["readonly"]


# ----------------------------------------------------------------------- layout ---


def _render_sample() -> None:
    from src.dashboard.app import render
    from tests.unit.dashboard_samples import sample_data

    render(sample_data())


def _render_empty() -> None:
    from src.dashboard.app import render
    from tests.unit.dashboard_samples import empty_data

    render(empty_data())


def test_layout_shows_the_kpis_and_the_four_views() -> None:
    app = AppTest.from_function(_render_sample).run(timeout=APP_TIMEOUT)
    assert not app.exception
    metrics = {metric.label: metric.value for metric in app.metric}
    assert metrics == {
        "Orders created": "40",
        "Orders delivered": "32",
        "Late-delivery rate": "25.00 %",
        "Avg delivery time": "30.00 min",
        "Cancellation rate": "10.00 %",
    }
    headings = [subheader.value for subheader in app.subheader]
    assert headings == [
        "Delivery time trend",
        "Late-delivery rate by city",
        "Peak hours",
        "Top drivers",
    ]
    charts = app.get("vega_lite_chart")
    assert len(charts) == 3  # trend, late rate, peak hours
    assert len(app.dataframe) == 1  # top drivers
    assert not app.info


def test_layout_explains_an_empty_selection_instead_of_failing() -> None:
    app = AppTest.from_function(_render_empty).run(timeout=APP_TIMEOUT)
    assert not app.exception
    assert [info.value for info in app.info] == [
        "No orders match these filters. Widen the date range or select more cities."
    ]
    assert not app.metric and not app.subheader
