"""Unit tests (no database): GPS-speed logic, the late flag, disposition counting, and a
per-check validation of every quality check.

The quality checks are SQL, so their behaviour is tested against PostgreSQL in
tests/integration/test_quality.py (one failing case per stored result). Here each check's query
is validated without a database: its parameters, its form, its status rule, and that its
failing case exists.
"""

import math
import re

import pytest

from src.contract import (
    ACCEPTED,
    DISPOSITIONS,
    DUPLICATE,
    GPS_IMPOSSIBLE_SPEED,
    GPS_NON_POSITIVE_INTERVAL,
    MAX_PLAUSIBLE_SPEED_KMH,
    PENDING,
    REJECTED,
    SOURCE_DELIVERY_EVENTS,
    SOURCE_DRIVERS,
    SOURCE_FILES,
    SOURCE_ORDERS,
    SOURCE_SHIFT_EVENTS,
    STALE_IGNORED,
)
from src.quality.checks import (
    BLOCKING,
    CHECKS,
    FAILED,
    KIND_COUNT,
    KIND_HOURS,
    KIND_SHARE,
    PASSED,
    WARNED,
    Check,
    QualityConfig,
    Scope,
    evaluate_result,
    query_parameters,
)
from src.transformations.gps import EARTH_RADIUS_KM, Ping, check_ping, haversine_km
from src.transformations.outputs import batch_summary
from src.transformations.rules import validate_order
from tests.integration.test_quality import FAILING_CASES
from tests.unit.transform_helpers import (
    DELIVERED_ORDER,
    DRIVER,
    make_batch,
    order,
    ping,
    run,
    shift,
)

pytestmark = pytest.mark.unit

ONE_DEGREE_KM = 2 * math.pi * EARTH_RADIUS_KM / 360  # 111.195 km along a meridian


# ---------------------------------------------------------------------- haversine ---


def test_haversine_one_degree_of_latitude() -> None:
    assert haversine_km(0.0, 0.0, 1.0, 0.0) == pytest.approx(ONE_DEGREE_KM, rel=1e-12)
    assert haversine_km(28.6, 77.2, 29.6, 77.2) == pytest.approx(ONE_DEGREE_KM, rel=1e-12)


def test_haversine_zero_distance_and_symmetry() -> None:
    assert haversine_km(28.6, 77.2, 28.6, 77.2) == 0.0
    there = haversine_km(12.97, 77.59, 19.07, 72.87)  # Bengaluru -> Mumbai centres
    back = haversine_km(19.07, 72.87, 12.97, 77.59)
    assert there == back
    assert 840 < there < 850


# ---------------------------------------------------------------------- check_ping ---


def _pings_at_speed(kmh: float, seconds: int = 60) -> tuple[Ping, Ping]:
    """Two pings ``seconds`` apart along a meridian, moving at ``kmh``."""
    degrees = kmh * seconds / 3600 / ONE_DEGREE_KM
    return Ping(1_000, "EVT-1", 0.0, 0.0), Ping(1_000 + seconds, "EVT-2", degrees, 0.0)


def test_check_ping_passes_at_exactly_the_threshold() -> None:
    previous, current = _pings_at_speed(MAX_PLAUSIBLE_SPEED_KMH)
    # The threshold equal to the measured speed: "above" is required to reject.
    measured = haversine_km(previous.lat, previous.lon, current.lat, current.lon) / 60 * 3600
    assert measured == pytest.approx(120.0, rel=1e-9)
    assert check_ping(previous, current, measured) is None


def test_check_ping_rejects_just_above_and_passes_just_below_120_kmh() -> None:
    assert check_ping(*_pings_at_speed(120.01), MAX_PLAUSIBLE_SPEED_KMH) == GPS_IMPOSSIBLE_SPEED
    assert check_ping(*_pings_at_speed(119.99), MAX_PLAUSIBLE_SPEED_KMH) is None


@pytest.mark.parametrize("seconds", [0, -60])
def test_check_ping_zero_or_negative_interval(seconds: int) -> None:
    previous = Ping(1_000, "EVT-1", 28.6, 77.2)
    current = Ping(1_000 + seconds, "EVT-2", 28.6, 77.2)
    assert check_ping(previous, current, MAX_PLAUSIBLE_SPEED_KMH) == GPS_NON_POSITIVE_INTERVAL


def test_check_ping_without_predecessor_passes() -> None:
    far_away = Ping(1_000, "EVT-1", 89.0, 179.0)
    assert check_ping(None, far_away, MAX_PLAUSIBLE_SPEED_KMH) is None


# ---------------------------------------------------------------------- late flag ---


def test_delivery_at_exactly_the_promised_time_is_on_time() -> None:
    _, exact = validate_order(order(promised_ts=DELIVERED_ORDER["delivery_ts"]))
    _, one_second_late = validate_order(order(promised_ts="2026-09-10T10:39:59Z"))
    assert exact is not None and one_second_late is not None
    assert exact.is_late() is False
    assert one_second_late.is_late() is True


def test_late_flag_is_none_for_cancelled_and_open_orders() -> None:
    _, cancelled = validate_order(
        order(status="cancelled", driver_id="", pickup_ts="", delivery_ts="")
    )
    _, open_order = validate_order(order(status="picked_up", delivery_ts=""))
    assert cancelled is not None and open_order is not None
    assert cancelled.is_late() is None and open_order.is_late() is None
    assert cancelled.delivery_minutes() is None and open_order.delivery_minutes() is None


# ---------------------------------------------------------- disposition counting ---


def test_disposition_counts_and_the_reconciliation_identity() -> None:
    newer = order(source_updated_at="2026-09-10T10:41:00Z")
    first = make_batch(
        1,
        drivers=[DRIVER],
        orders=[DELIVERED_ORDER, DELIVERED_ORDER, order(order_id="ORD-BAD", status="lost")],
        events=[ping("EVT-1", "2026-09-10T10:20:00Z", "28.6", "77.2", order_id="ORD-0000009")],
        shifts=[shift("SHF-1", "2026-09-10T09:00:00Z")],
    )
    second = make_batch(
        2,
        orders=[newer, order(order_id="ORD-0000009"), DELIVERED_ORDER],  # DELIVERED_ORDER is stale
    )
    (one, two), _ = run([first, second])

    for result in (one, two):
        for source, _ in SOURCE_FILES:
            counts = result.counts(source)
            assert list(counts) == list(DISPOSITIONS)  # every key, zeros included
        summary = batch_summary(result)["sources"]
        for source, numbers in summary.items():
            assert numbers["rows_input"] == sum(numbers["dispositions"].values()), source

    assert one.counts(SOURCE_ORDERS) == {
        ACCEPTED: 1,
        REJECTED: 1,
        DUPLICATE: 1,
        STALE_IGNORED: 0,
        PENDING: 0,
    }
    assert one.counts(SOURCE_DELIVERY_EVENTS)[PENDING] == 1
    assert one.counts(SOURCE_DRIVERS)[ACCEPTED] == 1
    assert one.counts(SOURCE_SHIFT_EVENTS)[ACCEPTED] == 1
    assert two.counts(SOURCE_ORDERS)[STALE_IGNORED] == 1

    # The pending event resolves in batch 2: counted as a resolution, not as batch-2 input.
    events_two = batch_summary(two)["sources"][SOURCE_DELIVERY_EVENTS]
    assert events_two["rows_input"] == 0
    assert sum(events_two["dispositions"].values()) == 0
    assert (events_two["pending_resolved"], events_two["pending_remaining"]) == (1, 0)


# ----------------------------------------------------- every quality check, no SQL ---

SCOPE = Scope("dataset", ("batch",), ())
SUPPLIED = set(query_parameters(SCOPE, QualityConfig()))
PARAMETER = re.compile(r"%\((\w+)\)s")


@pytest.mark.parametrize("check", CHECKS, ids=[c.name for c in CHECKS])
def test_check_query_uses_only_supplied_parameters(check: Check) -> None:
    # Some whole-table checks take no parameters at all; every one that is used must exist.
    used = set(PARAMETER.findall(check.query))
    assert used <= SUPPLIED, used - SUPPLIED


@pytest.mark.parametrize("check", CHECKS, ids=[c.name for c in CHECKS])
def test_check_query_is_one_statement(check: Check) -> None:
    assert ";" not in check.query
    assert check.query.lstrip().upper().startswith(("WITH", "SELECT")), check.name
    # A bare % would be read as a placeholder by the driver.
    assert "%" not in PARAMETER.sub("", check.query), check.name


@pytest.mark.parametrize("check", CHECKS, ids=[c.name for c in CHECKS])
def test_check_has_a_failing_case_in_the_integration_suite(check: Check) -> None:
    missing = [name for name in check.result_names() if name not in FAILING_CASES]
    assert missing == []


def _just_above(kind: str, threshold: float) -> float:
    return {KIND_COUNT: threshold + 1, KIND_SHARE: threshold + 0.001, KIND_HOURS: threshold + 0.01}[
        kind
    ]


@pytest.mark.parametrize("check", CHECKS, ids=[c.name for c in CHECKS])
def test_check_status_rule_at_and_just_above_the_threshold(check: Check) -> None:
    config = QualityConfig()
    for instance in check.instances:
        threshold = config.threshold(check, instance)
        rows = config.share_min_rows()  # a sample large enough for the share rule
        at = evaluate_result(check.kind, check.severity, threshold, threshold, rows, rows)
        above = evaluate_result(
            check.kind, check.severity, _just_above(check.kind, threshold), threshold, rows, rows
        )
        assert at == PASSED, check.result_name(instance)
        assert above == (FAILED if check.severity == BLOCKING else WARNED)
