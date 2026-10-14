"""Data-quality checks as code (catalogue and rules: docs/data_contracts.md, section 11).

Every check has a name, the layer and table it runs on, a severity and a threshold:

* **blocking**: a failure means a pipeline bug or corrupt state. During a load the batch is
  not marked succeeded (see ``src/database/load_postgres.py``).
* **warning**: recorded, never blocks. Every raw-layer detection check is a warning: raw
  defects are expected, and the pipeline quarantines or handles them.

Threshold kinds: ``count`` (failing rows), ``share`` (failing rows / rows checked) and
``hours`` (a measured lag). A check passes while its observed value is at most the threshold.
No check repeats a database constraint, because such a check could never fail.
"""

import logging
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from typing import Any, LiteralString

import psycopg

from src.contract import (
    CITY_STATES,
    DISPOSITIONS,
    LONG_GAP_MIN_SECONDS,
    MAX_PLAUSIBLE_SPEED_KMH,
    ORDER_STATUSES,
    REASON_CODES,
    REPORTING_TIMEZONE,
    VEHICLE_TYPES,
)
from src.quality import sql_integrity, sql_raw

logger = logging.getLogger("deliverypulse.quality")

BLOCKING = "blocking"
WARNING = "warning"
PASSED = "passed"
WARNED = "warned"
FAILED = "failed"
KIND_COUNT = "count"
KIND_SHARE = "share"
KIND_HOURS = "hours"
LAYER_RAW = "raw"
LAYER_STAGING = "staging"
LAYER_AUDIT = "audit"
LAYER_ANALYTICS = "analytics"
TRIGGER_LOAD = "load"
TRIGGER_MANUAL = "manual"
DEFAULT_RAW_SHARE = 0.02
DEFAULT_FRESHNESS_HOURS = 6.0
DEFAULT_MIN_SHARE_ROWS = 50
MIN_SHARE_ROWS = "min_share_rows"
RAW_META_COLUMNS = ("raw_id", "batch_id", "source_file", "source_row_number", "ingested_at")
SHARE_PLACES = 6


@dataclass(frozen=True)
class Check:
    """One data-quality check. ``instances`` are the sources its query reports ("" = one)."""

    name: str
    layer: str
    table: str
    severity: str
    kind: str
    description: str
    query: LiteralString
    instances: tuple[str, ...] = ("",)
    # None: QualityConfig.raw_share_threshold (raw detection checks).
    default_threshold: float | None = None

    def result_name(self, instance: str) -> str:
        """Stored check name: ``name`` or ``name.instance``."""
        return self.name if instance == "" else f"{self.name}.{instance}"

    def result_names(self) -> list[str]:
        """Every stored name this check produces."""
        return [self.result_name(instance) for instance in self.instances]


ALL_SOURCES = ("drivers", "orders", "delivery_events", "shift_events")

CHECKS: tuple[Check, ...] = (
    # ------------------------------------------------ raw-layer detection (warning) ---
    Check(
        "raw_duplicate_rows",
        LAYER_RAW,
        "raw.*_raw",
        WARNING,
        KIND_SHARE,
        "The same full row more than once in a batch (extra copies counted)",
        sql_raw.DUPLICATE_ROWS,
        ALL_SOURCES,
    ),
    Check(
        "raw_missing_required",
        LAYER_RAW,
        "raw.*_raw",
        WARNING,
        KIND_SHARE,
        "A required contract field is empty",
        sql_raw.MISSING_REQUIRED,
        ALL_SOURCES,
    ),
    Check(
        "raw_unparseable_timestamps",
        LAYER_RAW,
        "raw.*_raw",
        WARNING,
        KIND_SHARE,
        "A non-empty timestamp (or joined_date) is not a valid contract value",
        sql_raw.UNPARSEABLE_TIMESTAMPS,
        ALL_SOURCES,
    ),
    Check(
        "raw_invalid_status",
        LAYER_RAW,
        "raw.orders_raw",
        WARNING,
        KIND_SHARE,
        "Status not one of the five statuses, even after trim and lowercase",
        sql_raw.INVALID_STATUS,
        ("orders",),
    ),
    Check(
        "raw_status_not_canonical",
        LAYER_RAW,
        "raw.orders_raw",
        WARNING,
        KIND_SHARE,
        "Status valid only after trim and lowercase (accepted after normalisation)",
        sql_raw.STATUS_NOT_CANONICAL,
        ("orders",),
    ),
    Check(
        "raw_numeric_out_of_range",
        LAYER_RAW,
        "raw.orders_raw",
        WARNING,
        KIND_SHARE,
        "distance_km or fare_amount not numeric, or not greater than 0",
        sql_raw.NUMERIC_OUT_OF_RANGE,
        ("orders",),
    ),
    Check(
        "raw_driver_invalid_values",
        LAYER_RAW,
        "raw.drivers_raw",
        WARNING,
        KIND_SHARE,
        "Vehicle type not in the list, or rating not a number from 1 to 5",
        sql_raw.DRIVER_INVALID_VALUES,
        ("drivers",),
    ),
    Check(
        "raw_delivery_before_pickup",
        LAYER_RAW,
        "raw.orders_raw",
        WARNING,
        KIND_SHARE,
        "Both timestamps valid and delivery_ts earlier than pickup_ts",
        sql_raw.DELIVERY_BEFORE_PICKUP,
        ("orders",),
    ),
    Check(
        "raw_resent_unchanged",
        LAYER_RAW,
        "raw.orders_raw",
        WARNING,
        KIND_SHARE,
        "Order row identical to an order row of an earlier batch",
        sql_raw.RESENT_UNCHANGED,
        ("orders",),
    ),
    Check(
        "raw_stale_versions",
        LAYER_RAW,
        "raw.orders_raw",
        WARNING,
        KIND_SHARE,
        "source_updated_at older than the newest version of the order in earlier batches",
        sql_raw.STALE_VERSIONS,
        ("orders",),
    ),
    Check(
        "raw_version_updates",
        LAYER_RAW,
        "raw.orders_raw",
        WARNING,
        KIND_SHARE,
        "Newer version of an order seen in an earlier batch (accepted as an update)",
        sql_raw.VERSION_UPDATES,
        ("orders",),
        default_threshold=0.05,
    ),
    Check(
        "raw_replayed_from_earlier_batch",
        LAYER_RAW,
        "raw.events_raw, raw.shift_events_raw",
        WARNING,
        KIND_SHARE,
        "event_id already present in an earlier batch",
        sql_raw.REPLAYED_FROM_EARLIER_BATCH,
        ("delivery_events", "shift_events"),
    ),
    Check(
        "raw_gps_missing_coordinates",
        LAYER_RAW,
        "raw.events_raw",
        WARNING,
        KIND_SHARE,
        "gps_ping with lat or lon empty",
        sql_raw.GPS_MISSING_COORDINATES,
        ("delivery_events",),
    ),
    Check(
        "raw_gps_invalid_coordinates",
        LAYER_RAW,
        "raw.events_raw",
        WARNING,
        KIND_SHARE,
        "lat or lon not numeric or out of range",
        sql_raw.GPS_INVALID_COORDINATES,
        ("delivery_events",),
    ),
    Check(
        "raw_gps_impossible_speed",
        LAYER_RAW,
        "raw.events_raw",
        WARNING,
        KIND_SHARE,
        "Impossible speed from the driver's previous valid ping, unless that ping was itself "
        "reached at an impossible speed (the return leg after a jump is not counted)",
        sql_raw.GPS_IMPOSSIBLE_SPEED,
        ("delivery_events",),
    ),
    Check(
        "raw_gps_duplicate_timestamps",
        LAYER_RAW,
        "raw.events_raw",
        WARNING,
        KIND_SHARE,
        "Two different valid pings of one driver with the same event_ts",
        sql_raw.GPS_DUPLICATE_TIMESTAMPS,
        ("delivery_events",),
    ),
    Check(
        "raw_events_out_of_file_order",
        LAYER_RAW,
        "raw.events_raw",
        WARNING,
        KIND_SHARE,
        "Within one order, a GPS ping's event_ts is earlier than the previous ping's in file "
        "row order",
        sql_raw.EVENTS_OUT_OF_FILE_ORDER,
        ("delivery_events",),
    ),
    Check(
        "raw_gps_long_gaps",
        LAYER_RAW,
        "raw.events_raw",
        WARNING,
        KIND_SHARE,
        "Consecutive pings of one order at least long_gap_seconds apart at a plausible speed",
        sql_raw.GPS_LONG_GAPS,
        ("delivery_events",),
    ),
    Check(
        "raw_late_events",
        LAYER_RAW,
        "raw.events_raw",
        WARNING,
        KIND_SHARE,
        "event_ts before the batch window (first arrival of the event_id)",
        sql_raw.LATE_EVENTS,
        ("delivery_events",),
    ),
    Check(
        "raw_orphans",
        LAYER_RAW,
        "raw.events_raw, raw.shift_events_raw",
        WARNING,
        KIND_SHARE,
        "Parent order or driver row not received in this or an earlier batch",
        sql_raw.ORPHANS,
        ("delivery_events", "shift_events"),
    ),
    Check(
        "raw_parent_after_children",
        LAYER_RAW,
        "raw.orders_raw, raw.drivers_raw",
        WARNING,
        KIND_SHARE,
        "First row of an order or driver arriving after its events or shifts",
        sql_raw.PARENT_AFTER_CHILDREN,
        ("drivers", "orders"),
    ),
    # ------------------------------------------- staging, audit, analytics integrity ---
    Check(
        "reconciliation_identity",
        LAYER_AUDIT,
        "audit.record_dispositions, audit.pipeline_runs",
        BLOCKING,
        KIND_COUNT,
        "Every raw row has exactly one disposition; succeeded counters equal the dispositions "
        "and input = accepted + rejected + duplicate + stale_ignored + pending",
        sql_integrity.RECONCILIATION_IDENTITY,
        ALL_SOURCES,
        default_threshold=0,
    ),
    Check(
        "disposition_values",
        LAYER_AUDIT,
        "audit.record_dispositions, staging.rejected_records",
        BLOCKING,
        KIND_COUNT,
        "Dispositions and reasons are documented values; every rejection has a reason",
        sql_integrity.DISPOSITION_VALUES,
        default_threshold=0,
    ),
    Check(
        "rejected_quarantined",
        LAYER_STAGING,
        "staging.rejected_records",
        BLOCKING,
        KIND_COUNT,
        "Rejected rows per batch and source equal the rejected dispositions",
        sql_integrity.REJECTED_QUARANTINED,
        default_threshold=0,
    ),
    Check(
        "accepted_loaded",
        LAYER_ANALYTICS,
        "analytics.drivers, orders, delivery_events, driver_shift_events",
        BLOCKING,
        KIND_COUNT,
        "Every accepted record of the batch exists in its analytics table",
        sql_integrity.ACCEPTED_LOADED,
        default_threshold=0,
    ),
    Check(
        "pending_parent_missing",
        LAYER_STAGING,
        "staging.pending_events",
        BLOCKING,
        KIND_COUNT,
        "No pending record whose missing parent already exists",
        sql_integrity.PENDING_PARENT_MISSING,
        default_threshold=0,
    ),
    Check(
        "orders_required_by_status",
        LAYER_ANALYTICS,
        "analytics.orders",
        BLOCKING,
        KIND_COUNT,
        "driver_id set for assigned/picked_up/delivered; pickup_ts set for picked_up/delivered",
        sql_integrity.ORDERS_REQUIRED_BY_STATUS,
        default_threshold=0,
    ),
    Check(
        "orders_positive_amounts",
        LAYER_ANALYTICS,
        "analytics.orders",
        BLOCKING,
        KIND_COUNT,
        "distance_km and fare_amount greater than 0 (the schema allows 0)",
        sql_integrity.ORDERS_POSITIVE_AMOUNTS,
        default_threshold=0,
    ),
    Check(
        "gps_ping_unique",
        LAYER_ANALYTICS,
        "analytics.delivery_events",
        BLOCKING,
        KIND_COUNT,
        "At most one gps_ping per driver and event_ts",
        sql_integrity.GPS_PING_UNIQUE,
        default_threshold=0,
    ),
    Check(
        "metrics_match_facts",
        LAYER_ANALYTICS,
        "analytics.daily_delivery_metrics",
        BLOCKING,
        KIND_COUNT,
        "Daily counts per date and city equal direct counts from analytics.orders",
        sql_integrity.METRICS_MATCH_FACTS,
        default_threshold=0,
    ),
    Check(
        "no_future_timestamps",
        LAYER_ANALYTICS,
        "analytics.orders, delivery_events, driver_shift_events",
        BLOCKING,
        KIND_COUNT,
        "No accepted timestamp at or after its batch's window_end (promised_ts excluded)",
        sql_integrity.NO_FUTURE_TIMESTAMPS,
        default_threshold=0,
    ),
    Check(
        "freshness_lag",
        LAYER_ANALYTICS,
        "analytics.delivery_events",
        WARNING,
        KIND_HOURS,
        "Hours from the newest accepted event of the latest batch to its window_end",
        sql_integrity.FRESHNESS_LAG,
        default_threshold=DEFAULT_FRESHNESS_HOURS,
    ),
    Check(
        "known_cities",
        LAYER_ANALYTICS,
        "analytics.orders, analytics.drivers",
        WARNING,
        KIND_COUNT,
        "City not in the reference list (CITY_STATES)",
        sql_integrity.KNOWN_CITIES,
        default_threshold=0,
    ),
    Check(
        "utilisation_above_100",
        LAYER_ANALYTICS,
        "analytics.driver_performance",
        WARNING,
        KIND_COUNT,
        "Driver-days with utilisation above 100 % (busy outside paired online sessions)",
        sql_integrity.UTILISATION_ABOVE_100,
        default_threshold=0,
    ),
    Check(
        "gps_speed_in_analytics",
        LAYER_ANALYTICS,
        "analytics.delivery_events",
        WARNING,
        KIND_COUNT,
        "Consecutive accepted pings of a driver above the speed limit",
        sql_integrity.GPS_SPEED_IN_ANALYTICS,
        default_threshold=0,
    ),
)
CHECKS_BY_NAME: dict[str, Check] = {check.name: check for check in CHECKS}


def result_names() -> list[str]:
    """Every stored check name, in registry order."""
    return [name for check in CHECKS for name in check.result_names()]


class QualityConfigError(ValueError):
    """An invalid threshold override."""


@dataclass(frozen=True)
class QualityConfig:
    """Configurable thresholds. ``thresholds`` overrides by check name or ``name.source``."""

    raw_share_threshold: float = DEFAULT_RAW_SHARE
    max_speed_kmh: float = MAX_PLAUSIBLE_SPEED_KMH
    long_gap_seconds: int = LONG_GAP_MIN_SECONDS
    # A share check with fewer rows checked than this passes (counts and samples still stored).
    min_share_rows: int = DEFAULT_MIN_SHARE_ROWS
    thresholds: Mapping[str, float] = field(default_factory=dict)

    def share_min_rows(self) -> int:
        """Minimum rows checked for a share threshold to apply (override: ``min_share_rows``)."""
        return int(self.thresholds.get(MIN_SHARE_ROWS, self.min_share_rows))

    def threshold(self, check: Check, instance: str) -> float:
        """Threshold of one stored check: override, else the check's or the raw default."""
        full = check.result_name(instance)
        if full in self.thresholds:
            return self.thresholds[full]
        if check.name in self.thresholds:
            return self.thresholds[check.name]
        if check.default_threshold is None:
            return self.raw_share_threshold
        return check.default_threshold


def parse_thresholds(items: Sequence[str]) -> dict[str, float]:
    """Parse ``name=value`` overrides.

    Names are checks, ``check.source`` names, or ``min_share_rows`` (a whole number).
    """
    known = {check.name for check in CHECKS} | set(result_names()) | {MIN_SHARE_ROWS}
    parsed: dict[str, float] = {}
    for item in items:
        name, separator, value = item.partition("=")
        if not separator or name not in known:
            raise QualityConfigError(f"unknown threshold override {item!r}; use <check>=<value>")
        try:
            number = float(value)
        except ValueError as exc:
            raise QualityConfigError(f"threshold {item!r} is not a number") from exc
        if number < 0:
            raise QualityConfigError(f"threshold {item!r} must not be negative")
        if name == MIN_SHARE_ROWS and not number.is_integer():
            raise QualityConfigError(f"threshold {item!r} must be a whole number")
        parsed[name] = number
    return parsed


@dataclass(frozen=True)
class Scope:
    """What a quality run covers: batches (in sequence order) and reporting dates."""

    dataset: str
    batch_ids: tuple[str, ...]
    metric_dates: tuple[date, ...]


@dataclass(frozen=True)
class CheckResult:
    """The outcome of one stored check."""

    check_name: str
    layer: str
    table_name: str
    severity: str
    status: str
    observed_value: float | None
    threshold: float
    threshold_kind: str
    failing_rows: int
    rows_checked: int
    sample_keys: tuple[str, ...]
    duration_ms: int = 0


@dataclass
class QualityRun:
    """One evaluation of the whole suite."""

    dataset: str
    triggered_by: str
    results: list[CheckResult]
    duration_ms: int
    batch_id: str | None = None
    batch_sequence: int | None = None
    attempt: int | None = None

    @property
    def blocking_failures(self) -> list[CheckResult]:
        return [r for r in self.results if r.status == FAILED]

    @property
    def warnings(self) -> list[CheckResult]:
        return [r for r in self.results if r.status == WARNED]

    @property
    def status(self) -> str:
        if self.blocking_failures:
            return FAILED
        return WARNED if self.warnings else PASSED

    def summary(self) -> str:
        """One log line: checks, blocking failures, warnings, seconds."""
        return (
            f"{len(self.results)} checks, {len(self.blocking_failures)} blocking failures, "
            f"{len(self.warnings)} warnings, {self.duration_ms / 1000:.2f} s"
        )


class QualityGateError(RuntimeError):
    """A blocking check failed; the batch must not be marked succeeded."""

    def __init__(self, run: QualityRun) -> None:
        self.run = run
        names = ", ".join(r.check_name for r in run.blocking_failures)
        super().__init__(f"quality gate: {names}")


def evaluate_status(severity: str, observed: float | None, threshold: float) -> str:
    """passed while ``observed`` is at most ``threshold`` (no data passes)."""
    if observed is None or observed <= threshold:
        return PASSED
    return FAILED if severity == BLOCKING else WARNED


def evaluate_result(
    kind: str,
    severity: str,
    observed: float | None,
    threshold: float,
    rows_checked: int,
    min_share_rows: int,
) -> str:
    """Status of one result: like :func:`evaluate_status`, but a share threshold applies only
    from ``min_share_rows`` rows checked (a share of a tiny sample says nothing)."""
    if kind == KIND_SHARE and rows_checked < min_share_rows:
        return PASSED
    return evaluate_status(severity, observed, threshold)


def _observed(kind: str, failing: int, checked: int, measure: Any) -> float | None:
    if kind == KIND_COUNT:
        return float(failing)
    if kind == KIND_SHARE:
        return round(failing / checked, SHARE_PLACES) if checked else 0.0
    return None if measure is None else round(float(measure), 3)


def query_parameters(scope: Scope, config: QualityConfig) -> dict[str, Any]:
    """Bound parameters shared by every check query."""
    return {
        "dataset": scope.dataset,
        "batch_ids": list(scope.batch_ids),
        "dates": list(scope.metric_dates),
        "tz": REPORTING_TIMEZONE,
        "meta_columns": list(RAW_META_COLUMNS),
        "order_statuses": list(ORDER_STATUSES),
        "vehicle_types": list(VEHICLE_TYPES),
        "dispositions": list(DISPOSITIONS),
        "reason_codes": list(REASON_CODES),
        "cities": sorted(CITY_STATES),
        "max_speed_kmh": config.max_speed_kmh,
        "long_gap_seconds": config.long_gap_seconds,
    }


def run_check(
    cur: psycopg.Cursor[Any], check: Check, scope: Scope, config: QualityConfig
) -> list[CheckResult]:
    """Run one check; one result per instance, in the check's instance order."""
    started = time.perf_counter()
    rows = cur.execute(check.query, query_parameters(scope, config)).fetchall()
    duration_ms = int((time.perf_counter() - started) * 1000)
    by_instance = {str(row[0]): row for row in rows}
    results: list[CheckResult] = []
    for instance in check.instances:
        row = by_instance.get(instance)
        if row is None:
            raise RuntimeError(f"check {check.name} returned no row for {instance!r}")
        failing, checked, sample, measure = int(row[1]), int(row[2]), list(row[3]), row[4]
        threshold = config.threshold(check, instance)
        observed = _observed(check.kind, failing, checked, measure)
        status = evaluate_result(
            check.kind, check.severity, observed, threshold, checked, config.share_min_rows()
        )
        if check.kind == KIND_HOURS:
            failing = 0 if status == PASSED else 1
            sample = [] if status == PASSED else sample
        results.append(
            CheckResult(
                check_name=check.result_name(instance),
                layer=check.layer,
                table_name=check.table,
                severity=check.severity,
                status=status,
                observed_value=observed,
                threshold=threshold,
                threshold_kind=check.kind,
                failing_rows=failing,
                rows_checked=checked,
                sample_keys=tuple(str(key) for key in sample),
                duration_ms=duration_ms,
            )
        )
    return results


def run_checks(
    cur: psycopg.Cursor[Any],
    scope: Scope,
    config: QualityConfig,
    checks: Sequence[Check] = CHECKS,
) -> list[CheckResult]:
    """Run ``checks`` (default: all) over ``scope``.

    The tables the checks read are analyzed first (``staging.analyze_quality_tables()``, owned
    by deliverypulse_owner; the ETL role may only execute it). During a load the batch's rows
    are uncommitted; ANALYZE in the same transaction counts them, so the planner no longer
    underestimates them as about one row and does not choose quadratic nested loops. This
    replaces the earlier ``SET LOCAL enable_nestloop = off``.
    """
    cur.execute("SELECT staging.analyze_quality_tables()")
    return [result for check in checks for result in run_check(cur, check, scope, config)]


def run_quality(
    cur: psycopg.Cursor[Any],
    scope: Scope,
    config: QualityConfig,
    triggered_by: str,
    batch: tuple[str, int, int] | None = None,
) -> QualityRun:
    """Run every check and time it. ``batch`` is (batch_id, sequence, attempt) for a load."""
    started = time.perf_counter()
    results = run_checks(cur, scope, config)
    run = QualityRun(
        dataset=scope.dataset,
        triggered_by=triggered_by,
        results=results,
        duration_ms=int((time.perf_counter() - started) * 1000),
    )
    if batch is not None:
        run.batch_id, run.batch_sequence, run.attempt = batch
    return run


def store_run(cur: psycopg.Cursor[Any], run: QualityRun) -> int:
    """Insert the run and its results into audit.quality_runs / audit.quality_results."""
    cur.execute(
        "INSERT INTO audit.quality_runs (dataset, batch_id, batch_sequence, attempt, "
        "triggered_by, status, checks_total, blocking_failures, warnings, duration_ms) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING quality_run_id",
        (
            run.dataset,
            run.batch_id,
            run.batch_sequence,
            run.attempt,
            run.triggered_by,
            run.status,
            len(run.results),
            len(run.blocking_failures),
            len(run.warnings),
            run.duration_ms,
        ),
    )
    row = cur.fetchone()
    if row is None:
        raise RuntimeError("could not insert the quality run")
    run_id = int(row[0])
    cur.executemany(
        "INSERT INTO audit.quality_results (quality_run_id, check_name, layer, table_name, "
        "severity, status, observed_value, threshold, threshold_kind, failing_rows, "
        "rows_checked, sample_keys, duration_ms) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
        [
            (
                run_id,
                r.check_name,
                r.layer,
                r.table_name,
                r.severity,
                r.status,
                None if r.observed_value is None else Decimal(str(r.observed_value)),
                Decimal(str(r.threshold)),
                r.threshold_kind,
                r.failing_rows,
                r.rows_checked,
                list(r.sample_keys),
                r.duration_ms,
            )
            for r in run.results
        ],
    )
    return run_id


def slowest_checks(run: QualityRun, count: int = 3) -> str:
    """The ``count`` slowest checks of ``run``, e.g. ``a 1.20 s, b 0.80 s``."""
    by_check: dict[str, int] = {}
    for r in run.results:
        by_check[r.check_name.split(".")[0]] = r.duration_ms
    slowest = sorted(by_check.items(), key=lambda item: item[1], reverse=True)[:count]
    return ", ".join(f"{name} {ms / 1000:.2f} s" for name, ms in slowest)


def format_value(value: float | None, kind: str) -> str:
    """Human-readable observed value or threshold."""
    if value is None:
        return "n/a"
    if kind == KIND_SHARE:
        return f"{value * 100:.2f}%"
    if kind == KIND_HOURS:
        return f"{value:.2f} h"
    return f"{value:g}"


def result_table(results: Sequence[CheckResult]) -> list[str]:
    """Plain-text summary table, one line per stored check."""
    lines = ["check | severity | status | observed | threshold | failing/checked | sample"]
    for r in results:
        lines.append(
            f"{r.check_name} | {r.severity} | {r.status} | "
            f"{format_value(r.observed_value, r.threshold_kind)} | "
            f"{format_value(r.threshold, r.threshold_kind)} | "
            f"{r.failing_rows}/{r.rows_checked} | {', '.join(r.sample_keys[:3])}"
        )
    return lines
