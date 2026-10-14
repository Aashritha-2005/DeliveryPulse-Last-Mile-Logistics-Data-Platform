"""Load raw batches into PostgreSQL: idempotent, strictly in sequence, one transaction per batch.

Usage (from the project root)::

    uv run python -m src.database.load_postgres [--dataset NAME]

Per batch, on ONE connection (autocommit mode), using nested ``conn.transaction()`` blocks:

* A (short, commits on its own): leftover ``started`` audit rows of the batch become
  ``failed``/``abandoned``; four ``started`` rows (one per source) are inserted for a new attempt.
* B (all or nothing): raw rows, record dispositions, rejected records, the pending queue,
  pending resolutions, analytics upserts, pipeline state, the dimension and aggregate refresh
  for the affected reporting dates, the data-quality checks (``src/quality``) and their stored
  results, and the audit rows -> ``succeeded``. A failed blocking check aborts B.
* C (only if B failed; commits on its own): the attempt's audit rows -> ``failed``, plus the
  quality run when the quality gate was the cause.

Inside a test's outer transaction the three blocks become savepoints, so everything rolls back.
A batch whose ``batch_id`` already succeeded is skipped without writing anything.
Connects as ``deliverypulse_etl`` only.
"""

import argparse
import json
import logging
import sys
import time
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any, LiteralString

import psycopg
from psycopg import sql

from src.config import ConfigError, load_database_settings
from src.contract import (
    ACCEPTED,
    BATCH_METADATA_FILE,
    DUPLICATE,
    MAX_PLAUSIBLE_SPEED_KMH,
    PENDING,
    REJECTED,
    SOURCE_COLUMNS,
    SOURCE_DELIVERY_EVENTS,
    SOURCE_DRIVERS,
    SOURCE_FILES,
    SOURCE_ORDERS,
    SOURCE_SHIFT_EVENTS,
    STALE_IGNORED,
    parse_utc,
)
from src.database.metrics import affected_dates, refresh_dimensions, refresh_metrics
from src.database.postgres_state import (
    STATUS_SUCCEEDED,
    Connection,
    PostgresState,
    to_timestamp,
)
from src.quality.checks import (
    TRIGGER_LOAD,
    QualityConfig,
    QualityConfigError,
    QualityGateError,
    QualityRun,
    Scope,
    parse_thresholds,
    run_quality,
    slowest_checks,
    store_run,
)
from src.transformations.outputs import batch_summary
from src.transformations.pipeline import (
    DEFAULT_ORPHAN_MAX_AGE_BATCHES,
    EXPIRED,
    BatchResult,
    TransformConfig,
    process_batch,
)
from src.transformations.records import (
    Batch,
    BatchSchemaError,
    RawRecord,
    batch_folders,
    load_batch,
)
from src.transformations.state import BatchSequenceError
from src.transformations.transform import DEFAULT_RAW_DIR, TransformError, resolve_dataset
from src.utils.logging import configure_logging

logger = logging.getLogger("deliverypulse.load")

STATUS_STARTED = "started"
STATUS_FAILED = "failed"
ABANDONED = "abandoned"
LOADED = "loaded"
SKIPPED = "skipped"
# Session-level advisory lock: only one load runs against a database at a time.
LOAD_LOCK_KEY = 7_265_302_026
SOURCES = tuple(source for source, _ in SOURCE_FILES)
SOURCE_FILE_NAMES = dict(SOURCE_FILES)
RAW_TABLES = {
    SOURCE_DRIVERS: "drivers_raw",
    SOURCE_ORDERS: "orders_raw",
    SOURCE_DELIVERY_EVENTS: "events_raw",
    SOURCE_SHIFT_EVENTS: "shift_events_raw",
}


class LoadError(RuntimeError):
    """The load cannot run (another dataset is loaded, another load is running, ...)."""


@dataclass
class Effects:
    """Database effects of one source in one batch."""

    inserted: int = 0
    updated: int = 0
    unchanged: int = 0


@dataclass
class BatchLoad:
    """Outcome of loading one batch folder."""

    sequence: int
    batch_id: str
    status: str
    attempt: int = 0
    result: BatchResult | None = None
    effects: dict[str, Effects] = field(default_factory=dict)
    quality: QualityRun | None = None


# ---------------------------------------------------------------- raw values ---


def _raw_text(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    if isinstance(value, int) and not isinstance(value, bool):
        return str(value)
    return json.dumps(value, ensure_ascii=True)


def _reject_constant(name: str) -> Any:
    raise ValueError(f"non-standard JSON constant {name}")


def _parse_json(text: str) -> Any:
    """Parse strict JSON (no NaN/Infinity, which PostgreSQL jsonb rejects); numbers as text."""
    return json.loads(text, parse_float=str, parse_constant=_reject_constant)


def raw_values(rec: RawRecord) -> tuple[list[str | None], str | None]:
    """Contract column values exactly as received, plus extra columns as a JSON object.

    CSV values stay text ("" stays ""), JSON nulls become NULL, and a record that cannot be
    parsed into fields is kept whole in ``extra_columns`` as ``{"_raw_line": ...}``.
    """
    columns = SOURCE_COLUMNS[rec.source]
    try:
        parsed = _parse_json(rec.raw)
    except ValueError:
        parsed = None
    if not isinstance(parsed, dict):
        return [None] * len(columns), json.dumps({"_raw_line": rec.raw}, ensure_ascii=True)
    values = [_raw_text(parsed[column]) if column in parsed else None for column in columns]
    extras = {key: value for key, value in parsed.items() if key not in columns}
    extra_json = json.dumps(extras, sort_keys=True, ensure_ascii=True) if extras else None
    return values, extra_json


def payload_json(raw: str) -> str:
    """The raw record as JSON text for a jsonb column; unparseable text is wrapped."""
    try:
        _parse_json(raw)
    except ValueError:
        return json.dumps({"_raw_line": raw}, ensure_ascii=True)
    return raw


# ------------------------------------------------------------------- helpers ---


def copy_rows(
    cur: psycopg.Cursor[Any],
    table: tuple[str, str],
    columns: Sequence[str],
    rows: Iterable[Sequence[Any]],
) -> None:
    """Bulk-insert ``rows`` with COPY."""
    statement = sql.SQL("COPY {} ({}) FROM STDIN").format(
        sql.Identifier(*table), sql.SQL(", ").join(sql.Identifier(c) for c in columns)
    )
    with cur.copy(statement) as copy:
        for row in rows:
            copy.write_row(row)


def _temp_like(cur: psycopg.Cursor[Any], name: str, table: tuple[str, str]) -> None:
    """A temporary copy of ``table``'s columns (dropped at commit)."""
    cur.execute(sql.SQL("DROP TABLE IF EXISTS pg_temp.{}").format(sql.Identifier(name)))
    cur.execute(
        sql.SQL("CREATE TEMP TABLE {} (LIKE {}) ON COMMIT DROP").format(
            sql.Identifier(name), sql.Identifier(*table)
        )
    )


def read_batch_identity(folder: Path) -> tuple[int, str]:
    """(batch_sequence, batch_id) from ``batch.json``, without reading the data files."""
    metadata = json.loads((folder / BATCH_METADATA_FILE).read_text(encoding="utf-8"))
    return int(metadata["batch_sequence"]), str(metadata["batch_id"])


def read_batch_window(folder: Path) -> tuple[int, int]:
    """(window_start, window_end) epochs from ``batch.json``."""
    metadata = json.loads((folder / BATCH_METADATA_FILE).read_text(encoding="utf-8"))
    start = parse_utc(str(metadata["window_start"]))
    end = parse_utc(str(metadata["window_end_exclusive"]))
    if start is None or end is None:
        raise BatchSchemaError(f"{folder}: invalid window in {BATCH_METADATA_FILE}")
    return start, end


def succeeded_batches(conn: Connection, dataset: str) -> dict[int, str]:
    """batch_sequence -> batch_id of every succeeded batch of ``dataset``."""
    rows = conn.execute(
        "SELECT DISTINCT batch_sequence, batch_id FROM audit.pipeline_runs "
        "WHERE dataset = %s AND status = %s",
        (dataset, STATUS_SUCCEEDED),
    ).fetchall()
    return {int(row[0]): str(row[1]) for row in rows}


def check_single_dataset(conn: Connection, dataset: str) -> None:
    """The database holds one dataset; switching requires `make reset-db`."""
    rows = conn.execute(
        "SELECT DISTINCT dataset FROM audit.pipeline_runs WHERE status = %s AND dataset <> %s",
        (STATUS_SUCCEEDED, dataset),
    ).fetchall()
    if rows:
        others = ", ".join(sorted(str(row[0]) for row in rows))
        raise LoadError(
            f"The database already holds dataset(s) {others}; run `make reset-db` "
            f"(development data only) before loading {dataset}."
        )


# --------------------------------------------------------------------- audit ---


def start_attempt(
    conn: Connection,
    dataset: str,
    sequence: int,
    batch_id: str,
    window: tuple[int, int] | None = None,
) -> tuple[int, dict[str, int]]:
    """Transaction A: mark abandoned attempts, insert one ``started`` row per source.

    ``window`` is the batch's (window_start, window_end), stored for the quality checks.
    """
    window_start = None if window is None else to_timestamp(window[0])
    window_end = None if window is None else to_timestamp(window[1])
    with conn.transaction(), conn.cursor() as cur:
        cur.execute(
            "UPDATE audit.pipeline_runs SET status = %s, failure_reason = %s, "
            "finished_at = now() WHERE dataset = %s AND batch_id = %s AND status = %s",
            (STATUS_FAILED, ABANDONED, dataset, batch_id, STATUS_STARTED),
        )
        if cur.rowcount:
            logger.warning(
                "batch %d: %d abandoned audit rows marked failed", sequence, cur.rowcount
            )
        cur.execute(
            "SELECT coalesce(max(attempt), 0) + 1 FROM audit.pipeline_runs "
            "WHERE dataset = %s AND batch_id = %s",
            (dataset, batch_id),
        )
        row = cur.fetchone()
        attempt = 1 if row is None else int(row[0])
        run_ids: dict[str, int] = {}
        for source in SOURCES:
            cur.execute(
                "INSERT INTO audit.pipeline_runs (dataset, batch_id, batch_sequence, attempt, "
                "source, status, window_start, window_end) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s) RETURNING run_id",
                (
                    dataset,
                    batch_id,
                    sequence,
                    attempt,
                    source,
                    STATUS_STARTED,
                    window_start,
                    window_end,
                ),
            )
            inserted = cur.fetchone()
            if inserted is None:
                raise LoadError("could not insert the audit row")
            run_ids[source] = int(inserted[0])
    return attempt, run_ids


def mark_succeeded(
    cur: psycopg.Cursor[Any],
    run_ids: dict[str, int],
    result: BatchResult,
    effects: dict[str, Effects],
    started: float,
) -> None:
    """Part of transaction B: the attempt's audit rows -> ``succeeded`` with every counter."""
    summary = batch_summary(result)["sources"]
    duration_ms = int((time.perf_counter() - started) * 1000)
    for source in SOURCES:
        counts = summary[source]["dispositions"]
        effect = effects.get(source, Effects())
        cur.execute(
            "UPDATE audit.pipeline_runs SET status = %s, finished_at = now(), "
            "duration_ms = %s, rows_input = %s, rows_accepted = %s, rows_rejected = %s, "
            "rows_duplicate = %s, rows_stale_ignored = %s, rows_pending = %s, "
            "rows_inserted = %s, rows_updated = %s, rows_unchanged = %s, rows_failed = 0, "
            "pending_resolved = %s, pending_remaining = %s, pending_expired = %s "
            "WHERE run_id = %s",
            (
                STATUS_SUCCEEDED,
                duration_ms,
                summary[source]["rows_input"],
                counts[ACCEPTED],
                counts[REJECTED],
                counts[DUPLICATE],
                counts[STALE_IGNORED],
                counts[PENDING],
                effect.inserted,
                effect.updated,
                effect.unchanged,
                summary[source]["pending_resolved"],
                summary[source]["pending_remaining"],
                summary[source]["pending_expired"],
                run_ids[source],
            ),
        )


def mark_failed(
    conn: Connection,
    run_ids: dict[str, int],
    rows_input: dict[str, int],
    error: BaseException,
    started: float,
    quality: QualityRun | None = None,
) -> None:
    """Transaction C: the attempt's audit rows -> ``failed``. Nothing of B was committed.

    If the quality gate blocked the batch, its quality run (kept in memory while B rolled back)
    is stored in this same transaction, so the evidence survives.
    """
    reason = f"{type(error).__name__}: {error}"[:1000]
    if isinstance(error, QualityGateError):
        reason = str(error)[:1000]
    duration_ms = int((time.perf_counter() - started) * 1000)
    try:
        with conn.transaction(), conn.cursor() as cur:
            for source in SOURCES:
                cur.execute(
                    "UPDATE audit.pipeline_runs SET status = %s, failure_reason = %s, "
                    "finished_at = now(), duration_ms = %s, rows_input = %s, rows_failed = %s, "
                    "rows_accepted = 0, rows_rejected = 0, rows_duplicate = 0, "
                    "rows_stale_ignored = 0, rows_pending = 0, rows_inserted = 0, "
                    "rows_updated = 0, rows_unchanged = 0, pending_resolved = 0, "
                    "pending_remaining = 0, pending_expired = 0 WHERE run_id = %s",
                    (
                        STATUS_FAILED,
                        reason,
                        duration_ms,
                        rows_input.get(source, 0),
                        rows_input.get(source, 0),
                        run_ids[source],
                    ),
                )
            if quality is not None:
                store_run(cur, quality)
    except psycopg.Error:
        logger.exception("could not record the failed attempt in audit.pipeline_runs")


# ------------------------------------------------------------ batch writes ---


def write_raw(cur: psycopg.Cursor[Any], batch: Batch) -> None:
    """Every input row exactly as received, into raw.*_raw."""
    for source in SOURCES:
        columns = ["batch_id", "source_file", "source_row_number", *SOURCE_COLUMNS[source]]
        rows = []
        for rec in batch.records[source]:
            values, extras = raw_values(rec)
            rows.append(
                [batch.batch_id, SOURCE_FILE_NAMES[source], rec.row_number, *values, extras]
            )
        copy_rows(cur, ("raw", RAW_TABLES[source]), [*columns, "extra_columns"], rows)


def write_dispositions(cur: psycopg.Cursor[Any], batch_id: str, result: BatchResult) -> None:
    """One audit row per input row with its disposition and reason."""
    rows = (
        (batch_id, d.source, d.row_number, d.record_key, d.disposition, d.reason)
        for d in result.dispositions
    )
    columns = ["batch_id", "source", "row_number", "record_key", "disposition", "reason"]
    copy_rows(cur, ("audit", "record_dispositions"), columns, rows)


def write_rejected(cur: psycopg.Cursor[Any], batch_id: str, result: BatchResult) -> None:
    """Rejected rows of the batch, plus pending records that expired in this batch."""
    rows: list[tuple[str, str, str, str, str]] = [
        (batch_id, d.source, d.record_key, payload_json(d.raw), d.reason)
        for d in result.dispositions
        if d.disposition == REJECTED
    ]
    for resolution in result.resolutions:
        if resolution.outcome == EXPIRED:
            pending = resolution.record
            payload = payload_json(pending.raw)
            rows.append(
                (pending.batch_id, pending.source, pending.record_key, payload, resolution.reason)
            )
    columns = ["batch_id", "source", "record_key", "record_payload", "reason"]
    copy_rows(cur, ("staging", "rejected_records"), columns, rows)


def write_pending(cur: psycopg.Cursor[Any], result: BatchResult) -> None:
    """Replace the pending queue with its state at the end of this batch."""
    cur.execute("DELETE FROM staging.pending_events")
    rows = (
        (
            p.source,
            p.record_key,
            p.batch_id,
            p.batch_sequence,
            p.row_number,
            p.missing_reference,
            p.fields_json,
            payload_json(p.raw),
            to_timestamp(p.first_seen_at),
            None if p.last_retry_at is None else to_timestamp(p.last_retry_at),
            p.retry_count,
        )
        for p in result.pending_after
    )
    columns = [
        "source",
        "record_key",
        "batch_id",
        "batch_sequence",
        "row_number",
        "missing_reference",
        "fields",
        "record_payload",
        "first_seen_at",
        "last_retry_at",
        "retry_count",
    ]
    copy_rows(cur, ("staging", "pending_events"), columns, rows)


def write_resolutions(cur: psycopg.Cursor[Any], batch_id: str, result: BatchResult) -> None:
    """How pending records left the queue in this batch."""
    rows = (
        (
            r.record.source,
            r.record.record_key,
            r.record.batch_id,
            batch_id,
            r.outcome,
            r.disposition,
            r.reason,
        )
        for r in result.resolutions
    )
    columns = [
        "source",
        "record_key",
        "original_batch_id",
        "resolved_in_batch_id",
        "outcome",
        "disposition",
        "reason",
    ]
    copy_rows(cur, ("audit", "pending_resolutions"), columns, rows)


_ORDER_COLUMNS: tuple[LiteralString, ...] = (
    "customer_id",
    "driver_id",
    "order_created_ts",
    "pickup_ts",
    "delivery_ts",
    "promised_ts",
    "distance_km",
    "status",
    "city",
    "fare_amount",
    "source_updated_at",
)


def _insert_immutable(
    cur: psycopg.Cursor[Any], temp: str, table: tuple[str, str], key: str, rows: list[Any]
) -> Effects:
    """INSERT ... ON CONFLICT DO NOTHING via a temp table; returns the effects."""
    _temp_like(cur, temp, table)
    copy_rows(cur, ("pg_temp", temp), _columns(cur, table), rows)
    cur.execute(
        sql.SQL("INSERT INTO {} SELECT * FROM {} ON CONFLICT ({}) DO NOTHING").format(
            sql.Identifier(*table), sql.Identifier("pg_temp", temp), sql.Identifier(key)
        )
    )
    inserted = max(cur.rowcount, 0)
    return Effects(inserted=inserted, unchanged=len(rows) - inserted)


def _columns(cur: psycopg.Cursor[Any], table: tuple[str, str]) -> list[str]:
    cur.execute(
        "SELECT attname FROM pg_attribute WHERE attrelid = %s::regclass "
        "AND attnum > 0 AND NOT attisdropped ORDER BY attnum",
        (f"{table[0]}.{table[1]}",),
    )
    return [str(row[0]) for row in cur.fetchall()]


@dataclass
class AnalyticsWrite:
    """Effects of the analytics upserts, plus the (created, pickup, delivery) epochs of every
    inserted or updated order version, old and new (input for the metric refresh)."""

    effects: dict[str, Effects]
    order_versions: list[tuple[int, int | None, int | None]]


def _epoch(value: Any) -> int | None:
    return None if value is None else int(value.timestamp())


def write_analytics(cur: psycopg.Cursor[Any], state: PostgresState) -> AnalyticsWrite:
    """Upsert the batch's accepted records into analytics (FK order: drivers first).

    The orders upsert uses ``RETURNING old.*, new.*`` (PostgreSQL 18+) so the metric refresh
    also recomputes the dates an updated order leaves.
    """
    drivers = [
        (d.driver_id, d.city, d.vehicle_type, date.fromisoformat(d.joined_date), d.rating)
        for _, d in sorted(state.new_drivers.items())
    ]
    effects = {
        SOURCE_DRIVERS: _insert_immutable(
            cur, "tmp_drivers", ("analytics", "drivers"), "driver_id", drivers
        )
    }

    orders = [
        (
            o.order_id,
            o.customer_id,
            o.driver_id,
            to_timestamp(o.order_created_ts),
            None if o.pickup_ts is None else to_timestamp(o.pickup_ts),
            None if o.delivery_ts is None else to_timestamp(o.delivery_ts),
            to_timestamp(o.promised_ts),
            o.distance_km,
            o.status,
            o.city,
            o.fare_amount,
            to_timestamp(o.source_updated_at),
        )
        for _, o in sorted(state.new_orders.items())
    ]
    _temp_like(cur, "tmp_orders", ("analytics", "orders"))
    copy_rows(cur, ("pg_temp", "tmp_orders"), ["order_id", *_ORDER_COLUMNS], orders)
    assignments = sql.SQL(", ").join(
        sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(c), sql.Identifier(c))
        for c in _ORDER_COLUMNS
    )
    cur.execute(
        sql.SQL(
            "INSERT INTO analytics.orders AS o SELECT * FROM pg_temp.tmp_orders "
            "ON CONFLICT (order_id) DO UPDATE SET {} "
            "WHERE o.source_updated_at < EXCLUDED.source_updated_at "
            "RETURNING (xmax = 0), old.order_created_ts, old.pickup_ts, old.delivery_ts, "
            "new.order_created_ts, new.pickup_ts, new.delivery_ts"
        ).format(assignments)
    )
    returned = cur.fetchall()
    inserted = sum(1 for row in returned if row[0])
    updated = len(returned) - inserted
    effects[SOURCE_ORDERS] = Effects(inserted, updated, len(orders) - len(returned))
    order_versions: list[tuple[int, int | None, int | None]] = []
    for row in returned:
        for created, pickup, delivery in ((row[1], row[2], row[3]), (row[4], row[5], row[6])):
            created_epoch = _epoch(created)
            if created_epoch is not None:
                order_versions.append((created_epoch, _epoch(pickup), _epoch(delivery)))

    events = [
        (e.event_id, e.order_id, e.driver_id, e.event_type, to_timestamp(e.event_ts), e.lat, e.lon)
        for _, e in sorted(state.new_delivery_events.items())
    ]
    effects[SOURCE_DELIVERY_EVENTS] = _insert_immutable(
        cur, "tmp_delivery_events", ("analytics", "delivery_events"), "event_id", events
    )
    shifts = [
        (s.event_id, s.driver_id, s.event_type, to_timestamp(s.event_ts))
        for _, s in sorted(state.new_shift_events.items())
    ]
    effects[SOURCE_SHIFT_EVENTS] = _insert_immutable(
        cur, "tmp_shift_events", ("analytics", "driver_shift_events"), "event_id", shifts
    )
    return AnalyticsWrite(effects, order_versions)


def refresh_batch_metrics(
    cur: psycopg.Cursor[Any], state: PostgresState, written: AnalyticsWrite
) -> list[date]:
    """Refresh dimensions and aggregates for the dates this batch can change (in B)."""
    shift_times = [s.event_ts for s in state.new_shift_events.values()]
    dates = affected_dates(written.order_versions, shift_times)
    cities = [o.city for o in state.new_orders.values()]
    cities += [d.city for d in state.new_drivers.values()]
    refresh_dimensions(cur, cities)
    refresh_metrics(cur, dates)
    return dates


def write_state(cur: psycopg.Cursor[Any], state: PostgresState, batch_id: str) -> None:
    """Flush buffered pipeline state: record versions/hashes and new GPS reference pings."""
    records: list[tuple[str, str, Any, str, str]] = [
        (SOURCE_DRIVERS, key, None, record.content_hash(), batch_id)
        for key, record in sorted(state.new_drivers.items())
    ]
    records += [
        (SOURCE_ORDERS, key, to_timestamp(o.source_updated_at), o.content_hash(), batch_id)
        for key, o in sorted(state.new_orders.items())
    ]
    records += [
        (source, key, None, digest, batch_id)
        for (source, key), digest in sorted(state.new_marks.items())
    ]
    _temp_like(cur, "tmp_record_state", ("staging", "record_state"))
    columns = ["source", "record_key", "version_ts", "content_hash", "batch_id"]
    copy_rows(cur, ("pg_temp", "tmp_record_state"), columns, records)
    cur.execute(
        "INSERT INTO staging.record_state SELECT * FROM pg_temp.tmp_record_state "
        "ON CONFLICT (source, record_key) DO UPDATE SET version_ts = EXCLUDED.version_ts, "
        "content_hash = EXCLUDED.content_hash, batch_id = EXCLUDED.batch_id"
    )
    pings = (
        (driver_id, to_timestamp(p.event_ts), p.event_id, p.lat, p.lon, batch_id)
        for driver_id, p in state.new_pings
    )
    columns = ["driver_id", "event_ts", "event_id", "lat", "lon", "batch_id"]
    copy_rows(cur, ("staging", "gps_reference"), columns, pings)


# ------------------------------------------------------------------- loading ---


def _log_batch(load: BatchLoad, seconds: float) -> None:
    for source in SOURCES:
        effect = load.effects.get(source, Effects())
        logger.info(
            "batch %d %s: inserted=%d, updated=%d, unchanged=%d",
            load.sequence,
            source,
            effect.inserted,
            effect.updated,
            effect.unchanged,
        )
    logger.info(
        "batch %d (%s) loaded, attempt %d, in %.2f s",
        load.sequence,
        load.batch_id[:12],
        load.attempt,
        seconds,
    )


def load_batch_folder(
    conn: Connection,
    dataset: str,
    folder: Path,
    config: TransformConfig,
    quality_config: QualityConfig | None = None,
) -> BatchLoad:
    """Load one batch folder; a no-op if its batch_id already succeeded.

    The quality checks run inside transaction B, after every write and the metric refresh and
    before the audit rows become ``succeeded``. A blocking failure raises
    :class:`QualityGateError`: B rolls back and C records the failure and the quality run.
    """
    quality_config = quality_config or QualityConfig()
    sequence, batch_id = read_batch_identity(folder)
    check_single_dataset(conn, dataset)
    done = succeeded_batches(conn, dataset)
    if done.get(sequence) == batch_id:
        logger.info("batch %d (%s) already loaded; skipping (no-op)", sequence, batch_id[:12])
        return BatchLoad(sequence, batch_id, SKIPPED)
    if sequence in done:
        raise LoadError(
            f"batch {sequence} was already loaded with a different batch_id ({done[sequence][:12]})"
        )
    expected = max(done, default=0) + 1
    if sequence != expected:
        raise BatchSequenceError(
            f"batch {sequence} cannot be loaded before batch {expected}; "
            "batches must be loaded in sequence"
        )

    started = time.perf_counter()
    window = read_batch_window(folder)
    attempt, run_ids = start_attempt(conn, dataset, sequence, batch_id, window)
    rows_input: dict[str, int] = {}
    try:
        with conn.transaction():
            batch = load_batch(folder)
            rows_input = {source: len(batch.records[source]) for source in SOURCES}
            state = PostgresState(conn, dataset)
            state.prefetch(batch)
            result = process_batch(batch, state, config)
            with conn.cursor() as cur:
                write_raw(cur, batch)
                write_dispositions(cur, batch_id, result)
                write_rejected(cur, batch_id, result)
                write_pending(cur, result)
                write_resolutions(cur, batch_id, result)
                written = write_analytics(cur, state)
                write_state(cur, state, batch_id)
                metric_dates = refresh_batch_metrics(cur, state, written)
                scope = Scope(dataset, (batch_id,), tuple(metric_dates))
                quality = run_quality(
                    cur, scope, quality_config, TRIGGER_LOAD, (batch_id, sequence, attempt)
                )
                if quality.blocking_failures:
                    raise QualityGateError(quality)
                store_run(cur, quality)
                mark_succeeded(cur, run_ids, result, written.effects, started)
    except QualityGateError as exc:
        logger.error("batch %d quality: %s", sequence, exc.run.summary())
        for failed in exc.run.blocking_failures:
            logger.error(
                "batch %d blocked by %s: %d failing rows (sample: %s)",
                sequence,
                failed.check_name,
                failed.failing_rows,
                ", ".join(failed.sample_keys[:5]),
            )
        mark_failed(conn, run_ids, rows_input, exc, started, exc.run)
        raise
    except Exception as exc:
        mark_failed(conn, run_ids, rows_input, exc, started)
        raise
    logger.info("batch %d quality: %s", sequence, quality.summary())
    logger.info("batch %d slowest checks: %s", sequence, slowest_checks(quality))
    load = BatchLoad(sequence, batch_id, LOADED, attempt, result, written.effects, quality)
    if metric_dates:
        logger.info(
            "batch %d metrics: %d reporting dates refreshed (%s to %s)",
            sequence,
            len(metric_dates),
            metric_dates[0],
            metric_dates[-1],
        )
    _log_batch(load, time.perf_counter() - started)
    return load


def load_dataset(
    conn: Connection,
    dataset_dir: Path,
    config: TransformConfig,
    quality_config: QualityConfig | None = None,
    through_batch: int | None = None,
) -> list[BatchLoad]:
    """Load the batches of ``dataset_dir`` in sequence; already-loaded batches are skipped.

    ``through_batch`` limits the load to batches 1..N (the orchestrator loads one more batch per
    scheduled run); None loads every batch. N outside 1..(number of batches) is refused before
    anything is written.
    """
    if not conn.autocommit:
        raise LoadError("the load needs a connection in autocommit mode")
    folders = batch_folders(dataset_dir)
    if not folders:
        raise LoadError(f"No batch folders in {dataset_dir}.")
    if through_batch is not None:
        if not 1 <= through_batch <= len(folders):
            raise LoadError(
                f"--through-batch {through_batch} is outside 1..{len(folders)} "
                f"(the batches in {dataset_dir.name})"
            )
        folders = folders[:through_batch]
    row = conn.execute("SELECT pg_try_advisory_lock(%s)", (LOAD_LOCK_KEY,)).fetchone()
    if row is None or not row[0]:
        raise LoadError("another load is running against this database")
    try:
        return [
            load_batch_folder(conn, dataset_dir.name, folder, config, quality_config)
            for folder in folders
        ]
    finally:
        conn.execute("SELECT pg_advisory_unlock(%s)", (LOAD_LOCK_KEY,))


def positive_int(text: str) -> int:
    """argparse type: a whole number of at least 1."""
    try:
        value = int(text)
    except ValueError:
        raise argparse.ArgumentTypeError(f"{text!r} is not a whole number") from None
    if value < 1:
        raise argparse.ArgumentTypeError(f"{value} must be at least 1")
    return value


def main(argv: Sequence[str] | None = None) -> int:
    """Command-line entry point; returns the process exit code."""
    parser = argparse.ArgumentParser(description="Load raw batches into PostgreSQL.")
    parser.add_argument("--raw-dir", type=Path, default=DEFAULT_RAW_DIR)
    parser.add_argument("--dataset", default=None, help="dataset folder name in --raw-dir")
    parser.add_argument("--max-speed-kmh", type=float, default=MAX_PLAUSIBLE_SPEED_KMH)
    parser.add_argument(
        "--orphan-max-age-batches", type=int, default=DEFAULT_ORPHAN_MAX_AGE_BATCHES
    )
    parser.add_argument(
        "--threshold",
        action="append",
        default=[],
        metavar="CHECK=VALUE",
        help="override a quality-check threshold (repeatable), e.g. raw_duplicate_rows=0.05",
    )
    parser.add_argument(
        "--through-batch",
        type=positive_int,
        default=None,
        metavar="N",
        help="load batches 1..N only (default: every batch)",
    )
    args = parser.parse_args(argv)
    configure_logging()
    raw_dir: Path = args.raw_dir
    config = TransformConfig(
        max_speed_kmh=args.max_speed_kmh, orphan_max_age_batches=args.orphan_max_age_batches
    )
    started = time.perf_counter()
    try:
        quality_config = QualityConfig(
            max_speed_kmh=args.max_speed_kmh, thresholds=parse_thresholds(args.threshold)
        )
        dataset_dir = resolve_dataset(raw_dir, args.dataset)
        settings = load_database_settings("etl")
        with psycopg.connect(settings.conninfo(), autocommit=True) as conn:
            logger.info("loading dataset %s as %s", dataset_dir.name, settings.user)
            loads = load_dataset(
                conn, dataset_dir, config, quality_config, through_batch=args.through_batch
            )
    except (
        TransformError,
        ConfigError,
        LoadError,
        BatchSchemaError,
        BatchSequenceError,
        QualityConfigError,
        QualityGateError,
        psycopg.Error,
    ) as exc:
        logger.error("%s", exc)
        return 1
    loaded = sum(1 for load in loads if load.status == LOADED)
    logger.info(
        "%d batches loaded, %d already loaded (skipped), in %.2f s",
        loaded,
        len(loads) - loaded,
        time.perf_counter() - started,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
