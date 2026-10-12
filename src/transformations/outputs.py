"""Deterministic Parquet and JSON outputs of the transform.

Per batch (``batches/batch-<k>/``):

* ``dispositions.parquet``: one row per input row (source, row_number, record_key, disposition,
  reason, record_payload for non-accepted rows)
* ``rejected_records.parquet``: rows shaped like ``staging.rejected_records`` (plus orphans that
  expired in this batch)
* ``pending_events.parquet``: rows shaped like ``staging.pending_events``, as of the batch end
* ``summary.json``: counts per source, disposition and reason, and pending-queue counts

Cleaned layer (``cleaned/``), a derived, normalised representation of the accepted records (not
the source): drivers unpartitioned; orders, delivery events and shift events partitioned
Hive-style as ``date=YYYY-MM-DD`` by reporting date (Asia/Kolkata).

Determinism: explicit schemas, rows sorted by key, one fixed file name per partition, logical
times instead of the wall clock. Timestamps that the staging tables fill with ``now()`` are set
to the end of the batch window here.
"""

import json
from collections import Counter
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from src.contract import (
    DISPOSITIONS,
    REJECTED,
    SOURCE_DELIVERY_EVENTS,
    SOURCE_FILES,
    SOURCE_ORDERS,
    SOURCE_SHIFT_EVENTS,
    format_reporting_date,
)
from src.transformations.pipeline import EXPIRED, RESOLVED, BatchResult
from src.transformations.state import LocalState

PART_FILE = "part-0.parquet"
TIMESTAMP = pa.timestamp("s", tz="UTC")

DISPOSITION_SCHEMA = pa.schema(
    [
        ("source", pa.string()),
        ("row_number", pa.int64()),
        ("record_key", pa.string()),
        ("disposition", pa.string()),
        ("reason", pa.string()),
        ("record_payload", pa.string()),
    ]
)
REJECTED_SCHEMA = pa.schema(
    [
        ("batch_id", pa.string()),
        ("source", pa.string()),
        ("record_key", pa.string()),
        ("record_payload", pa.string()),
        ("reason", pa.string()),
        ("rejected_at", TIMESTAMP),
    ]
)
PENDING_SCHEMA = pa.schema(
    [
        ("source", pa.string()),
        ("event_id", pa.string()),
        ("batch_id", pa.string()),
        ("missing_reference", pa.string()),
        ("record_payload", pa.string()),
        ("first_seen_at", TIMESTAMP),
        ("last_retry_at", TIMESTAMP),
        ("retry_count", pa.int32()),
    ]
)
DRIVER_SCHEMA = pa.schema(
    [
        ("driver_id", pa.string()),
        ("city", pa.string()),
        ("vehicle_type", pa.string()),
        ("joined_date", pa.date32()),
        ("rating", pa.decimal128(3, 2)),
    ]
)
ORDER_SCHEMA = pa.schema(
    [
        ("order_id", pa.string()),
        ("customer_id", pa.string()),
        ("driver_id", pa.string()),
        ("order_created_ts", TIMESTAMP),
        ("pickup_ts", TIMESTAMP),
        ("delivery_ts", TIMESTAMP),
        ("promised_ts", TIMESTAMP),
        ("distance_km", pa.decimal128(7, 3)),
        ("status", pa.string()),
        ("city", pa.string()),
        ("fare_amount", pa.decimal128(10, 2)),
        ("source_updated_at", TIMESTAMP),
        ("delivery_minutes", pa.float64()),
        ("is_late", pa.bool_()),
    ]
)
DELIVERY_EVENT_SCHEMA = pa.schema(
    [
        ("event_id", pa.string()),
        ("order_id", pa.string()),
        ("driver_id", pa.string()),
        ("event_type", pa.string()),
        ("event_ts", TIMESTAMP),
        ("lat", pa.float64()),
        ("lon", pa.float64()),
    ]
)
SHIFT_EVENT_SCHEMA = pa.schema(
    [
        ("event_id", pa.string()),
        ("driver_id", pa.string()),
        ("event_type", pa.string()),
        ("event_ts", TIMESTAMP),
    ]
)


def _ts(epoch: int | None) -> datetime | None:
    return None if epoch is None else datetime.fromtimestamp(epoch, UTC)


def _scaled(value: Decimal | None, places: str) -> Decimal | None:
    """Fit a decimal to the column scale (contract values already have exactly this scale)."""
    return None if value is None else value.quantize(Decimal(places))


def write_parquet(path: Path, schema: Any, rows: list[dict[str, Any]]) -> None:
    """Write ``rows`` with an explicit schema (deterministic for a given pyarrow version)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows, schema=schema), path)


def batch_summary(result: BatchResult) -> dict[str, Any]:
    """Counts per source, disposition and reason, plus pending-queue counts."""
    sources: dict[str, Any] = {}
    for source, _ in SOURCE_FILES:
        rows = [d for d in result.dispositions if d.source == source]
        reasons = Counter((d.disposition, d.reason) for d in rows if d.reason)
        resolutions = [r for r in result.resolutions if r.record.source == source]
        sources[source] = {
            "rows_input": result.rows_input[source],
            "dispositions": result.counts(source),
            "reasons": {
                f"{disposition}:{reason}": count
                for (disposition, reason), count in sorted(reasons.items())
            },
            "pending_resolved": sum(1 for r in resolutions if r.outcome == RESOLVED),
            "pending_expired": sum(1 for r in resolutions if r.outcome == EXPIRED),
            "pending_remaining": sum(1 for p in result.pending_after if p.source == source),
        }
    return {
        "batch_sequence": result.sequence,
        "batch_id": result.batch_id,
        "dispositions_order": list(DISPOSITIONS),
        "sources": sources,
    }


def write_batch_outputs(folder: Path, result: BatchResult) -> None:
    """Write the four per-batch outputs of ``result`` into ``folder``."""
    window_end = _ts(result.window_end)
    dispositions = [
        {
            "source": d.source,
            "row_number": d.row_number,
            "record_key": d.record_key,
            "disposition": d.disposition,
            "reason": d.reason,
            "record_payload": d.raw or None,
        }
        for d in result.dispositions
    ]
    rejected = [
        {
            "batch_id": result.batch_id,
            "source": d.source,
            "record_key": d.record_key,
            "record_payload": d.raw,
            "reason": d.reason,
            "rejected_at": window_end,
        }
        for d in result.dispositions
        if d.disposition == REJECTED
    ]
    rejected += [
        {
            "batch_id": r.record.batch_id,
            "source": r.record.source,
            "record_key": r.record.record_key,
            "record_payload": r.record.raw,
            "reason": r.reason,
            "rejected_at": window_end,
        }
        for r in result.resolutions
        if r.outcome == EXPIRED
    ]
    pending = [
        {
            "source": p.source,
            "event_id": p.record_key,
            "batch_id": p.batch_id,
            "missing_reference": p.missing_reference,
            "record_payload": p.raw,
            "first_seen_at": _ts(p.first_seen_at),
            "last_retry_at": _ts(p.last_retry_at),
            "retry_count": p.retry_count,
        }
        for p in result.pending_after
    ]
    write_parquet(folder / "dispositions.parquet", DISPOSITION_SCHEMA, dispositions)
    write_parquet(folder / "rejected_records.parquet", REJECTED_SCHEMA, rejected)
    write_parquet(folder / "pending_events.parquet", PENDING_SCHEMA, pending)
    summary = json.dumps(batch_summary(result), indent=2, sort_keys=True, ensure_ascii=True)
    (folder / "summary.json").write_text(summary + "\n", encoding="utf-8", newline="\n")


def _write_partitioned(folder: Path, schema: Any, rows: list[tuple[int, dict[str, Any]]]) -> None:
    """Write rows (each with its partition epoch) as ``date=YYYY-MM-DD/part-0.parquet``."""
    partitions: dict[str, list[dict[str, Any]]] = {}
    for epoch, row in rows:
        partitions.setdefault(format_reporting_date(epoch), []).append(row)
    for day in sorted(partitions):
        write_parquet(folder / f"date={day}" / PART_FILE, schema, partitions[day])


def write_cleaned(folder: Path, state: LocalState) -> None:
    """Write the cleaned layer from the accepted records in ``state``."""
    drivers = [
        {
            "driver_id": d.driver_id,
            "city": d.city,
            "vehicle_type": d.vehicle_type,
            "joined_date": date.fromisoformat(d.joined_date),
            "rating": _scaled(d.rating, "0.01"),
        }
        for _, d in sorted(state.drivers.items())
    ]
    write_parquet(folder / "drivers" / PART_FILE, DRIVER_SCHEMA, drivers)

    orders = [
        (
            o.order_created_ts,
            {
                "order_id": o.order_id,
                "customer_id": o.customer_id,
                "driver_id": o.driver_id,
                "order_created_ts": _ts(o.order_created_ts),
                "pickup_ts": _ts(o.pickup_ts),
                "delivery_ts": _ts(o.delivery_ts),
                "promised_ts": _ts(o.promised_ts),
                "distance_km": _scaled(o.distance_km, "0.001"),
                "status": o.status,
                "city": o.city,
                "fare_amount": _scaled(o.fare_amount, "0.01"),
                "source_updated_at": _ts(o.source_updated_at),
                "delivery_minutes": o.delivery_minutes(),
                "is_late": o.is_late(),
            },
        )
        for _, o in sorted(state.orders.items())
    ]
    _write_partitioned(folder / SOURCE_ORDERS, ORDER_SCHEMA, orders)

    events = [
        (
            e.event_ts,
            {
                "event_id": e.event_id,
                "order_id": e.order_id,
                "driver_id": e.driver_id,
                "event_type": e.event_type,
                "event_ts": _ts(e.event_ts),
                "lat": e.lat,
                "lon": e.lon,
            },
        )
        for _, e in sorted(state.delivery_events.items())
    ]
    _write_partitioned(folder / SOURCE_DELIVERY_EVENTS, DELIVERY_EVENT_SCHEMA, events)

    shifts = [
        (
            s.event_ts,
            {
                "event_id": s.event_id,
                "driver_id": s.driver_id,
                "event_type": s.event_type,
                "event_ts": _ts(s.event_ts),
            },
        )
        for _, s in sorted(state.shift_events.items())
    ]
    _write_partitioned(folder / SOURCE_SHIFT_EVENTS, SHIFT_EVENT_SCHEMA, shifts)
