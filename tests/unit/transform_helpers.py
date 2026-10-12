"""Helpers to build small handmade batches for transform unit tests."""

import json
from collections.abc import Sequence

from src.contract import (
    SOURCE_COLUMNS,
    SOURCE_DELIVERY_EVENTS,
    SOURCE_DRIVERS,
    SOURCE_ORDERS,
    SOURCE_SHIFT_EVENTS,
)
from src.transformations.pipeline import BatchResult, TransformConfig, process_batch
from src.transformations.records import Batch, RawRecord
from src.transformations.state import LocalState

Fields = dict[str, str]

DRIVER: Fields = {
    "driver_id": "DRV-00001",
    "city": "Delhi",
    "vehicle_type": "motorbike",
    "joined_date": "2025-01-01",
    "rating": "4.50",
}

DELIVERED_ORDER: Fields = {
    "order_id": "ORD-0000001",
    "customer_id": "CUST-000001",
    "driver_id": "DRV-00001",
    "order_created_ts": "2026-09-10T10:00:00Z",
    "pickup_ts": "2026-09-10T10:15:00Z",
    "delivery_ts": "2026-09-10T10:40:00Z",
    "promised_ts": "2026-09-10T10:45:00Z",
    "distance_km": "4.200",
    "status": "delivered",
    "city": "Delhi",
    "fare_amount": "67.00",
    "source_updated_at": "2026-09-10T10:40:00Z",
}

ASSIGNED_ORDER: Fields = {
    **DELIVERED_ORDER,
    "pickup_ts": "",
    "delivery_ts": "",
    "status": "assigned",
    "source_updated_at": "2026-09-10T10:02:00Z",
}


def order(**overrides: str) -> Fields:
    """A valid delivered order with some fields replaced."""
    return {**DELIVERED_ORDER, **overrides}


def ping(
    event_id: str,
    ts: str,
    lat: str,
    lon: str,
    order_id: str = "ORD-0000001",
    driver_id: str = "DRV-00001",
    event_type: str = "gps_ping",
) -> Fields:
    """A delivery event (a GPS ping unless ``event_type`` says otherwise)."""
    return {
        "event_id": event_id,
        "order_id": order_id,
        "driver_id": driver_id,
        "event_type": event_type,
        "event_ts": ts,
        "lat": lat,
        "lon": lon,
    }


def shift(event_id: str, ts: str, driver_id: str = "DRV-00001", kind: str = "online") -> Fields:
    """A shift event."""
    return {"event_id": event_id, "driver_id": driver_id, "event_type": kind, "event_ts": ts}


def raw_record(source: str, row_number: int, fields: Fields) -> RawRecord:
    """A parsed raw record whose raw payload is the canonical JSON of its fields."""
    full = {**dict.fromkeys(SOURCE_COLUMNS[source], ""), **fields}
    return RawRecord(source, row_number, full, json.dumps(full, sort_keys=True))


def make_batch(
    sequence: int,
    drivers: Sequence[Fields] = (),
    orders: Sequence[Fields] = (),
    events: Sequence[Fields] = (),
    shifts: Sequence[Fields] = (),
) -> Batch:
    """A batch built from field dicts, rows numbered from 1 per source."""
    rows = {
        SOURCE_DRIVERS: drivers,
        SOURCE_ORDERS: orders,
        SOURCE_DELIVERY_EVENTS: events,
        SOURCE_SHIFT_EVENTS: shifts,
    }
    records = {
        source: [raw_record(source, n, fields) for n, fields in enumerate(items, start=1)]
        for source, items in rows.items()
    }
    window_start = 1_000 * sequence
    return Batch(sequence, f"batch-id-{sequence}", window_start, window_start + 1_000, records)


def run(
    batches: Sequence[Batch], config: TransformConfig | None = None
) -> tuple[list[BatchResult], LocalState]:
    """Process ``batches`` in order from fresh state."""
    state = LocalState()
    results = [process_batch(batch, state, config or TransformConfig()) for batch in batches]
    return results, state


def outcome(result: BatchResult, source: str, row_number: int) -> tuple[str, str]:
    """(disposition, reason) of one input row."""
    matches = [
        (d.disposition, d.reason)
        for d in result.dispositions
        if d.source == source and d.row_number == row_number
    ]
    assert len(matches) == 1, matches
    return matches[0]
