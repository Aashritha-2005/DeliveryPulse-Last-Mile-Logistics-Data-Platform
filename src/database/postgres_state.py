"""PostgreSQL implementation of the pipeline state (:class:`PipelineState`).

The pipeline asks the state one question per row. Answering each with a query would mean
hundreds of thousands of round trips, so :class:`PostgresState`:

* reads once per batch (:meth:`PostgresState.prefetch`) only the keys that the batch and the
  pending queue reference, plus each driver's GPS predecessor candidates;
* answers lookups from those caches, which also hold the writes made earlier in the same batch
  (a key that was not prefetched falls back to a single query);
* buffers every write; the loader flushes them inside the batch transaction.

Memory therefore scales with one batch, not with the whole history.
"""

import bisect
import json
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import LiteralString

import psycopg
from psycopg.rows import TupleRow

from src.contract import (
    SOURCE_DELIVERY_EVENTS,
    SOURCE_DRIVERS,
    SOURCE_ORDERS,
    SOURCE_SHIFT_EVENTS,
    parse_utc,
)
from src.transformations.gps import Ping
from src.transformations.records import Batch
from src.transformations.rules import DriverRecord, EventRecord, OrderRecord, ShiftRecord
from src.transformations.state import BatchSequenceError, OrderVersion, PendingRecord

Connection = psycopg.Connection[TupleRow]

STATUS_SUCCEEDED = "succeeded"

_STATE_QUERY: LiteralString = (
    "SELECT record_key, version_ts, content_hash FROM staging.record_state "
    "WHERE source = %s AND record_key = ANY(%s)"
)
# For each driver: every reference ping at or after the latest one before t0 (t0 = the
# driver's earliest ping in the batch). That covers every predecessor the batch can need.
_PING_QUERY: LiteralString = """
    SELECT d.driver_id, g.event_ts, g.event_id, g.lat, g.lon
    FROM unnest(%s::text[], %s::timestamptz[]) AS d(driver_id, t0)
    CROSS JOIN LATERAL (
        SELECT max(r.event_ts) AS before_ts
        FROM staging.gps_reference r
        WHERE r.driver_id = d.driver_id AND r.event_ts < d.t0
    ) AS b
    JOIN staging.gps_reference g
      ON g.driver_id = d.driver_id AND g.event_ts >= coalesce(b.before_ts, d.t0)
"""
_PENDING_QUERY: LiteralString = """
    SELECT source, record_key, batch_sequence, batch_id, row_number, missing_reference,
           fields, record_payload::text, first_seen_at, retry_count, last_retry_at
    FROM staging.pending_events
"""


def to_timestamp(epoch: int) -> datetime:
    """Epoch seconds as an aware UTC datetime (for timestamptz columns)."""
    return datetime.fromtimestamp(epoch, UTC)


def to_epoch(value: datetime) -> int:
    """An aware datetime (from a timestamptz column) as epoch seconds."""
    return int(value.timestamp())


def _ping_key(ping: Ping) -> tuple[int, str]:
    return (ping.event_ts, ping.event_id)


@dataclass
class NeededKeys:
    """Keys the pipeline will look up while processing one batch."""

    drivers: set[str]
    orders: set[str]
    delivery_events: set[str]
    shift_events: set[str]
    ping_starts: dict[str, int]


def needed_keys(batch: Batch, pending: Sequence[PendingRecord]) -> NeededKeys:
    """Every state key that processing ``batch`` (and retrying ``pending``) can look up."""
    keys = NeededKeys(set(), set(), set(), set(), {})

    def add_driver(value: str) -> None:
        keys.drivers.update({value, value.strip()})

    for rec in batch.records[SOURCE_DRIVERS]:
        add_driver(rec.fields["driver_id"])
    for rec in batch.records[SOURCE_ORDERS]:
        keys.orders.add(rec.fields["order_id"])
        add_driver(rec.fields["driver_id"])
    for rec in batch.records[SOURCE_DELIVERY_EVENTS]:
        keys.delivery_events.add(rec.key)
        keys.orders.add(rec.fields["order_id"])
        add_driver(rec.fields["driver_id"])
        ts = parse_utc(rec.fields["event_ts"])
        if rec.fields["event_type"] == "gps_ping" and ts is not None:
            driver_id = rec.fields["driver_id"]
            keys.ping_starts[driver_id] = min(ts, keys.ping_starts.get(driver_id, ts))
    for rec in batch.records[SOURCE_SHIFT_EVENTS]:
        keys.shift_events.add(rec.key)
        add_driver(rec.fields["driver_id"])
    for record in pending:
        fields = record.fields()
        if "order_id" in fields:
            keys.orders.add(fields["order_id"])
        if "driver_id" in fields:
            add_driver(fields["driver_id"])
    return keys


class PostgresState:
    """:class:`PipelineState` backed by ``staging`` / ``audit`` tables, prefetched per batch."""

    def __init__(self, conn: Connection, dataset: str) -> None:
        self.conn = conn
        self.dataset = dataset
        self._drivers: dict[str, str | None] = {}
        self._orders: dict[str, OrderVersion | None] = {}
        self._events: dict[tuple[str, str], str | None] = {}
        self._pings: dict[str, list[Ping]] = {}
        self._ping_t0: dict[str, int] = {}
        self._pending: dict[tuple[str, str], PendingRecord] = {}
        self.processed: tuple[int, str] | None = None
        # Writes buffered for the batch transaction.
        self.new_drivers: dict[str, DriverRecord] = {}
        self.new_orders: dict[str, OrderRecord] = {}
        self.new_delivery_events: dict[str, EventRecord] = {}
        self.new_shift_events: dict[str, ShiftRecord] = {}
        self.new_marks: dict[tuple[str, str], str] = {}
        self.new_pings: list[tuple[str, Ping]] = []

    # ---------------------------------------------------------------- prefetch ---

    def prefetch(self, batch: Batch) -> None:
        """Load the pending queue and every key ``batch`` needs."""
        self._load_pending()
        keys = needed_keys(batch, list(self._pending.values()))
        self._fetch_state(SOURCE_DRIVERS, keys.drivers)
        self._fetch_state(SOURCE_ORDERS, keys.orders)
        self._fetch_state(SOURCE_DELIVERY_EVENTS, keys.delivery_events)
        self._fetch_state(SOURCE_SHIFT_EVENTS, keys.shift_events)
        self._fetch_pings(keys.ping_starts)

    def _load_pending(self) -> None:
        self._pending = {}
        for row in self.conn.execute(_PENDING_QUERY).fetchall():
            record = PendingRecord(
                source=row[0],
                record_key=row[1],
                batch_sequence=row[2],
                batch_id=row[3],
                row_number=row[4],
                missing_reference=row[5],
                fields_json=json.dumps(row[6], sort_keys=True, ensure_ascii=True),
                raw=row[7],
                first_seen_at=to_epoch(row[8]),
                retry_count=row[9],
                last_retry_at=None if row[10] is None else to_epoch(row[10]),
            )
            self._pending[(record.source, record.record_key)] = record

    def _fetch_state(self, source: str, keys: set[str]) -> None:
        wanted = sorted(key for key in keys if not self._cached(source, key))
        if not wanted:
            return
        rows = self.conn.execute(_STATE_QUERY, (source, wanted)).fetchall()
        found = {row[0]: (row[1], row[2]) for row in rows}
        for key in wanted:
            entry = found.get(key)
            if source == SOURCE_DRIVERS:
                self._drivers[key] = None if entry is None else entry[1]
            elif source == SOURCE_ORDERS:
                version = None if entry is None else OrderVersion(to_epoch(entry[0]), entry[1])
                self._orders[key] = version
            else:
                self._events[(source, key)] = None if entry is None else entry[1]

    def _cached(self, source: str, key: str) -> bool:
        if source == SOURCE_DRIVERS:
            return key in self._drivers
        if source == SOURCE_ORDERS:
            return key in self._orders
        return (source, key) in self._events

    def _fetch_pings(self, starts: dict[str, int]) -> None:
        drivers = sorted(starts)
        if not drivers:
            return
        t0s = [to_timestamp(starts[driver]) for driver in drivers]
        fetched: dict[str, list[Ping]] = {driver: [] for driver in drivers}
        for row in self.conn.execute(_PING_QUERY, (drivers, t0s)).fetchall():
            fetched[row[0]].append(Ping(to_epoch(row[1]), row[2], row[3], row[4]))
        for driver in drivers:
            pings = fetched[driver] + [p for d, p in self.new_pings if d == driver]
            self._pings[driver] = sorted(set(pings), key=_ping_key)
            self._ping_t0[driver] = starts[driver]

    def _ensure_pings(self, driver_id: str, event_ts: int) -> list[Ping]:
        if driver_id not in self._pings or event_ts < self._ping_t0[driver_id]:
            self._fetch_pings({driver_id: event_ts})
        return self._pings[driver_id]

    # ----------------------------------------------------------------- batches ---

    def last_batch(self) -> int:
        row = self.conn.execute(
            "SELECT coalesce(max(batch_sequence), 0) FROM audit.pipeline_runs "
            "WHERE dataset = %s AND status = %s",
            (self.dataset, STATUS_SUCCEEDED),
        ).fetchone()
        return 0 if row is None else int(row[0])

    def mark_processed(self, sequence: int, batch_id: str) -> None:
        if sequence != self.last_batch() + 1:
            raise BatchSequenceError(
                f"batch {sequence} cannot follow batch {self.last_batch()}; "
                "batches must be processed in sequence"
            )
        self.processed = (sequence, batch_id)

    # ----------------------------------------------------------------- records ---

    def driver_hash(self, driver_id: str) -> str | None:
        if driver_id not in self._drivers:
            self._fetch_state(SOURCE_DRIVERS, {driver_id})
        return self._drivers[driver_id]

    def accept_driver(self, record: DriverRecord) -> None:
        self._drivers[record.driver_id] = record.content_hash()
        self.new_drivers[record.driver_id] = record

    def order_version(self, order_id: str) -> OrderVersion | None:
        if order_id not in self._orders:
            self._fetch_state(SOURCE_ORDERS, {order_id})
        return self._orders[order_id]

    def accept_order(self, record: OrderRecord) -> None:
        self._orders[record.order_id] = OrderVersion(
            record.source_updated_at, record.content_hash()
        )
        self.new_orders[record.order_id] = record

    def event_hash(self, source: str, event_id: str) -> str | None:
        if (source, event_id) not in self._events:
            self._fetch_state(source, {event_id})
        return self._events[(source, event_id)]

    def mark_event(self, source: str, event_id: str, fields_hash: str) -> None:
        self._events[(source, event_id)] = fields_hash
        self.new_marks[(source, event_id)] = fields_hash

    def accept_delivery_event(self, record: EventRecord) -> None:
        self.new_delivery_events[record.event_id] = record

    def accept_shift_event(self, record: ShiftRecord) -> None:
        self.new_shift_events[record.event_id] = record

    # --------------------------------------------------------------------- GPS ---

    def previous_ping(self, driver_id: str, event_ts: int, event_id: str) -> Ping | None:
        pings = self._ensure_pings(driver_id, event_ts)
        index = bisect.bisect_left(pings, (event_ts, event_id), key=_ping_key)
        return pings[index - 1] if index > 0 else None

    def add_ping(self, driver_id: str, ping: Ping) -> None:
        pings = self._ensure_pings(driver_id, ping.event_ts)
        bisect.insort(pings, ping, key=_ping_key)
        self.new_pings.append((driver_id, ping))

    # ----------------------------------------------------------------- pending ---

    def pending(self) -> list[PendingRecord]:
        return sorted(
            self._pending.values(),
            key=lambda r: (r.batch_sequence, r.source, r.row_number, r.record_key),
        )

    def put_pending(self, record: PendingRecord) -> None:
        self._pending[(record.source, record.record_key)] = record

    def remove_pending(self, source: str, record_key: str) -> None:
        del self._pending[(source, record_key)]
