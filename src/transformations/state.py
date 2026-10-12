"""Cross-batch pipeline state: the interface and a local, file-based implementation.

Dispositions depend on what earlier batches did: accepted drivers and order versions, event ids
already seen, accepted GPS pings per driver, and pending orphans. :class:`PipelineState` is the
interface the pipeline uses. :class:`LocalState` keeps it in memory and saves it as Parquet
under ``data/processed/<dataset>/state/`` after each batch. Task 5 adds a PostgreSQL
implementation of the same interface.
"""

import bisect
import json
import shutil
import tempfile
from dataclasses import asdict, dataclass, replace
from decimal import Decimal
from pathlib import Path
from typing import Any, Protocol, Self

import pyarrow as pa
import pyarrow.parquet as pq

from src.transformations.gps import Ping
from src.transformations.rules import DriverRecord, EventRecord, OrderRecord, ShiftRecord

ORDER_KEY_SEPARATOR = "@"


class BatchSequenceError(RuntimeError):
    """Batches must be processed strictly in sequence."""


@dataclass(frozen=True)
class OrderVersion:
    """The accepted version of an order."""

    source_updated_at: int
    content_hash: str


@dataclass(frozen=True)
class PendingRecord:
    """A valid record waiting for its parent (order or driver).

    ``record_key`` is the event id for events, and ``<order_id>@<source_updated_at>`` for
    orders, so two pending versions of one order never collide. ``first_seen_at`` and
    ``last_retry_at`` are logical times (the end of the batch window), never the wall clock, so
    outputs stay deterministic.
    """

    source: str
    record_key: str
    batch_sequence: int
    batch_id: str
    row_number: int
    missing_reference: str
    fields_json: str
    raw: str
    first_seen_at: int
    retry_count: int = 0
    last_retry_at: int | None = None

    def fields(self) -> dict[str, str]:
        """The contract fields of the held record."""
        loaded: dict[str, str] = json.loads(self.fields_json)
        return loaded

    def retried(self, at: int, missing_reference: str) -> Self:
        """Copy after one more unsuccessful retry."""
        return replace(
            self,
            retry_count=self.retry_count + 1,
            last_retry_at=at,
            missing_reference=missing_reference,
        )


class PipelineState(Protocol):
    """State the pipeline reads and updates while processing batches in sequence."""

    def last_batch(self) -> int: ...

    def mark_processed(self, sequence: int, batch_id: str) -> None: ...

    def driver_hash(self, driver_id: str) -> str | None: ...

    def accept_driver(self, record: DriverRecord) -> None: ...

    def order_version(self, order_id: str) -> OrderVersion | None: ...

    def accept_order(self, record: OrderRecord) -> None: ...

    def event_hash(self, source: str, event_id: str) -> str | None: ...

    def mark_event(self, source: str, event_id: str, fields_hash: str) -> None: ...

    def accept_delivery_event(self, record: EventRecord) -> None: ...

    def accept_shift_event(self, record: ShiftRecord) -> None: ...

    def previous_ping(self, driver_id: str, event_ts: int, event_id: str) -> Ping | None: ...

    def add_ping(self, driver_id: str, ping: Ping) -> None: ...

    def pending(self) -> list[PendingRecord]: ...

    def put_pending(self, record: PendingRecord) -> None: ...

    def remove_pending(self, source: str, record_key: str) -> None: ...


def _ping_key(ping: Ping) -> tuple[int, str]:
    return (ping.event_ts, ping.event_id)


class LocalState:
    """In-memory :class:`PipelineState`, saved to and loaded from Parquet files."""

    def __init__(self) -> None:
        self.batches: list[tuple[int, str]] = []
        self.drivers: dict[str, DriverRecord] = {}
        self.driver_hashes: dict[str, str] = {}
        self.orders: dict[str, OrderRecord] = {}
        self.seen: dict[tuple[str, str], str] = {}
        self.delivery_events: dict[str, EventRecord] = {}
        self.shift_events: dict[str, ShiftRecord] = {}
        self.pings: dict[str, list[Ping]] = {}
        self.pending_records: dict[tuple[str, str], PendingRecord] = {}

    # ---------------------------------------------------------------- batches ---

    def last_batch(self) -> int:
        return self.batches[-1][0] if self.batches else 0

    def mark_processed(self, sequence: int, batch_id: str) -> None:
        if sequence != self.last_batch() + 1:
            raise BatchSequenceError(
                f"batch {sequence} cannot follow batch {self.last_batch()}; "
                "batches must be processed in sequence"
            )
        self.batches.append((sequence, batch_id))

    # ---------------------------------------------------------------- records ---

    def driver_hash(self, driver_id: str) -> str | None:
        return self.driver_hashes.get(driver_id)

    def accept_driver(self, record: DriverRecord) -> None:
        self.drivers[record.driver_id] = record
        self.driver_hashes[record.driver_id] = record.content_hash()

    def order_version(self, order_id: str) -> OrderVersion | None:
        record = self.orders.get(order_id)
        if record is None:
            return None
        return OrderVersion(record.source_updated_at, record.content_hash())

    def accept_order(self, record: OrderRecord) -> None:
        self.orders[record.order_id] = record

    def event_hash(self, source: str, event_id: str) -> str | None:
        return self.seen.get((source, event_id))

    def mark_event(self, source: str, event_id: str, fields_hash: str) -> None:
        self.seen[(source, event_id)] = fields_hash

    def accept_delivery_event(self, record: EventRecord) -> None:
        self.delivery_events[record.event_id] = record

    def accept_shift_event(self, record: ShiftRecord) -> None:
        self.shift_events[record.event_id] = record

    # ------------------------------------------------------------------- GPS ---

    def previous_ping(self, driver_id: str, event_ts: int, event_id: str) -> Ping | None:
        pings = self.pings.get(driver_id, [])
        index = bisect.bisect_left(pings, (event_ts, event_id), key=_ping_key)
        return pings[index - 1] if index > 0 else None

    def add_ping(self, driver_id: str, ping: Ping) -> None:
        bisect.insort(self.pings.setdefault(driver_id, []), ping, key=_ping_key)

    # --------------------------------------------------------------- pending ---

    def pending(self) -> list[PendingRecord]:
        return sorted(
            self.pending_records.values(),
            key=lambda r: (r.batch_sequence, r.source, r.row_number, r.record_key),
        )

    def put_pending(self, record: PendingRecord) -> None:
        self.pending_records[(record.source, record.record_key)] = record

    def remove_pending(self, source: str, record_key: str) -> None:
        del self.pending_records[(source, record_key)]

    # ----------------------------------------------------------- persistence ---

    def save(self, folder: Path) -> None:
        """Write the state to ``folder`` atomically (temp folder, then rename)."""
        folder.parent.mkdir(parents=True, exist_ok=True)
        staging = Path(tempfile.mkdtemp(prefix=f".{folder.name}-tmp-", dir=folder.parent))
        try:
            for name, (schema, rows) in self._tables().items():
                table = pa.Table.from_pylist(rows, schema=schema)
                pq.write_table(table, staging / f"{name}.parquet")
            if folder.exists():
                shutil.rmtree(folder)
            staging.rename(folder)
        except BaseException:
            shutil.rmtree(staging, ignore_errors=True)
            raise

    @classmethod
    def load(cls, folder: Path) -> Self:
        """Read a state saved with :meth:`save`."""
        state = cls()
        tables = {name: pq.read_table(folder / f"{name}.parquet").to_pylist() for name in _SCHEMAS}
        state.batches = [(int(r["sequence"]), str(r["batch_id"])) for r in tables["batches"]]
        for row in tables["drivers"]:
            rating = row["rating"]
            state.accept_driver(
                DriverRecord(
                    driver_id=row["driver_id"],
                    city=row["city"],
                    vehicle_type=row["vehicle_type"],
                    joined_date=row["joined_date"],
                    rating=None if rating is None else Decimal(rating),
                )
            )
        for row in tables["orders"]:
            row["distance_km"] = Decimal(row["distance_km"])
            row["fare_amount"] = Decimal(row["fare_amount"])
            state.accept_order(OrderRecord(**row))
        state.seen = {(r["source"], r["event_id"]): r["fields_hash"] for r in tables["seen"]}
        for row in tables["delivery_events"]:
            state.accept_delivery_event(EventRecord(**row))
        for row in tables["shift_events"]:
            state.accept_shift_event(ShiftRecord(**row))
        for row in tables["pings"]:
            driver_id = row.pop("driver_id")
            state.pings.setdefault(driver_id, []).append(Ping(**row))
        for pings in state.pings.values():
            pings.sort(key=_ping_key)
        for row in tables["pending"]:
            state.put_pending(PendingRecord(**row))
        return state

    def _tables(self) -> dict[str, tuple[pa.Schema, list[dict[str, Any]]]]:
        drivers = [
            {**asdict(r), "rating": None if r.rating is None else str(r.rating)}
            for _, r in sorted(self.drivers.items())
        ]
        orders = [
            {**asdict(r), "distance_km": str(r.distance_km), "fare_amount": str(r.fare_amount)}
            for _, r in sorted(self.orders.items())
        ]
        pings = [
            {"driver_id": driver_id, **asdict(ping)}
            for driver_id in sorted(self.pings)
            for ping in self.pings[driver_id]
        ]
        rows: dict[str, list[dict[str, Any]]] = {
            "batches": [{"sequence": s, "batch_id": b} for s, b in self.batches],
            "drivers": drivers,
            "orders": orders,
            "seen": [
                {"source": s, "event_id": e, "fields_hash": h}
                for (s, e), h in sorted(self.seen.items())
            ],
            "delivery_events": [asdict(r) for _, r in sorted(self.delivery_events.items())],
            "shift_events": [asdict(r) for _, r in sorted(self.shift_events.items())],
            "pings": pings,
            "pending": [asdict(r) for r in self.pending()],
        }
        return {name: (_SCHEMAS[name], rows[name]) for name in _SCHEMAS}


_SCHEMAS: dict[str, pa.Schema] = {
    "batches": pa.schema([("sequence", pa.int64()), ("batch_id", pa.string())]),
    "drivers": pa.schema(
        [
            ("driver_id", pa.string()),
            ("city", pa.string()),
            ("vehicle_type", pa.string()),
            ("joined_date", pa.string()),
            ("rating", pa.string()),
        ]
    ),
    "orders": pa.schema(
        [
            ("order_id", pa.string()),
            ("customer_id", pa.string()),
            ("driver_id", pa.string()),
            ("order_created_ts", pa.int64()),
            ("pickup_ts", pa.int64()),
            ("delivery_ts", pa.int64()),
            ("promised_ts", pa.int64()),
            ("distance_km", pa.string()),
            ("status", pa.string()),
            ("city", pa.string()),
            ("fare_amount", pa.string()),
            ("source_updated_at", pa.int64()),
        ]
    ),
    "seen": pa.schema(
        [("source", pa.string()), ("event_id", pa.string()), ("fields_hash", pa.string())]
    ),
    "delivery_events": pa.schema(
        [
            ("event_id", pa.string()),
            ("order_id", pa.string()),
            ("driver_id", pa.string()),
            ("event_type", pa.string()),
            ("event_ts", pa.int64()),
            ("lat", pa.float64()),
            ("lon", pa.float64()),
        ]
    ),
    "shift_events": pa.schema(
        [
            ("event_id", pa.string()),
            ("driver_id", pa.string()),
            ("event_type", pa.string()),
            ("event_ts", pa.int64()),
        ]
    ),
    "pings": pa.schema(
        [
            ("driver_id", pa.string()),
            ("event_ts", pa.int64()),
            ("event_id", pa.string()),
            ("lat", pa.float64()),
            ("lon", pa.float64()),
        ]
    ),
    "pending": pa.schema(
        [
            ("source", pa.string()),
            ("record_key", pa.string()),
            ("batch_sequence", pa.int64()),
            ("batch_id", pa.string()),
            ("row_number", pa.int64()),
            ("missing_reference", pa.string()),
            ("fields_json", pa.string()),
            ("raw", pa.string()),
            ("first_seen_at", pa.int64()),
            ("retry_count", pa.int64()),
            ("last_retry_at", pa.int64()),
        ]
    ),
}
