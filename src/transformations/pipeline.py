"""Process one raw batch into exactly one disposition per input row.

Fixed step order within a batch (docs/data_contracts.md, section 7):

1. drivers of the batch
2. retry pending orders (their driver may have arrived); expire old ones
3. orders of the batch
4. retry pending delivery and shift events (parents may have arrived); expire old ones
5. delivery events of the batch (GPS checks per driver in (event_ts, event_id) order)
6. shift events of the batch

Every source starts with: malformed record -> rejected; exact copy of an earlier row of the same
batch -> duplicate. Retried pending records are reported as resolutions, never as input rows of
the current batch.
"""

import json
import logging
import time
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass, field

from src.contract import (
    ACCEPTED,
    CONFLICTING_VERSION,
    DISPOSITIONS,
    DUPLICATE,
    MALFORMED_RECORD,
    MAX_PLAUSIBLE_SPEED_KMH,
    MISSING_DRIVER,
    MISSING_ORDER,
    OLDER_SOURCE_UPDATED_AT,
    ORPHAN_TIMEOUT,
    PENDING,
    REJECTED,
    SOURCE_DELIVERY_EVENTS,
    SOURCE_DRIVERS,
    SOURCE_FILES,
    SOURCE_ORDERS,
    SOURCE_SHIFT_EVENTS,
    STALE_IGNORED,
)
from src.transformations.gps import Ping, check_ping
from src.transformations.records import Batch, RawRecord
from src.transformations.rules import (
    EventRecord,
    OrderRecord,
    fields_hash,
    validate_driver,
    validate_event_basics,
    validate_event_coordinates,
    validate_order,
    validate_shift_event,
)
from src.transformations.state import (
    ORDER_KEY_SEPARATOR,
    BatchSequenceError,
    PendingRecord,
    PipelineState,
)

logger = logging.getLogger("deliverypulse.transform")

DEFAULT_ORPHAN_MAX_AGE_BATCHES = 2
PARENT_ORDER = "order"
PARENT_DRIVER = "driver"
RESOLVED = "resolved"
EXPIRED = "expired"
_SOURCE_ORDER = {source: position for position, (source, _) in enumerate(SOURCE_FILES)}


@dataclass(frozen=True)
class TransformConfig:
    """Configurable thresholds of the pipeline."""

    max_speed_kmh: float = MAX_PLAUSIBLE_SPEED_KMH
    orphan_max_age_batches: int = DEFAULT_ORPHAN_MAX_AGE_BATCHES


@dataclass(frozen=True)
class Disposition:
    """The outcome for one input row. ``raw`` is kept for every non-accepted row."""

    source: str
    row_number: int
    record_key: str
    disposition: str
    reason: str
    raw: str


@dataclass(frozen=True)
class Resolution:
    """A pending record that left the pending queue in this batch."""

    record: PendingRecord
    outcome: str
    disposition: str
    reason: str


@dataclass
class BatchResult:
    """Everything one batch produced."""

    sequence: int
    batch_id: str
    window_end: int
    dispositions: list[Disposition] = field(default_factory=list)
    resolutions: list[Resolution] = field(default_factory=list)
    pending_after: list[PendingRecord] = field(default_factory=list)
    rows_input: dict[str, int] = field(default_factory=dict)

    def counts(self, source: str) -> dict[str, int]:
        """Disposition counts of ``source`` (every disposition present, zero if unused)."""
        counter = Counter(d.disposition for d in self.dispositions if d.source == source)
        return {disposition: counter[disposition] for disposition in DISPOSITIONS}


def _missing_parent(state: PipelineState, source: str, fields: dict[str, str]) -> str | None:
    """Which parent of a record is not accepted yet ("order", "driver"), or None."""
    if source == SOURCE_DELIVERY_EVENTS and state.order_version(fields["order_id"]) is None:
        return PARENT_ORDER
    driver_id = fields["driver_id"].strip()
    if driver_id and state.driver_hash(driver_id) is None:
        return PARENT_DRIVER
    return None


class _BatchProcessor:
    def __init__(self, batch: Batch, state: PipelineState, config: TransformConfig) -> None:
        self.batch = batch
        self.state = state
        self.config = config
        self.result = BatchResult(batch.sequence, batch.batch_id, batch.window_end)

    # ----------------------------------------------------------------- helpers ---

    def record(self, rec: RawRecord, disposition: str, reason: str = "") -> None:
        raw = "" if disposition == ACCEPTED else rec.raw
        self.result.dispositions.append(
            Disposition(rec.source, rec.row_number, rec.key, disposition, reason, raw)
        )

    def reject(self, rec: RawRecord, reason: str | None) -> None:
        self.record(rec, REJECTED, reason or MALFORMED_RECORD)

    def pend(self, rec: RawRecord, record_key: str, parent: str) -> None:
        self.state.put_pending(
            PendingRecord(
                source=rec.source,
                record_key=record_key,
                batch_sequence=self.batch.sequence,
                batch_id=self.batch.batch_id,
                row_number=rec.row_number,
                missing_reference=parent,
                fields_json=json.dumps(rec.fields, sort_keys=True, ensure_ascii=True),
                raw=rec.raw,
                first_seen_at=self.batch.window_end,
            )
        )
        self.record(rec, PENDING, MISSING_ORDER if parent == PARENT_ORDER else MISSING_DRIVER)

    def prefilter(self, source: str) -> list[RawRecord]:
        """Malformed rows are rejected; exact copies of an earlier row are duplicates."""
        seen_raw: set[str] = set()
        remaining: list[RawRecord] = []
        for rec in self.batch.records[source]:
            if rec.parse_error:
                self.reject(rec, MALFORMED_RECORD)
            elif rec.raw in seen_raw:
                self.record(rec, DUPLICATE)
            else:
                seen_raw.add(rec.raw)
                remaining.append(rec)
        return remaining

    def apply_order_version(self, order: OrderRecord) -> tuple[str, str]:
        """Compare an order version with the accepted state; accept it if it is newer."""
        current = self.state.order_version(order.order_id)
        if current is None or current.source_updated_at < order.source_updated_at:
            self.state.accept_order(order)
            return ACCEPTED, ""
        if current.source_updated_at == order.source_updated_at:
            if current.content_hash == order.content_hash():
                return DUPLICATE, ""
            return REJECTED, CONFLICTING_VERSION
        return STALE_IGNORED, OLDER_SOURCE_UPDATED_AT

    # ----------------------------------------------------------------- drivers ---

    def drivers(self) -> None:
        for rec in self.prefilter(SOURCE_DRIVERS):
            reason, driver = validate_driver(rec.fields)
            if driver is None:
                self.reject(rec, reason)
                continue
            known = self.state.driver_hash(driver.driver_id)
            if known is None:
                self.state.accept_driver(driver)
                self.record(rec, ACCEPTED)
            elif known == driver.content_hash():
                self.record(rec, DUPLICATE)
            else:
                self.record(rec, REJECTED, CONFLICTING_VERSION)

    # ----------------------------------------------------------------- pending ---

    def retry_pending(self, sources: tuple[str, ...]) -> None:
        for pending in self.state.pending():
            if pending.source not in sources:
                continue
            fields = pending.fields()
            parent = _missing_parent(self.state, pending.source, fields)
            age = self.batch.sequence - pending.batch_sequence
            if parent is None:
                self.state.remove_pending(pending.source, pending.record_key)
                disposition, reason = self.resolve(pending.source, fields)
                self.result.resolutions.append(Resolution(pending, RESOLVED, disposition, reason))
            elif age > self.config.orphan_max_age_batches:
                self.state.remove_pending(pending.source, pending.record_key)
                self.result.resolutions.append(
                    Resolution(pending, EXPIRED, REJECTED, ORPHAN_TIMEOUT)
                )
            else:
                self.state.put_pending(pending.retried(self.batch.window_end, parent))

    def resolve(self, source: str, fields: dict[str, str]) -> tuple[str, str]:
        """Accept a pending record whose parent now exists (it already passed every check)."""
        if source == SOURCE_ORDERS:
            _, order = validate_order(fields)
            if order is None:
                return REJECTED, MALFORMED_RECORD
            return self.apply_order_version(order)
        if source == SOURCE_DELIVERY_EVENTS:
            _, ts = validate_event_basics(fields)
            event = None if ts is None else validate_event_coordinates(fields, ts)[1]
            if event is None:
                return REJECTED, MALFORMED_RECORD
            self.state.accept_delivery_event(event)
            return ACCEPTED, ""
        _, shift = validate_shift_event(fields)
        if shift is None:
            return REJECTED, MALFORMED_RECORD
        self.state.accept_shift_event(shift)
        return ACCEPTED, ""

    # ------------------------------------------------------------------ orders ---

    def orders(self) -> None:
        groups: dict[str, list[tuple[RawRecord, OrderRecord]]] = {}
        for rec in self.prefilter(SOURCE_ORDERS):
            reason, order = validate_order(rec.fields)
            if order is None:
                self.reject(rec, reason)
                continue
            if order.driver_id is not None and self.state.driver_hash(order.driver_id) is None:
                key = f"{order.order_id}{ORDER_KEY_SEPARATOR}{rec.fields['source_updated_at']}"
                self.pend(rec, key, PARENT_DRIVER)
                continue
            groups.setdefault(order.order_id, []).append((rec, order))
        for rows in groups.values():
            self.order_group(rows)

    def order_group(self, rows: list[tuple[RawRecord, OrderRecord]]) -> None:
        """Version logic for all valid rows of one order in this batch."""
        by_version: dict[int, list[tuple[RawRecord, OrderRecord]]] = {}
        for rec, order in rows:
            by_version.setdefault(order.source_updated_at, []).append((rec, order))
        clean: dict[int, list[tuple[RawRecord, OrderRecord]]] = {}
        for version, version_rows in by_version.items():
            if len({order.content_hash() for _, order in version_rows}) > 1:
                for rec, _ in version_rows:
                    self.record(rec, REJECTED, CONFLICTING_VERSION)
            else:
                clean[version] = version_rows
        if not clean:
            return
        newest = max(clean)
        for version, version_rows in clean.items():
            if version != newest:
                for rec, _ in version_rows:
                    self.record(rec, STALE_IGNORED, OLDER_SOURCE_UPDATED_AT)
        first_rec, first_order = clean[newest][0]
        for rec, _ in clean[newest][1:]:
            self.record(rec, DUPLICATE)
        disposition, reason = self.apply_order_version(first_order)
        self.record(first_rec, disposition, reason)

    # ---------------------------------------------------------- delivery events ---

    def delivery_events(self) -> None:
        source = SOURCE_DELIVERY_EVENTS
        first_in_batch: dict[str, str] = {}
        candidates: list[tuple[RawRecord, EventRecord, str]] = []
        for rec in self.prefilter(source):
            reason, ts = validate_event_basics(rec.fields)
            if ts is None:
                self.reject(rec, reason)
                continue
            digest = fields_hash(rec.fields)
            known = self.state.event_hash(source, rec.key) or first_in_batch.get(rec.key)
            if known is not None:
                self.repeated(rec, known, digest)
                continue
            first_in_batch[rec.key] = digest
            reason, event = validate_event_coordinates(rec.fields, ts)
            if event is None:
                self.reject(rec, reason)
                continue
            candidates.append((rec, event, digest))

        gps_rejected: set[int] = set()
        pings = sorted(
            (c for c in candidates if c[1].event_type == "gps_ping"),
            key=lambda c: (c[1].driver_id, c[1].event_ts, c[1].event_id),
        )
        for rec, event, _ in pings:
            if event.lat is None or event.lon is None:
                continue
            ping = Ping(event.event_ts, event.event_id, event.lat, event.lon)
            previous = self.state.previous_ping(event.driver_id, event.event_ts, event.event_id)
            gps_reason = check_ping(previous, ping, self.config.max_speed_kmh)
            if gps_reason is None:
                self.state.add_ping(event.driver_id, ping)
            else:
                self.record(rec, REJECTED, gps_reason)
                gps_rejected.add(rec.row_number)

        for rec, event, digest in candidates:
            if rec.row_number in gps_rejected:
                continue
            self.state.mark_event(source, event.event_id, digest)
            parent = _missing_parent(self.state, source, rec.fields)
            if parent is None:
                self.state.accept_delivery_event(event)
                self.record(rec, ACCEPTED)
            else:
                self.pend(rec, event.event_id, parent)

    def repeated(self, rec: RawRecord, known: str, digest: str) -> None:
        """An event id seen before: identical content is a duplicate, different is a conflict."""
        if known == digest:
            self.record(rec, DUPLICATE)
        else:
            self.record(rec, REJECTED, CONFLICTING_VERSION)

    # ------------------------------------------------------------- shift events ---

    def shift_events(self) -> None:
        source = SOURCE_SHIFT_EVENTS
        first_in_batch: dict[str, str] = {}
        for rec in self.prefilter(source):
            reason, shift = validate_shift_event(rec.fields)
            if shift is None:
                self.reject(rec, reason)
                continue
            digest = fields_hash(rec.fields)
            known = self.state.event_hash(source, rec.key) or first_in_batch.get(rec.key)
            if known is not None:
                self.repeated(rec, known, digest)
                continue
            first_in_batch[rec.key] = digest
            self.state.mark_event(source, shift.event_id, digest)
            parent = _missing_parent(self.state, source, rec.fields)
            if parent is None:
                self.state.accept_shift_event(shift)
                self.record(rec, ACCEPTED)
            else:
                self.pend(rec, shift.event_id, parent)

    # --------------------------------------------------------------------- run ---

    def timed(self, source: str, step: Callable[[], None]) -> None:
        """Run the step for one source and log rows in, every disposition count and duration."""
        started = time.perf_counter()
        step()
        elapsed = time.perf_counter() - started
        counts = self.result.counts(source)
        logger.info(
            "batch %d %s: %d rows in; %s; %.2f s",
            self.batch.sequence,
            source,
            len(self.batch.records[source]),
            ", ".join(f"{name}={count}" for name, count in counts.items()),
            elapsed,
        )

    def timed_retry(self, label: str, sources: tuple[str, ...]) -> None:
        """Retry pending records of ``sources``; log counts for those sources only."""
        started = time.perf_counter()
        self.retry_pending(sources)
        elapsed = time.perf_counter() - started
        outcomes = Counter(r.outcome for r in self.result.resolutions if r.record.source in sources)
        still_pending = sum(1 for p in self.state.pending() if p.source in sources)
        logger.info(
            "batch %d %s: resolved=%d, expired=%d, still pending=%d; %.2f s",
            self.batch.sequence,
            label,
            outcomes[RESOLVED],
            outcomes[EXPIRED],
            still_pending,
            elapsed,
        )

    def run(self) -> BatchResult:
        self.timed(SOURCE_DRIVERS, self.drivers)
        self.timed_retry("pending orders", (SOURCE_ORDERS,))
        self.timed(SOURCE_ORDERS, self.orders)
        self.timed_retry("pending events", (SOURCE_DELIVERY_EVENTS, SOURCE_SHIFT_EVENTS))
        self.timed(SOURCE_DELIVERY_EVENTS, self.delivery_events)
        self.timed(SOURCE_SHIFT_EVENTS, self.shift_events)
        self.result.dispositions.sort(key=lambda d: (_SOURCE_ORDER[d.source], d.row_number))
        self.result.rows_input = {
            source: len(self.batch.records[source]) for source, _ in SOURCE_FILES
        }
        self.result.pending_after = self.state.pending()
        return self.result


def process_batch(batch: Batch, state: PipelineState, config: TransformConfig) -> BatchResult:
    """Process ``batch`` against ``state`` (updated in place); batches must come in sequence."""
    if batch.sequence != state.last_batch() + 1:
        raise BatchSequenceError(
            f"batch {batch.sequence} cannot follow batch {state.last_batch()}; "
            "batches must be processed in sequence"
        )
    result = _BatchProcessor(batch, state, config).run()
    state.mark_processed(batch.sequence, batch.batch_id)
    return result
