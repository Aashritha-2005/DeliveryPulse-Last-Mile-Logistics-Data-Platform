"""Verify a defect manifest against the raw files it describes.

Two kinds of evidence:

1. Per case: the record exists in the stated batch file and really shows the defect.
2. Independently of the manifest: simple whole-file counts (identical rows, null coordinates,
   non-canonical statuses, orphans, ...) must equal the manifest's case counts. Three types
   (impossible_gps_speed, out_of_order_event, gps_long_gap) are checked per case only.

Also checks batch identities, that scenarios never share a record, order or driver, and that
every source and batch reconciles: input = accepted + rejected + duplicate + stale + pending.
"""

import csv
import io
import json
import math
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from src.groundtruth.manifest import ACCEPTED, DISPOSITIONS, Case, Manifest
from src.ingestion.generator_config import (
    BATCH_METADATA_FILE,
    DELIVERY_EVENT_FIELDS,
    DRIVER_COLUMNS,
    LONG_GAP_MIN_SECONDS,
    MAX_PLAUSIBLE_SPEED_KMH,
    ORDER_COLUMNS,
    ORDER_STATUSES,
    RATING_MAX,
    RATING_MIN,
    SHIFT_EVENT_COLUMNS,
    SOURCE_FILES,
    VEHICLE_TYPES,
    normalise_status,
    parse_utc,
)
from src.ingestion.simulation import Row, haversine_km
from src.ingestion.writers import compute_batch_id

O_ID = ORDER_COLUMNS.index("order_id")
O_CUSTOMER = ORDER_COLUMNS.index("customer_id")
O_CREATED = ORDER_COLUMNS.index("order_created_ts")
O_PICKUP = ORDER_COLUMNS.index("pickup_ts")
O_DELIVERY = ORDER_COLUMNS.index("delivery_ts")
O_PROMISED = ORDER_COLUMNS.index("promised_ts")
O_DISTANCE = ORDER_COLUMNS.index("distance_km")
O_STATUS = ORDER_COLUMNS.index("status")
O_CITY = ORDER_COLUMNS.index("city")
O_FARE = ORDER_COLUMNS.index("fare_amount")
O_UPDATED = ORDER_COLUMNS.index("source_updated_at")
O_TIMESTAMPS = (O_CREATED, O_PICKUP, O_DELIVERY, O_PROMISED, O_UPDATED)
O_REQUIRED = (O_CUSTOMER, O_CITY, O_PROMISED)
E_ID = DELIVERY_EVENT_FIELDS.index("event_id")
E_ORDER = DELIVERY_EVENT_FIELDS.index("order_id")
E_DRIVER = DELIVERY_EVENT_FIELDS.index("driver_id")
E_TYPE = DELIVERY_EVENT_FIELDS.index("event_type")
E_TS = DELIVERY_EVENT_FIELDS.index("event_ts")
E_LAT = DELIVERY_EVENT_FIELDS.index("lat")
E_LON = DELIVERY_EVENT_FIELDS.index("lon")
D_ID = DRIVER_COLUMNS.index("driver_id")
D_VEHICLE = DRIVER_COLUMNS.index("vehicle_type")
D_RATING = DRIVER_COLUMNS.index("rating")
S_ID = SHIFT_EVENT_COLUMNS.index("event_id")
S_DRIVER = SHIFT_EVENT_COLUMNS.index("driver_id")

SOURCES = tuple(source for source, _ in SOURCE_FILES)
PER_CASE_ONLY_TYPES = ("impossible_gps_speed", "out_of_order_event", "gps_long_gap")


@dataclass(frozen=True)
class LoadedBatch:
    """Parsed contents of one batch folder."""

    sequence: int
    metadata: dict[str, Any]
    window_start: int
    window_end: int
    data_files: dict[str, bytes]
    rows: dict[str, list[Row]]


def _parse_csv(content: bytes) -> list[Row]:
    reader = csv.reader(io.StringIO(content.decode("utf-8"), newline=""))
    next(reader, None)  # header
    return [tuple(row) for row in reader]


def _parse_events(content: bytes) -> list[Row]:
    rows: list[Row] = []
    for line in content.decode("utf-8").splitlines():
        item: dict[str, Any] = json.loads(line, parse_float=str)
        rows.append(
            tuple("" if item[name] is None else str(item[name]) for name in DELIVERY_EVENT_FIELDS)
        )
    return rows


def _required_epoch(text: str, label: str) -> int:
    epoch = parse_utc(text)
    if epoch is None:
        raise ValueError(f"Invalid {label} {text!r} in batch metadata.")
    return epoch


def load_dataset(dataset_dir: Path) -> list[LoadedBatch]:
    """Load every ``batch-<k>`` folder of a raw dataset, ignoring dot-prefixed entries."""
    folders = [
        path
        for path in dataset_dir.iterdir()
        if path.is_dir() and not path.name.startswith(".") and path.name.startswith("batch-")
    ]
    batches: list[LoadedBatch] = []
    for folder in sorted(folders, key=lambda path: int(path.name.removeprefix("batch-"))):
        metadata: dict[str, Any] = json.loads((folder / BATCH_METADATA_FILE).read_text("utf-8"))
        data_files = {name: (folder / name).read_bytes() for _, name in SOURCE_FILES}
        rows = {
            source: _parse_events(data_files[name])
            if name.endswith(".jsonl")
            else _parse_csv(data_files[name])
            for source, name in SOURCE_FILES
        }
        batches.append(
            LoadedBatch(
                sequence=int(metadata["batch_sequence"]),
                metadata=metadata,
                window_start=_required_epoch(str(metadata["window_start"]), "window_start"),
                window_end=_required_epoch(
                    str(metadata["window_end_exclusive"]), "window_end_exclusive"
                ),
                data_files=data_files,
                rows=rows,
            )
        )
    return batches


def _float(text: str) -> float | None:
    try:
        value = float(text)
    except ValueError:
        return None
    return value if math.isfinite(value) else None


def _coordinates(row: Row) -> tuple[float, float] | None:
    lat = _float(row[E_LAT])
    lon = _float(row[E_LON])
    if lat is None or lon is None:
        return None
    return (lat, lon)


def _out_of_range(row: Row) -> bool:
    lat = _float(row[E_LAT])
    lon = _float(row[E_LON])
    return (lat is not None and abs(lat) > 90) or (lon is not None and abs(lon) > 180)


def speed_kmh(a: Row, b: Row) -> float:
    """Implied speed between two pings (infinite for a non-positive interval)."""
    start = _coordinates(a)
    end = _coordinates(b)
    ta = parse_utc(a[E_TS])
    tb = parse_utc(b[E_TS])
    if start is None or end is None or ta is None or tb is None:
        return math.nan
    seconds = tb - ta
    if seconds <= 0:
        return math.inf
    return haversine_km(start, end) / seconds * 3600


def _invalid_driver(row: Row) -> bool:
    rating = _float(row[D_RATING]) if row[D_RATING] else None
    bad_rating = rating is not None and not RATING_MIN <= rating <= RATING_MAX
    return bad_rating or row[D_VEHICLE] not in VEHICLE_TYPES


class _Index:
    """Lookups over a loaded dataset."""

    def __init__(self, batches: list[LoadedBatch]) -> None:
        self.batches = {batch.sequence: batch for batch in batches}
        self.by_key: dict[str, dict[str, list[tuple[int, Row]]]] = {s: {} for s in SOURCES}
        key_column = {
            "drivers": D_ID,
            "orders": O_ID,
            "delivery_events": E_ID,
            "shift_events": S_ID,
        }
        self.events_by_order: dict[str, list[tuple[int, Row]]] = {}
        self.shifts_by_driver: dict[str, list[tuple[int, Row]]] = {}
        for batch in batches:
            for source in SOURCES:
                for row in batch.rows[source]:
                    entry = (batch.sequence, row)
                    self.by_key[source].setdefault(row[key_column[source]], []).append(entry)
            for row in batch.rows["delivery_events"]:
                self.events_by_order.setdefault(row[E_ORDER], []).append((batch.sequence, row))
            for row in batch.rows["shift_events"]:
                self.shifts_by_driver.setdefault(row[S_DRIVER], []).append((batch.sequence, row))

    def rows(self, source: str, key: str, batch: int | None = None) -> list[Row]:
        entries = self.by_key[source].get(key, [])
        return [row for sequence, row in entries if batch is None or sequence == batch]

    def earlier_rows(self, source: str, key: str, batch: int) -> list[Row]:
        return [row for sequence, row in self.by_key[source].get(key, []) if sequence < batch]

    def first_batch(self, source: str, key: str) -> int | None:
        entries = self.by_key[source].get(key, [])
        return min((sequence for sequence, _ in entries), default=None)

    def one(self, source: str, key: str, batch: int) -> Row | None:
        rows = self.rows(source, key, batch)
        return rows[0] if rows else None

    def order_pings(self, order_id: str) -> list[Row]:
        events = self.events_by_order.get(order_id, [])
        pings = [row for _, row in events if row[E_TYPE] == "gps_ping"]
        return sorted(pings, key=lambda row: (row[E_TS], row[E_ID]))


def _max_updated(rows: list[Row]) -> int | None:
    stamps = [parse_utc(row[O_UPDATED]) for row in rows]
    valid = [stamp for stamp in stamps if stamp is not None]
    return max(valid) if valid else None


def _ts_less(a: str, b: str) -> bool:
    first = parse_utc(a)
    second = parse_utc(b)
    return first is not None and second is not None and first < second


# --------------------------------------------------------------- per case ---

Check = Callable[[Case, _Index], bool]


def _orders(ix: _Index, case: Case) -> list[Row]:
    return ix.rows("orders", case.record_key, case.batch)


def _event(ix: _Index, case: Case) -> Row | None:
    return ix.one("delivery_events", case.record_key, case.batch)


def _related_event(ix: _Index, case: Case) -> Row | None:
    if not case.related_keys:
        return None
    return ix.one("delivery_events", case.related_keys[0], case.batch)


def _check_exact_duplicate(case: Case, ix: _Index) -> bool:
    rows = _orders(ix, case)
    return any(rows.count(row) >= 2 for row in rows)


def _check_unchanged_resend(case: Case, ix: _Index) -> bool:
    earlier = ix.earlier_rows("orders", case.record_key, case.batch)
    return any(row in earlier for row in _orders(ix, case))


def _check_stale(case: Case, ix: _Index) -> bool:
    newest = _max_updated(ix.earlier_rows("orders", case.record_key, case.batch))
    stamps = [parse_utc(row[O_UPDATED]) for row in _orders(ix, case)]
    return newest is not None and any(s is not None and s < newest for s in stamps)


def _check_newer(case: Case, ix: _Index) -> bool:
    newest = _max_updated(ix.earlier_rows("orders", case.record_key, case.batch))
    stamps = [parse_utc(row[O_UPDATED]) for row in _orders(ix, case)]
    return newest is not None and any(s is not None and s > newest for s in stamps)


def _check_missing_required(case: Case, ix: _Index) -> bool:
    return any(any(row[i] == "" for i in O_REQUIRED) for row in _orders(ix, case))


def _check_invalid_status(case: Case, ix: _Index) -> bool:
    return any(normalise_status(row[O_STATUS]) not in ORDER_STATUSES for row in _orders(ix, case))


def _check_status_normalisation(case: Case, ix: _Index) -> bool:
    return any(
        row[O_STATUS] not in ORDER_STATUSES and normalise_status(row[O_STATUS]) in ORDER_STATUSES
        for row in _orders(ix, case)
    )


def _check_delivery_before_pickup(case: Case, ix: _Index) -> bool:
    return any(_ts_less(row[O_DELIVERY], row[O_PICKUP]) for row in _orders(ix, case))


def _unparseable(row: Row) -> bool:
    return any(row[i] != "" and parse_utc(row[i]) is None for i in O_TIMESTAMPS)


def _check_unparseable(case: Case, ix: _Index) -> bool:
    return any(_unparseable(row) for row in _orders(ix, case))


def _bad_numeric(row: Row) -> bool:
    fare = _float(row[O_FARE])
    distance = _float(row[O_DISTANCE])
    return (fare is not None and fare <= 0) or (distance is not None and distance <= 0)


def _check_numeric(case: Case, ix: _Index) -> bool:
    return any(_bad_numeric(row) for row in _orders(ix, case))


def _check_delayed_order(case: Case, ix: _Index) -> bool:
    rows = _orders(ix, case)
    window_start = ix.batches[case.batch].window_start
    created = [parse_utc(row[O_CREATED]) for row in rows]
    events = ix.events_by_order.get(case.record_key, [])
    early_events = [sequence for sequence, _ in events if sequence == case.batch - 1]
    return (
        ix.first_batch("orders", case.record_key) == case.batch
        and any(c is not None and c < window_start for c in created)
        and len(early_events) > 0
    )


def _check_orphan_event(case: Case, ix: _Index) -> bool:
    row = _event(ix, case)
    if row is None:
        return False
    first = ix.first_batch("orders", row[E_ORDER])
    return first == case.batch + 1


def _check_same_batch_replay(case: Case, ix: _Index) -> bool:
    rows = ix.rows("delivery_events", case.record_key, case.batch)
    return any(rows.count(row) >= 2 for row in rows)


def _check_next_batch_replay(case: Case, ix: _Index) -> bool:
    earlier = ix.earlier_rows("delivery_events", case.record_key, case.batch)
    rows = ix.rows("delivery_events", case.record_key, case.batch)
    return any(row in earlier for row in rows)


def _check_missing_coordinates(case: Case, ix: _Index) -> bool:
    row = _event(ix, case)
    return row is not None and row[E_TYPE] == "gps_ping" and row[E_LAT] == "" == row[E_LON]


def _check_out_of_range(case: Case, ix: _Index) -> bool:
    row = _event(ix, case)
    return row is not None and _out_of_range(row)


def _immediately_before(ix: _Index, row: Row, other: Row) -> bool:
    """True if ``other`` is the ping of the same order directly before ``row`` in time."""
    pings = ix.order_pings(row[E_ORDER])
    if row not in pings or other not in pings:
        return False
    return pings.index(row) - pings.index(other) == 1


def _check_impossible_speed(case: Case, ix: _Index) -> bool:
    row = _event(ix, case)
    previous = _related_event(ix, case)
    if row is None or previous is None or not _immediately_before(ix, row, previous):
        return False
    return speed_kmh(previous, row) > MAX_PLAUSIBLE_SPEED_KMH


def _check_duplicate_timestamp(case: Case, ix: _Index) -> bool:
    row = _event(ix, case)
    original = _related_event(ix, case)
    return (
        row is not None
        and original is not None
        and row[E_TYPE] == original[E_TYPE] == "gps_ping"
        and row[E_DRIVER] == original[E_DRIVER]
        and row[E_TS] == original[E_TS]
        and original[E_ID] < row[E_ID]
    )


def _check_out_of_order(case: Case, ix: _Index) -> bool:
    first_in_file = _event(ix, case)
    second_in_file = _related_event(ix, case)
    return (
        first_in_file is not None
        and second_in_file is not None
        and first_in_file[E_ORDER] == second_in_file[E_ORDER]
        and first_in_file[E_ID] < second_in_file[E_ID]
        and _ts_less(second_in_file[E_TS], first_in_file[E_TS])
    )


def _check_long_gap(case: Case, ix: _Index) -> bool:
    after = _event(ix, case)
    before = _related_event(ix, case)
    if after is None or before is None or not _immediately_before(ix, after, before):
        return False
    start = parse_utc(before[E_TS])
    end = parse_utc(after[E_TS])
    return (
        start is not None
        and end is not None
        and end - start >= LONG_GAP_MIN_SECONDS
        and speed_kmh(before, after) <= MAX_PLAUSIBLE_SPEED_KMH
    )


def _check_late_event(case: Case, ix: _Index) -> bool:
    row = _event(ix, case)
    if row is None:
        return False
    ts = parse_utc(row[E_TS])
    first_order_batch = ix.first_batch("orders", row[E_ORDER])
    return (
        ts is not None
        and ts < ix.batches[case.batch].window_start
        and first_order_batch is not None
        and first_order_batch < case.batch
        and row not in ix.earlier_rows("delivery_events", case.record_key, case.batch)
    )


def _check_delayed_driver(case: Case, ix: _Index) -> bool:
    shifts = ix.shifts_by_driver.get(case.record_key, [])
    early_shifts = [sequence for sequence, _ in shifts if sequence == case.batch - 1]
    return ix.first_batch("drivers", case.record_key) == case.batch > 1 and len(early_shifts) > 0


def _check_invalid_driver(case: Case, ix: _Index) -> bool:
    return any(_invalid_driver(row) for row in ix.rows("drivers", case.record_key, case.batch))


def _check_orphan_shift(case: Case, ix: _Index) -> bool:
    row = ix.one("shift_events", case.record_key, case.batch)
    return row is not None and ix.first_batch("drivers", row[S_DRIVER]) == case.batch + 1


def _check_shift_replay(case: Case, ix: _Index) -> bool:
    rows = ix.rows("shift_events", case.record_key, case.batch)
    return any(rows.count(row) >= 2 for row in rows)


CHECKS: dict[str, Check] = {
    "exact_duplicate_row": _check_exact_duplicate,
    "unchanged_order_resend": _check_unchanged_resend,
    "stale_order_version": _check_stale,
    "newer_order_update": _check_newer,
    "missing_required_value": _check_missing_required,
    "invalid_status": _check_invalid_status,
    "status_needs_normalisation": _check_status_normalisation,
    "delivery_before_pickup": _check_delivery_before_pickup,
    "unparseable_timestamp": _check_unparseable,
    "invalid_numeric_range": _check_numeric,
    "delayed_order_record": _check_delayed_order,
    "orphan_event_missing_order": _check_orphan_event,
    "replayed_event_same_batch": _check_same_batch_replay,
    "replayed_event_next_batch": _check_next_batch_replay,
    "missing_coordinates": _check_missing_coordinates,
    "out_of_range_coordinates": _check_out_of_range,
    "impossible_gps_speed": _check_impossible_speed,
    "duplicate_timestamp": _check_duplicate_timestamp,
    "out_of_order_event": _check_out_of_order,
    "gps_long_gap": _check_long_gap,
    "late_event": _check_late_event,
    "delayed_driver_record": _check_delayed_driver,
    "invalid_driver_value": _check_invalid_driver,
    "orphan_shift_event_missing_driver": _check_orphan_shift,
    "replayed_shift_event": _check_shift_replay,
}


# ------------------------------------------------------ independent counts ---


def independent_counts(batches: list[LoadedBatch]) -> Counter[str]:
    """Defect counts derived from the files alone, without reading the manifest."""
    ix = _Index(batches)
    counts: Counter[str] = Counter()
    for batch in batches:
        b = batch.sequence
        for source, defect in (
            ("orders", "exact_duplicate_row"),
            ("delivery_events", "replayed_event_same_batch"),
            ("shift_events", "replayed_shift_event"),
        ):
            rows = batch.rows[source]
            counts[defect] += len(rows) - len(set(rows))

        for row in batch.rows["drivers"]:
            counts["invalid_driver_value"] += _invalid_driver(row)
            first = ix.first_batch("drivers", row[D_ID])
            counts["delayed_driver_record"] += first == b and b > 1

        for row in batch.rows["orders"]:
            earlier = ix.earlier_rows("orders", row[O_ID], b)
            newest = _max_updated(earlier)
            updated = parse_utc(row[O_UPDATED])
            created = parse_utc(row[O_CREATED])
            counts["unchanged_order_resend"] += row in earlier
            counts["stale_order_version"] += (
                newest is not None and updated is not None and updated < newest
            )
            counts["newer_order_update"] += (
                newest is not None and updated is not None and updated > newest
            )
            counts["missing_required_value"] += any(row[i] == "" for i in O_REQUIRED)
            status = row[O_STATUS]
            counts["invalid_status"] += normalise_status(status) not in ORDER_STATUSES
            counts["status_needs_normalisation"] += (
                status not in ORDER_STATUSES and normalise_status(status) in ORDER_STATUSES
            )
            counts["delivery_before_pickup"] += _ts_less(row[O_DELIVERY], row[O_PICKUP])
            counts["unparseable_timestamp"] += _unparseable(row)
            counts["invalid_numeric_range"] += _bad_numeric(row)
            counts["delayed_order_record"] += (
                not earlier and created is not None and created < batch.window_start
            )

        known_orders = {
            row[O_ID] for other in batches if other.sequence <= b for row in other.rows["orders"]
        }
        pings_seen: dict[tuple[str, str], list[str]] = {}
        for row in batch.rows["delivery_events"]:
            earlier_events = ix.earlier_rows("delivery_events", row[E_ID], b)
            ts = parse_utc(row[E_TS])
            counts["replayed_event_next_batch"] += row in earlier_events
            counts["missing_coordinates"] += row[E_LAT] == "" and row[E_LON] == ""
            counts["out_of_range_coordinates"] += _out_of_range(row)
            counts["orphan_event_missing_order"] += row[E_ORDER] not in known_orders
            counts["late_event"] += (
                ts is not None and ts < batch.window_start and row not in earlier_events
            )
            if row[E_TYPE] == "gps_ping":
                pings_seen.setdefault((row[E_DRIVER], row[E_TS]), []).append(row[E_ID])
        for ids in pings_seen.values():
            counts["duplicate_timestamp"] += len(set(ids)) - 1

        known_drivers = {
            row[D_ID] for other in batches if other.sequence <= b for row in other.rows["drivers"]
        }
        for row in batch.rows["shift_events"]:
            counts["orphan_shift_event_missing_driver"] += row[S_DRIVER] not in known_drivers
    return counts


# --------------------------------------------------------------- reconcile ---


def expected_reconciliation(
    batches: list[LoadedBatch], cases: tuple[Case, ...]
) -> dict[tuple[str, int], dict[str, int]]:
    """Expected disposition counts per (source, batch); accepted = input minus the rest."""
    counter = Counter((case.source, case.batch, case.expected_disposition) for case in cases)
    result: dict[tuple[str, int], dict[str, int]] = {}
    for batch in batches:
        for source in SOURCES:
            counts = {
                disposition: counter[(source, batch.sequence, disposition)]
                for disposition in DISPOSITIONS
                if disposition != ACCEPTED
            }
            row_input = len(batch.rows[source])
            result[(source, batch.sequence)] = {
                "input": row_input,
                ACCEPTED: row_input - sum(counts.values()),
                **counts,
            }
    return result


# ------------------------------------------------------------------ verify ---


def _entity(case: Case, ix: _Index) -> tuple[str, str] | None:
    if case.source == "orders":
        return ("order", case.record_key)
    if case.source == "drivers":
        return ("driver", case.record_key)
    if case.source == "delivery_events":
        row = ix.one("delivery_events", case.record_key, case.batch)
        return None if row is None else ("order", row[E_ORDER])
    row = ix.one("shift_events", case.record_key, case.batch)
    return None if row is None else ("driver", row[S_DRIVER])


def verify(dataset_dir: Path, manifest: Manifest) -> list[str]:
    """Return a list of problems; an empty list means the manifest matches the files."""
    problems: list[str] = []
    batches = load_dataset(dataset_dir)
    if len(batches) != len(manifest.batch_ids):
        return [f"expected {len(manifest.batch_ids)} batches, found {len(batches)}"]
    ix = _Index(batches)

    canonical = json.dumps(manifest.config, sort_keys=True, separators=(",", ":"))
    for batch, expected_id in zip(batches, manifest.batch_ids, strict=True):
        recomputed = compute_batch_id(canonical, batch.sequence, batch.data_files)
        if recomputed != expected_id or batch.metadata["batch_id"] != expected_id:
            problems.append(f"batch {batch.sequence}: batch_id does not match its files")
        if batch.metadata["config_sha256"] != manifest.config_sha256:
            problems.append(f"batch {batch.sequence}: config_sha256 differs from the manifest")

    keys = Counter((case.source, case.batch, case.record_key) for case in manifest.cases)
    problems += [f"record {key} has {n} cases" for key, n in sorted(keys.items()) if n > 1]

    owners: dict[tuple[str, str], str] = {}
    for case in manifest.cases:
        check = CHECKS.get(case.defect_type)
        if check is None:
            problems.append(f"{case.scenario}: unknown defect type {case.defect_type}")
        elif not check(case, ix):
            problems.append(f"{case.scenario}: {case.defect_type} not found at {case.record_key}")
        entity = _entity(case, ix)
        if entity is not None:
            owner = owners.setdefault(entity, case.scenario)
            if owner != case.scenario:
                problems.append(f"{entity} is used by both {owner} and {case.scenario}")

    manifest_counts = Counter(case.defect_type for case in manifest.cases)
    found = independent_counts(batches)
    for defect_type in sorted(set(manifest_counts) | set(found)):
        if defect_type in PER_CASE_ONLY_TYPES:
            continue
        if manifest_counts[defect_type] != found[defect_type]:
            problems.append(
                f"{defect_type}: manifest has {manifest_counts[defect_type]} cases, "
                f"files show {found[defect_type]}"
            )

    for (source, sequence), counts in expected_reconciliation(batches, manifest.cases).items():
        if counts[ACCEPTED] < 0:
            problems.append(f"{source} batch {sequence}: more cases than input rows")
    return problems
