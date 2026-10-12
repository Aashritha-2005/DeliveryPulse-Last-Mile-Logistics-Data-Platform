"""Pure, record-level validation rules of the data contract.

Each ``validate_*`` function checks one record on its own (no cross-record or cross-batch
state) and returns either a reason code or a normalised record. Checks run in the documented
order and the first failing check wins (see docs/data_contracts.md, section 7).
"""

import hashlib
import json
import math
from dataclasses import astuple, dataclass
from decimal import Decimal, InvalidOperation

from src.contract import (
    DELIVERY_BEFORE_PICKUP,
    DELIVERY_EVENT_TYPES,
    INCONSISTENT_STATUS_FIELDS,
    INCONSISTENT_TIMESTAMPS,
    INVALID_COORDINATES,
    INVALID_EVENT_TYPE,
    INVALID_NUMBER,
    INVALID_STATUS,
    INVALID_TIMESTAMP,
    INVALID_VEHICLE_TYPE,
    MISSING_COORDINATES,
    MISSING_REQUIRED_FIELD,
    ORDER_STATUSES,
    OUT_OF_RANGE_VALUE,
    RATING_MAX,
    RATING_MIN,
    SHIFT_EVENT_TYPES,
    VEHICLE_TYPES,
    normalise_status,
    parse_date,
    parse_utc,
)

# Largest values the analytics column types can hold (numeric(7,3), numeric(10,2)).
MAX_DISTANCE_KM = Decimal("9999.999")
MAX_FARE_AMOUNT = Decimal("99999999.99")
STATUSES_WITH_DRIVER = ("assigned", "picked_up", "delivered")
STATUSES_WITH_PICKUP = ("picked_up", "delivered")
ORDER_REQUIRED = (
    "order_id",
    "customer_id",
    "order_created_ts",
    "promised_ts",
    "distance_km",
    "status",
    "city",
    "fare_amount",
    "source_updated_at",
)
ORDER_TIMESTAMPS = (
    "order_created_ts",
    "pickup_ts",
    "delivery_ts",
    "promised_ts",
    "source_updated_at",
)
DRIVER_REQUIRED = ("driver_id", "city", "vehicle_type", "joined_date")
EVENT_REQUIRED = ("event_id", "order_id", "driver_id", "event_type", "event_ts")
SHIFT_REQUIRED = ("event_id", "driver_id", "event_type", "event_ts")


def content_hash(values: tuple[object, ...]) -> str:
    """Stable hash of a record's content, used for duplicate and conflict detection."""
    text = json.dumps([str(value) for value in values], ensure_ascii=True)
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class DriverRecord:
    """A valid, normalised driver."""

    driver_id: str
    city: str
    vehicle_type: str
    joined_date: str
    rating: Decimal | None

    def content_hash(self) -> str:
        return content_hash(astuple(self))


@dataclass(frozen=True)
class OrderRecord:
    """A valid, normalised order version. Timestamps are UTC epoch seconds."""

    order_id: str
    customer_id: str
    driver_id: str | None
    order_created_ts: int
    pickup_ts: int | None
    delivery_ts: int | None
    promised_ts: int
    distance_km: Decimal
    status: str
    city: str
    fare_amount: Decimal
    source_updated_at: int

    def content_hash(self) -> str:
        return content_hash(astuple(self))

    def delivery_minutes(self) -> float | None:
        """Minutes from ``pickup_ts`` to ``delivery_ts``; delivered orders only."""
        if self.status != "delivered" or self.pickup_ts is None or self.delivery_ts is None:
            return None
        return (self.delivery_ts - self.pickup_ts) / 60

    def is_late(self) -> bool | None:
        """``delivery_ts > promised_ts`` for delivered orders; None otherwise."""
        if self.status != "delivered" or self.delivery_ts is None:
            return None
        return self.delivery_ts > self.promised_ts


@dataclass(frozen=True)
class EventRecord:
    """A delivery event that passed the record-level checks."""

    event_id: str
    order_id: str
    driver_id: str
    event_type: str
    event_ts: int
    lat: float | None
    lon: float | None


@dataclass(frozen=True)
class ShiftRecord:
    """A valid shift event."""

    event_id: str
    driver_id: str
    event_type: str
    event_ts: int


def _missing(fields: dict[str, str], required: tuple[str, ...]) -> bool:
    return any(fields[name].strip() == "" for name in required)


def _decimal(text: str) -> Decimal | None:
    try:
        value = Decimal(text)
    except InvalidOperation:
        return None
    return value if value.is_finite() else None


def _float(text: str) -> float | None:
    try:
        value = float(text)
    except ValueError:
        return None
    return value if math.isfinite(value) else None


def validate_driver(fields: dict[str, str]) -> tuple[str | None, DriverRecord | None]:
    """Driver checks: required fields, vehicle type, joined date, rating range."""
    if _missing(fields, DRIVER_REQUIRED):
        return MISSING_REQUIRED_FIELD, None
    if fields["vehicle_type"] not in VEHICLE_TYPES:
        return INVALID_VEHICLE_TYPE, None
    if not parse_date(fields["joined_date"]):
        return INVALID_TIMESTAMP, None
    rating: Decimal | None = None
    if fields["rating"] != "":
        rating = _decimal(fields["rating"])
        if rating is None:
            return INVALID_NUMBER, None
        if not Decimal(str(RATING_MIN)) <= rating <= Decimal(str(RATING_MAX)):
            return OUT_OF_RANGE_VALUE, None
    record = DriverRecord(
        driver_id=fields["driver_id"],
        city=fields["city"],
        vehicle_type=fields["vehicle_type"],
        joined_date=fields["joined_date"],
        rating=rating,
    )
    return None, record


def validate_order(fields: dict[str, str]) -> tuple[str | None, OrderRecord | None]:
    """Order checks, first failure wins.

    required fields -> status -> timestamps parse -> numbers -> status/field consistency ->
    delivery before pickup -> other timestamp ordering.
    """
    if _missing(fields, ORDER_REQUIRED):
        return MISSING_REQUIRED_FIELD, None
    status = normalise_status(fields["status"])
    if status not in ORDER_STATUSES:
        return INVALID_STATUS, None
    stamps: dict[str, int | None] = {}
    for name in ORDER_TIMESTAMPS:
        if fields[name] == "":
            stamps[name] = None
            continue
        parsed = parse_utc(fields[name])
        if parsed is None:
            return INVALID_TIMESTAMP, None
        stamps[name] = parsed
    distance = _decimal(fields["distance_km"])
    fare = _decimal(fields["fare_amount"])
    if distance is None or fare is None:
        return INVALID_NUMBER, None
    if not (0 < distance <= MAX_DISTANCE_KM and 0 < fare <= MAX_FARE_AMOUNT):
        return OUT_OF_RANGE_VALUE, None
    created = stamps["order_created_ts"]
    promised = stamps["promised_ts"]
    updated = stamps["source_updated_at"]
    pickup = stamps["pickup_ts"]
    delivery = stamps["delivery_ts"]
    if created is None or promised is None or updated is None:
        return MISSING_REQUIRED_FIELD, None
    driver_id = fields["driver_id"].strip()
    needs_driver = status in STATUSES_WITH_DRIVER and driver_id == ""
    needs_pickup = status in STATUSES_WITH_PICKUP and pickup is None
    delivery_mismatch = (status == "delivered") != (delivery is not None)
    if needs_driver or needs_pickup or delivery_mismatch:
        return INCONSISTENT_STATUS_FIELDS, None
    if pickup is not None and delivery is not None and delivery < pickup:
        return DELIVERY_BEFORE_PICKUP, None
    if (pickup is not None and pickup < created) or promised <= created or updated < created:
        return INCONSISTENT_TIMESTAMPS, None
    record = OrderRecord(
        order_id=fields["order_id"],
        customer_id=fields["customer_id"],
        driver_id=driver_id or None,
        order_created_ts=created,
        pickup_ts=pickup,
        delivery_ts=delivery,
        promised_ts=promised,
        distance_km=distance,
        status=status,
        city=fields["city"],
        fare_amount=fare,
        source_updated_at=updated,
    )
    return None, record


def validate_event_basics(fields: dict[str, str]) -> tuple[str | None, int | None]:
    """Delivery event checks before the duplicate check: required, event type, timestamp."""
    if _missing(fields, EVENT_REQUIRED):
        return MISSING_REQUIRED_FIELD, None
    if fields["event_type"] not in DELIVERY_EVENT_TYPES:
        return INVALID_EVENT_TYPE, None
    ts = parse_utc(fields["event_ts"])
    if ts is None:
        return INVALID_TIMESTAMP, None
    return None, ts


def validate_event_coordinates(
    fields: dict[str, str], event_ts: int
) -> tuple[str | None, EventRecord | None]:
    """Coordinate checks: missing (pings need both; never only one), then parse and range."""
    lat_text = fields["lat"]
    lon_text = fields["lon"]
    is_ping = fields["event_type"] == "gps_ping"
    if (lat_text == "") != (lon_text == "") or (is_ping and lat_text == ""):
        return MISSING_COORDINATES, None
    lat: float | None = None
    lon: float | None = None
    if lat_text != "":
        lat = _float(lat_text)
        lon = _float(lon_text)
        if lat is None or lon is None or abs(lat) > 90 or abs(lon) > 180:
            return INVALID_COORDINATES, None
    record = EventRecord(
        event_id=fields["event_id"],
        order_id=fields["order_id"],
        driver_id=fields["driver_id"],
        event_type=fields["event_type"],
        event_ts=event_ts,
        lat=lat,
        lon=lon,
    )
    return None, record


def validate_shift_event(fields: dict[str, str]) -> tuple[str | None, ShiftRecord | None]:
    """Shift event checks: required fields, event type, timestamp."""
    if _missing(fields, SHIFT_REQUIRED):
        return MISSING_REQUIRED_FIELD, None
    if fields["event_type"] not in SHIFT_EVENT_TYPES:
        return INVALID_EVENT_TYPE, None
    ts = parse_utc(fields["event_ts"])
    if ts is None:
        return INVALID_TIMESTAMP, None
    record = ShiftRecord(
        event_id=fields["event_id"],
        driver_id=fields["driver_id"],
        event_type=fields["event_type"],
        event_ts=ts,
    )
    return None, record


def fields_hash(fields: dict[str, str]) -> str:
    """Hash of the contract fields of a raw record (event identity and replay detection)."""
    return content_hash(tuple(fields[name] for name in sorted(fields)))
