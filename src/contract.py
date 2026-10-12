"""The data contract shared by the generator and the pipeline (see docs/data_contracts.md).

Columns, accepted values, units, timestamp format, GPS thresholds, dispositions and reason codes.
The accepted-value lists must equal the CHECK constraints in ``sql/schema.sql``; a unit test
enforces this.
"""

import re
from datetime import UTC, date, datetime

# ---------------------------------------------------------------- time ---
REPORTING_TIMEZONE = "Asia/Kolkata"
REPORTING_UTC_OFFSET_SECONDS = 19_800  # Asia/Kolkata is UTC+05:30 all year (no DST)
SECONDS_PER_DAY = 86_400

_UTC_TS_PATTERN = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z")
_UTC_TS_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
_DATE_PATTERN = re.compile(r"\d{4}-\d{2}-\d{2}")

# ------------------------------------------------------- accepted values ---
ORDER_STATUSES = ("created", "assigned", "picked_up", "delivered", "cancelled")
DELIVERY_EVENT_TYPES = ("assigned", "picked_up", "gps_ping", "delivered", "cancelled")
SHIFT_EVENT_TYPES = ("online", "offline", "break_start", "break_end")
VEHICLE_TYPES = ("bicycle", "scooter", "motorbike", "car")
RATING_MIN = 1.0
RATING_MAX = 5.0

# Reference data for analytics.dim_city: city -> state. A city not listed gets UNKNOWN_STATE.
CITY_STATES: dict[str, str] = {
    "Bengaluru": "Karnataka",
    "Chennai": "Tamil Nadu",
    "Delhi": "Delhi",
    "Hyderabad": "Telangana",
    "Mumbai": "Maharashtra",
}
UNKNOWN_STATE = "Unknown"

# ------------------------------------------------------ metric rules ---
# A paired online session or break longer than this is treated as unpaired (contract 6.4).
MAX_SESSION_SECONDS = 86_400

# ---------------------------------------------------------- GPS rules ---
MAX_PLAUSIBLE_SPEED_KMH = 120.0
LONG_GAP_MIN_SECONDS = 1_200

# ------------------------------------------------------------ sources ---
DRIVER_COLUMNS = ("driver_id", "city", "vehicle_type", "joined_date", "rating")
ORDER_COLUMNS = (
    "order_id",
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
DELIVERY_EVENT_FIELDS = (
    "event_id",
    "order_id",
    "driver_id",
    "event_type",
    "event_ts",
    "lat",
    "lon",
)
SHIFT_EVENT_COLUMNS = ("event_id", "driver_id", "event_type", "event_ts")

SOURCE_DRIVERS = "drivers"
SOURCE_ORDERS = "orders"
SOURCE_DELIVERY_EVENTS = "delivery_events"
SOURCE_SHIFT_EVENTS = "shift_events"

# (source name, file name), in processing order.
SOURCE_FILES = (
    (SOURCE_DRIVERS, "drivers.csv"),
    (SOURCE_ORDERS, "orders.csv"),
    (SOURCE_DELIVERY_EVENTS, "delivery_events.jsonl"),
    (SOURCE_SHIFT_EVENTS, "shift_events.csv"),
)
SOURCE_COLUMNS: dict[str, tuple[str, ...]] = {
    SOURCE_DRIVERS: DRIVER_COLUMNS,
    SOURCE_ORDERS: ORDER_COLUMNS,
    SOURCE_DELIVERY_EVENTS: DELIVERY_EVENT_FIELDS,
    SOURCE_SHIFT_EVENTS: SHIFT_EVENT_COLUMNS,
}
SOURCE_KEYS: dict[str, str] = {
    SOURCE_DRIVERS: "driver_id",
    SOURCE_ORDERS: "order_id",
    SOURCE_DELIVERY_EVENTS: "event_id",
    SOURCE_SHIFT_EVENTS: "event_id",
}
BATCH_METADATA_FILE = "batch.json"
BATCH_FOLDER_PREFIX = "batch-"

# -------------------------------------------------------- dispositions ---
ACCEPTED = "accepted"
REJECTED = "rejected"
DUPLICATE = "duplicate"
STALE_IGNORED = "stale_ignored"
PENDING = "pending"
DISPOSITIONS = (ACCEPTED, REJECTED, DUPLICATE, STALE_IGNORED, PENDING)

# -------------------------------------------------------- reason codes ---
MISSING_REQUIRED_FIELD = "missing_required_field"
INVALID_STATUS = "invalid_status"
INVALID_TIMESTAMP = "invalid_timestamp"
INVALID_NUMBER = "invalid_number"
OUT_OF_RANGE_VALUE = "out_of_range_value"
INVALID_VEHICLE_TYPE = "invalid_vehicle_type"
INVALID_EVENT_TYPE = "invalid_event_type"
INCONSISTENT_STATUS_FIELDS = "inconsistent_status_fields"
INCONSISTENT_TIMESTAMPS = "inconsistent_timestamps"
DELIVERY_BEFORE_PICKUP = "delivery_before_pickup"
MISSING_COORDINATES = "missing_coordinates"
INVALID_COORDINATES = "invalid_coordinates"
GPS_IMPOSSIBLE_SPEED = "gps_impossible_speed"
GPS_NON_POSITIVE_INTERVAL = "gps_non_positive_interval"
CONFLICTING_VERSION = "conflicting_version"
MALFORMED_RECORD = "malformed_record"
ORPHAN_TIMEOUT = "orphan_timeout"
OLDER_SOURCE_UPDATED_AT = "older_source_updated_at"
MISSING_ORDER = "missing_order"
MISSING_DRIVER = "missing_driver"
# Every reason a disposition or rejected record may carry ("" = no reason).
REASON_CODES = (
    "",
    MISSING_REQUIRED_FIELD,
    INVALID_STATUS,
    INVALID_TIMESTAMP,
    INVALID_NUMBER,
    OUT_OF_RANGE_VALUE,
    INVALID_VEHICLE_TYPE,
    INVALID_EVENT_TYPE,
    INCONSISTENT_STATUS_FIELDS,
    INCONSISTENT_TIMESTAMPS,
    DELIVERY_BEFORE_PICKUP,
    MISSING_COORDINATES,
    INVALID_COORDINATES,
    GPS_IMPOSSIBLE_SPEED,
    GPS_NON_POSITIVE_INTERVAL,
    CONFLICTING_VERSION,
    MALFORMED_RECORD,
    ORPHAN_TIMEOUT,
    OLDER_SOURCE_UPDATED_AT,
    MISSING_ORDER,
    MISSING_DRIVER,
)


def parse_utc(text: str) -> int | None:
    """Parse a contract timestamp (``YYYY-MM-DDTHH:MM:SSZ``); return None if it is invalid."""
    if not _UTC_TS_PATTERN.fullmatch(text):
        return None
    try:
        parsed = datetime.strptime(text, _UTC_TS_FORMAT).replace(tzinfo=UTC)
    except ValueError:
        return None
    return int(parsed.timestamp())


def parse_date(text: str) -> bool:
    """True if ``text`` is a valid ``YYYY-MM-DD`` calendar date."""
    if not _DATE_PATTERN.fullmatch(text):
        return False
    try:
        datetime.strptime(text, "%Y-%m-%d")
    except ValueError:
        return False
    return True


def format_utc(epoch: int) -> str:
    """Format epoch seconds as a contract timestamp in UTC."""
    return datetime.fromtimestamp(epoch, UTC).strftime(_UTC_TS_FORMAT)


def format_reporting_date(epoch: int) -> str:
    """Calendar date (``YYYY-MM-DD``) of ``epoch`` in the reporting timezone."""
    local = datetime.fromtimestamp(epoch + REPORTING_UTC_OFFSET_SECONDS, UTC)
    return local.strftime("%Y-%m-%d")


def reporting_date(epoch: int) -> date:
    """Calendar date of ``epoch`` in the reporting timezone (Asia/Kolkata has no DST)."""
    return datetime.fromtimestamp(epoch + REPORTING_UTC_OFFSET_SECONDS, UTC).date()


def format_coordinate(value: float) -> str:
    """Fixed six-decimal coordinate formatting."""
    return f"{value:.6f}"


def normalise_status(text: str) -> str:
    """Contract status normalisation: trim surrounding whitespace and lowercase."""
    return text.strip().lower()
