"""The benchmark workload, implemented identically by the pandas and the Spark engine.

This is a defined, vectorised subset of the pipeline's cleaning rules, not the stateful
disposition engine (pending, stale, cross-batch state), which stays the reference
(``src/transformations``). Both engines must return identical results; a test proves it.

Input: per shard, ``orders.parquet`` and ``delivery_events.parquet`` with the contract columns
as text ("" = missing) plus ``row_id`` (int64, unique, input order).

Orders
  O1  required: order_id, status, city, order_created_ts, promised_ts, source_updated_at.
  O2  order_created_ts, promised_ts and source_updated_at parse as ``YYYY-MM-DDTHH:MM:SSZ``;
      pickup_ts and delivery_ts are empty or parse.
  O3  status is one of the contract statuses.
  O4  a delivered order has pickup_ts and delivery_ts, with delivery_ts >= pickup_ts.
  A row failing O1-O4 is rejected. Among valid rows, one row per order_id is kept: the newest
  source_updated_at, ties broken by the larger row_id; the others are duplicates.
  Daily metrics per reporting date (Asia/Kolkata) of order_created_ts and city: orders,
  delivered, late (delivered with delivery_ts > promised_ts), cancelled, and the sum of
  delivery seconds (delivery_ts - pickup_ts) of delivered orders.

Delivery events
  E1  required: event_id, order_id, driver_id, event_type, event_ts; event_ts parses; event_type
      is a contract event type.
  E2  a gps_ping has numeric lat in [-90, 90] and lon in [-180, 180].
  A row failing E1-E2 is rejected. One row per event_id is kept (the smallest row_id); the
  others are duplicates. GPS pings of each driver, ordered by (event_ts, event_id), are flagged
  when the interval to the previous ping is <= 0 s or the Haversine speed exceeds 120 km/h.
  Daily counts per reporting date of event_ts: events, gps_pings, flagged.
"""

from dataclasses import asdict, dataclass, field
from typing import Any

from src.contract import (
    DELIVERY_EVENT_FIELDS,
    DELIVERY_EVENT_TYPES,
    MAX_PLAUSIBLE_SPEED_KMH,
    ORDER_COLUMNS,
    ORDER_STATUSES,
    REPORTING_UTC_OFFSET_SECONDS,
)

ORDERS_FILE = "orders.parquet"
EVENTS_FILE = "delivery_events.parquet"
ROW_ID = "row_id"
ORDER_INPUT_COLUMNS = (*ORDER_COLUMNS, ROW_ID)
EVENT_INPUT_COLUMNS = (*DELIVERY_EVENT_FIELDS, ROW_ID)
ORDER_REQUIRED = (
    "order_id",
    "status",
    "city",
    "order_created_ts",
    "promised_ts",
    "source_updated_at",
)
EVENT_REQUIRED = ("event_id", "order_id", "driver_id", "event_type", "event_ts")
STATUSES = ORDER_STATUSES
EVENT_TYPES = DELIVERY_EVENT_TYPES
GPS_PING = "gps_ping"
DELIVERED = "delivered"
CANCELLED = "cancelled"
MAX_SPEED_KMH = MAX_PLAUSIBLE_SPEED_KMH
OFFSET_SECONDS = REPORTING_UTC_OFFSET_SECONDS
EARTH_RADIUS_KM = 6371.0088  # as src/transformations/gps.py
DEGREES_TO_RADIANS = 0.017453292519943295  # math.pi / 180, written out so both engines agree
# Python strptime pattern and the equivalent Spark datetime pattern of a contract timestamp.
TS_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
SPARK_TS_FORMAT = "yyyy-MM-dd'T'HH:mm:ss'Z'"


@dataclass
class WorkloadResult:
    """Counts and daily aggregates of one workload run; equal across engines by design."""

    orders_input: int = 0
    orders_rejected: int = 0
    orders_duplicate: int = 0
    orders_clean: int = 0
    events_input: int = 0
    events_rejected: int = 0
    events_duplicate: int = 0
    events_clean: int = 0
    gps_flagged: int = 0
    # (date, city, orders, delivered, late, cancelled, delivery_seconds), sorted
    order_days: list[tuple[str, str, int, int, int, int, int]] = field(default_factory=list)
    # (date, events, gps_pings, flagged), sorted
    event_days: list[tuple[str, int, int, int]] = field(default_factory=list)

    def to_json(self) -> dict[str, Any]:
        """A JSON-serialisable dictionary."""
        data = asdict(self)
        data["order_days"] = [list(row) for row in self.order_days]
        data["event_days"] = [list(row) for row in self.event_days]
        return data

    @classmethod
    def from_json(cls, data: dict[str, Any]) -> "WorkloadResult":
        """The inverse of :meth:`to_json`."""
        values = dict(data)
        values["order_days"] = [tuple(row) for row in data["order_days"]]
        values["event_days"] = [tuple(row) for row in data["event_days"]]
        return cls(**values)
