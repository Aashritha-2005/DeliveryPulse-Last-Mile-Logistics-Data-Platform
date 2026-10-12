"""Independent plain-Python implementation of the metric definitions (tests only).

Written from docs/data_contracts.md section 5 and docs/data_model.md, without reusing any of
the SQL in src/database/metrics.py: it uses zoneinfo for the reporting timezone, plain loops
for interval unions and Decimal with ROUND_HALF_UP (PostgreSQL's round() on numeric rounds
half away from zero, which is the same for these non-negative values).
"""

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import ROUND_HALF_UP, Decimal
from zoneinfo import ZoneInfo

TZ = ZoneInfo("Asia/Kolkata")
MAX_SESSION_SECONDS = 86_400


@dataclass(frozen=True)
class Order:
    """The fields of an accepted order the metrics use (timestamps are epoch seconds)."""

    order_id: str
    driver_id: str | None
    city: str
    status: str
    created: int
    pickup: int | None
    delivery: int | None
    promised: int


@dataclass(frozen=True)
class Shift:
    """An accepted shift event."""

    event_id: str
    driver_id: str
    event_type: str
    ts: int


# (total_orders, avg_delivery_minutes, late_rate, cancelled_rate,
#  delivered_orders, late_orders, cancelled_orders)
DailyRow = tuple[int, Decimal | None, Decimal | None, Decimal | None, int, int, int]
# (deliveries, avg_delivery_minutes, utilisation_pct, busy_minutes, online_minutes)
DriverRow = tuple[int, Decimal | None, Decimal | None, Decimal, Decimal | None]


def local_date(epoch: int) -> date:
    """Reporting date of ``epoch``."""
    return datetime.fromtimestamp(epoch, TZ).date()


def day_bounds(day: date) -> tuple[int, int]:
    """[start, end) of a reporting day as epoch seconds."""
    start = datetime(day.year, day.month, day.day, tzinfo=TZ)
    end = start + timedelta(days=1)
    return int(start.timestamp()), int(end.timestamp())


def ratio(numerator: int, denominator: int, places: int) -> Decimal | None:
    """numerator / denominator rounded half-up to ``places``; None if the denominator is 0."""
    if denominator == 0:
        return None
    exponent = Decimal(1).scaleb(-places)
    return (Decimal(numerator) / Decimal(denominator)).quantize(exponent, rounding=ROUND_HALF_UP)


def days_overlapping(start: int, end: int) -> list[date]:
    """Reporting days d with start < d_end and end > d_start."""
    found = []
    day = local_date(start) - timedelta(days=1)
    while day <= local_date(end):
        day_start, day_end = day_bounds(day)
        if start < day_end and end > day_start:
            found.append(day)
        day += timedelta(days=1)
    return found


def daily_metrics(orders: list[Order]) -> dict[tuple[date, str], DailyRow]:
    """daily_delivery_metrics, keyed by (metric_date, city)."""
    created: dict[tuple[date, str], list[int]] = {}
    delivered: dict[tuple[date, str], list[int]] = {}
    for o in orders:
        counts = created.setdefault((local_date(o.created), o.city), [0, 0])
        counts[0] += 1
        counts[1] += o.status == "cancelled"
        if o.status == "delivered" and o.delivery is not None and o.pickup is not None:
            d = delivered.setdefault((local_date(o.delivery), o.city), [0, 0, 0])
            d[0] += 1
            d[1] += o.delivery > o.promised
            d[2] += o.delivery - o.pickup
    rows: dict[tuple[date, str], DailyRow] = {}
    for key in set(created) | set(delivered):
        total, cancelled = created.get(key, [0, 0])
        count, late, seconds = delivered.get(key, [0, 0, 0])
        rows[key] = (
            total,
            ratio(seconds, 60 * count, 2),
            ratio(late, count, 4),
            ratio(cancelled, total, 4),
            count,
            late,
            cancelled,
        )
    return rows


def _union_seconds(intervals: list[tuple[int, int]]) -> int:
    total = 0
    current: tuple[int, int] | None = None
    for start, end in sorted(intervals):
        if current is not None and start <= current[1]:
            current = (current[0], max(current[1], end))
        else:
            if current is not None:
                total += current[1] - current[0]
            current = (start, end)
    if current is not None:
        total += current[1] - current[0]
    return total


def _pairs(events: list[Shift], opening: str, closing: str) -> list[tuple[int, int]]:
    """(start, end) of each `opening` immediately followed by a `closing` within 24 h."""
    pairs = []
    for current, following in zip(events, events[1:], strict=False):
        duration = following.ts - current.ts
        is_pair = current.event_type == opening and following.event_type == closing
        if is_pair and duration <= MAX_SESSION_SECONDS:
            pairs.append((current.ts, following.ts))
    return pairs


def driver_performance(
    orders: list[Order], shifts: list[Shift]
) -> dict[tuple[str, date], DriverRow]:
    """driver_performance, keyed by (driver_id, metric_date)."""
    deliveries: dict[tuple[str, date], list[int]] = {}
    pieces: dict[tuple[str, date], list[tuple[int, int]]] = {}
    for o in orders:
        if o.status != "delivered" or o.driver_id is None:
            continue
        if o.pickup is None or o.delivery is None:
            continue
        d = deliveries.setdefault((o.driver_id, local_date(o.delivery)), [0, 0])
        d[0] += 1
        d[1] += o.delivery - o.pickup
        for day in days_overlapping(o.pickup, o.delivery):
            day_start, day_end = day_bounds(day)
            piece = (max(o.pickup, day_start), min(o.delivery, day_end))
            pieces.setdefault((o.driver_id, day), []).append(piece)
    busy = {key: _union_seconds(intervals) for key, intervals in pieces.items()}

    by_driver: dict[str, list[Shift]] = {}
    for s in shifts:
        by_driver.setdefault(s.driver_id, []).append(s)
    session_seconds: dict[tuple[str, date], int] = {}
    break_seconds: dict[tuple[str, date], int] = {}
    for driver_id, events in by_driver.items():
        ordered = sorted(events, key=lambda e: (e.ts, e.event_id))
        onoff = [e for e in ordered if e.event_type in ("online", "offline")]
        breaks_ = [e for e in ordered if e.event_type in ("break_start", "break_end")]
        sessions = _pairs(onoff, "online", "offline")
        breaks = _pairs(breaks_, "break_start", "break_end")
        for start, end in sessions:
            for day in days_overlapping(start, end):
                day_start, day_end = day_bounds(day)
                key = (driver_id, day)
                overlap = min(end, day_end) - max(start, day_start)
                session_seconds[key] = session_seconds.get(key, 0) + overlap
        for b_start, b_end in breaks:
            for s_start, s_end in sessions:
                if not (s_start < b_end and s_end > b_start):
                    continue
                for day in days_overlapping(b_start, b_end):
                    day_start, day_end = day_bounds(day)
                    overlap = min(b_end, s_end, day_end) - max(b_start, s_start, day_start)
                    if overlap > 0:
                        key = (driver_id, day)
                        break_seconds[key] = break_seconds.get(key, 0) + overlap

    rows: dict[tuple[str, date], DriverRow] = {}
    for key in set(deliveries) | set(busy) | set(session_seconds):
        count, seconds = deliveries.get(key, [0, 0])
        busy_seconds = busy.get(key, 0)
        online: int | None = None
        if key in session_seconds:
            online = session_seconds[key] - break_seconds.get(key, 0)
        utilisation = None
        if online is not None and online > 0:
            utilisation = ratio(busy_seconds * 100, online, 2)
        busy_minutes = (Decimal(busy_seconds) / Decimal(60)).quantize(
            Decimal("0.01"), rounding=ROUND_HALF_UP
        )
        rows[key] = (
            count,
            ratio(seconds, 60 * count, 2),
            utilisation,
            busy_minutes,
            None if online is None else ratio(online, 60, 2),
        )
    return rows
