"""GPS plausibility rule of the data contract.

A ping is compared with the driver's latest accepted ping that precedes it in
``(event_ts, event_id)`` order, across all batches processed so far. For in-order data that is
simply the last accepted ping; for a late ping it is the accepted ping just before it in time.
Only the predecessor is checked, never the successor. Rejected pings never become a reference,
so only the bad ping is flagged, not the one after it. A long gap at a plausible speed passes.
"""

import math
from dataclasses import dataclass

from src.contract import GPS_IMPOSSIBLE_SPEED, GPS_NON_POSITIVE_INTERVAL

EARTH_RADIUS_KM = 6371.0088


@dataclass(frozen=True, order=True)
class Ping:
    """An accepted GPS reference point; ordering is (event_ts, event_id)."""

    event_ts: int
    event_id: str
    lat: float
    lon: float


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in kilometres."""
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = phi2 - phi1
    dlambda = math.radians(lon2 - lon1)
    h = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return 2 * EARTH_RADIUS_KM * math.asin(math.sqrt(h))


def check_ping(previous: Ping | None, ping: Ping, max_speed_kmh: float) -> str | None:
    """Reason code if ``ping`` is implausible after ``previous``; None if it passes."""
    if previous is None:
        return None
    seconds = ping.event_ts - previous.event_ts
    if seconds <= 0:
        return GPS_NON_POSITIVE_INTERVAL
    distance = haversine_km(previous.lat, previous.lon, ping.lat, ping.lon)
    if distance / seconds * 3600 > max_speed_kmh:
        return GPS_IMPOSSIBLE_SPEED
    return None
