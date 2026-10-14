"""The benchmark workload (src/bench/spec.py) in vectorised pandas."""

from collections.abc import Sequence
from pathlib import Path

import numpy as np
import pandas as pd

from src.bench.spec import (
    CANCELLED,
    DEGREES_TO_RADIANS,
    DELIVERED,
    EARTH_RADIUS_KM,
    EVENT_INPUT_COLUMNS,
    EVENT_REQUIRED,
    EVENT_TYPES,
    EVENTS_FILE,
    GPS_PING,
    MAX_SPEED_KMH,
    OFFSET_SECONDS,
    ORDER_INPUT_COLUMNS,
    ORDER_REQUIRED,
    ORDERS_FILE,
    ROW_ID,
    STATUSES,
    TS_FORMAT,
    WorkloadResult,
)


def read_shards(shards: Sequence[Path], name: str, columns: Sequence[str]) -> pd.DataFrame:
    """All shards' ``name`` files as one frame."""
    return pd.concat(
        [pd.read_parquet(shard / name, columns=list(columns)) for shard in shards],
        ignore_index=True,
    )


EPOCH = pd.Timestamp("1970-01-01", tz="UTC")


def epoch_seconds(text: pd.Series) -> pd.Series:
    """Contract timestamps as epoch seconds (float, exact for whole seconds; NaN if invalid)."""
    parsed = pd.to_datetime(text, format=TS_FORMAT, utc=True, errors="coerce")
    return ((parsed - EPOCH) / pd.Timedelta(seconds=1)).astype("float64")


def reporting_date(seconds: pd.Series) -> pd.Series:
    """The Asia/Kolkata calendar date (``YYYY-MM-DD``) of epoch seconds."""
    shifted = pd.to_datetime((seconds + OFFSET_SECONDS).astype("int64"), unit="s")
    return shifted.dt.strftime("%Y-%m-%d")


def present(frame: pd.DataFrame, columns: Sequence[str]) -> pd.Series:
    """True where every column in ``columns`` is non-empty."""
    mask = pd.Series(True, index=frame.index)
    for column in columns:
        mask &= frame[column].fillna("").ne("")
    return mask


def clean_orders(orders: pd.DataFrame) -> tuple[pd.DataFrame, int, int]:
    """Rules O1-O4 and the dedup; returns (clean orders, rejected, duplicates)."""
    created = epoch_seconds(orders["order_created_ts"])
    promised = epoch_seconds(orders["promised_ts"])
    updated = epoch_seconds(orders["source_updated_at"])
    pickup = epoch_seconds(orders["pickup_ts"])
    delivery = epoch_seconds(orders["delivery_ts"])
    pickup_given = orders["pickup_ts"].fillna("").ne("")
    delivery_given = orders["delivery_ts"].fillna("").ne("")
    delivered = orders["status"].eq(DELIVERED)
    valid = (
        present(orders, ORDER_REQUIRED)
        & created.notna()
        & promised.notna()
        & updated.notna()
        & orders["status"].isin(STATUSES)
        & (~pickup_given | pickup.notna())
        & (~delivery_given | delivery.notna())
        & (~delivered | (pickup.notna() & delivery.notna() & (delivery >= pickup)))
    )
    frame = pd.DataFrame(
        {
            "order_id": orders["order_id"],
            "status": orders["status"],
            "city": orders["city"],
            "created": created,
            "promised": promised,
            "updated": updated,
            "pickup": pickup,
            "delivery": delivery,
            ROW_ID: orders[ROW_ID],
        }
    )[valid]
    newest = frame.sort_values(["order_id", "updated", ROW_ID]).drop_duplicates(
        "order_id", keep="last"
    )
    return newest, int((~valid).sum()), len(frame) - len(newest)


def order_days(clean: pd.DataFrame) -> list[tuple[str, str, int, int, int, int, int]]:
    """Daily metrics per reporting date of order_created_ts and city."""
    delivered = clean["status"].eq(DELIVERED)
    frame = pd.DataFrame(
        {
            "date": reporting_date(clean["created"]),
            "city": clean["city"],
            "delivered": delivered.astype("int64"),
            "late": (delivered & (clean["delivery"] > clean["promised"])).astype("int64"),
            "cancelled": clean["status"].eq(CANCELLED).astype("int64"),
            "seconds": (clean["delivery"] - clean["pickup"]).where(delivered, 0).astype("int64"),
        }
    )
    grouped = frame.groupby(["date", "city"], sort=True).agg(
        orders=("delivered", "size"),
        delivered=("delivered", "sum"),
        late=("late", "sum"),
        cancelled=("cancelled", "sum"),
        seconds=("seconds", "sum"),
    )
    return [
        (
            str(date),
            str(city),
            int(row.orders),
            int(row.delivered),
            int(row.late),
            int(row.cancelled),
            int(row.seconds),
        )
        for (date, city), row in grouped.iterrows()
    ]


def clean_events(events: pd.DataFrame) -> tuple[pd.DataFrame, int, int]:
    """Rules E1-E2 and the dedup; returns (clean events, rejected, duplicates)."""
    ts = epoch_seconds(events["event_ts"])
    lat = pd.to_numeric(events["lat"], errors="coerce")
    lon = pd.to_numeric(events["lon"], errors="coerce")
    ping = events["event_type"].eq(GPS_PING)
    coordinates_ok = lat.between(-90, 90) & lon.between(-180, 180)
    valid = (
        present(events, EVENT_REQUIRED)
        & ts.notna()
        & events["event_type"].isin(EVENT_TYPES)
        & (~ping | coordinates_ok)
    )
    frame = pd.DataFrame(
        {
            "event_id": events["event_id"],
            "driver_id": events["driver_id"],
            "ping": ping,
            "ts": ts,
            "lat": lat,
            "lon": lon,
            ROW_ID: events[ROW_ID],
        }
    )[valid]
    first = frame.sort_values(["event_id", ROW_ID]).drop_duplicates("event_id", keep="first")
    return first, int((~valid).sum()), len(frame) - len(first)


def flag_gps(clean: pd.DataFrame) -> pd.Series:
    """Per driver, ordered by (event_ts, event_id): True for an impossible GPS ping."""
    pings = clean[clean["ping"]].sort_values(["driver_id", "ts", "event_id"])
    same_driver = pings["driver_id"].eq(pings["driver_id"].shift())
    phi1 = pings["lat"].shift() * DEGREES_TO_RADIANS
    phi2 = pings["lat"] * DEGREES_TO_RADIANS
    dphi = (pings["lat"] - pings["lat"].shift()) * DEGREES_TO_RADIANS
    dlambda = (pings["lon"] - pings["lon"].shift()) * DEGREES_TO_RADIANS
    h = np.sin(dphi / 2) ** 2 + np.cos(phi1) * np.cos(phi2) * np.sin(dlambda / 2) ** 2
    distance = 2 * EARTH_RADIUS_KM * np.arcsin(np.sqrt(h))
    seconds = pings["ts"] - pings["ts"].shift()
    speed = distance / seconds * 3600
    flagged = same_driver & ((seconds <= 0) | (speed > MAX_SPEED_KMH))
    return flagged.reindex(clean.index, fill_value=False)


def event_days(clean: pd.DataFrame, flagged: pd.Series) -> list[tuple[str, int, int, int]]:
    """Daily counts per reporting date of event_ts."""
    frame = pd.DataFrame(
        {
            "date": reporting_date(clean["ts"]),
            "ping": clean["ping"].astype("int64"),
            "flagged": flagged.astype("int64"),
        }
    )
    grouped = frame.groupby("date", sort=True).agg(
        events=("ping", "size"), pings=("ping", "sum"), flagged=("flagged", "sum")
    )
    return [
        (str(date), int(row.events), int(row.pings), int(row.flagged))
        for date, row in grouped.iterrows()
    ]


def run_workload(shards: Sequence[Path]) -> WorkloadResult:
    """The whole workload over ``shards``."""
    orders = read_shards(shards, ORDERS_FILE, ORDER_INPUT_COLUMNS)
    clean, rejected, duplicates = clean_orders(orders)
    result = WorkloadResult(
        orders_input=len(orders),
        orders_rejected=rejected,
        orders_duplicate=duplicates,
        orders_clean=len(clean),
        order_days=order_days(clean),
    )
    del orders, clean
    events = read_shards(shards, EVENTS_FILE, EVENT_INPUT_COLUMNS)
    clean_e, rejected_e, duplicates_e = clean_events(events)
    result.events_input = len(events)
    del events
    flagged = flag_gps(clean_e)
    result.events_rejected = rejected_e
    result.events_duplicate = duplicates_e
    result.events_clean = len(clean_e)
    result.gps_flagged = int(flagged.sum())
    result.event_days = event_days(clean_e, flagged)
    return result
