"""Partitioned vs non-partitioned Parquet reads (the read benchmark).

From the shards of one size, typed delivery events (event_ts as a timestamp, plus the reporting
date) are written twice: as one file and as a Hive-style dataset partitioned by ``date``. Each
engine then answers the same query on both copies: events and GPS pings per day for one week.
On the partitioned copy only that week's folders need to be read (partition pruning); on the
single file the engine must scan, helped only by row-group statistics.
"""

from collections.abc import Sequence
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from src.bench.spec import EVENTS_FILE, GPS_PING, OFFSET_SECONDS, TS_FORMAT

SINGLE_FILE = "events_single.parquet"
PARTITIONED_DIR = "events_partitioned"
FIRST_DAY = date(2026, 9, 10)  # first reporting date of the generated data
WEEK = tuple((FIRST_DAY + timedelta(days=offset)).isoformat() for offset in range(7))


def typed_events(shard: Path) -> pa.Table:
    """One shard's events with a parsable event_ts, typed, with the reporting date."""
    table = pq.read_table(shard / EVENTS_FILE, columns=["event_type", "event_ts"])
    ts = pc.strptime(table["event_ts"], format=TS_FORMAT, unit="s", error_is_null=True)
    table = pa.table({"event_type": table["event_type"], "event_ts": ts}).filter(pc.is_valid(ts))
    local = pc.add(table["event_ts"], pa.scalar(OFFSET_SECONDS, pa.duration("s")))
    return table.append_column("date", pc.strftime(local, format="%Y-%m-%d"))


def write_read_copies(shards: Sequence[Path], target: Path) -> tuple[Path, Path]:
    """Write the single-file and the partitioned copy of ``shards``' events into ``target``."""
    target.mkdir(parents=True, exist_ok=True)
    single = target / SINGLE_FILE
    partitioned = target / PARTITIONED_DIR
    writer: pq.ParquetWriter | None = None
    try:
        for index, shard in enumerate(shards, start=1):
            table = typed_events(shard)
            if writer is None:
                writer = pq.ParquetWriter(single, table.schema)
            writer.write_table(table)
            pq.write_to_dataset(
                table,
                root_path=str(partitioned),
                partition_cols=["date"],
                basename_template=f"shard-{index:04d}-{{i}}.parquet",
            )
    finally:
        if writer is not None:
            writer.close()
    return single, partitioned


def pandas_week_counts(path: Path) -> list[tuple[str, int, int]]:
    """(date, events, gps_pings) for :data:`WEEK`, read with pandas (pyarrow engine)."""
    frame = pd.read_parquet(path, columns=["date", "event_type"], filters=[("date", "in", WEEK)])
    frame["date"] = frame["date"].astype(str)
    frame["ping"] = frame["event_type"].eq(GPS_PING).astype("int64")
    grouped = frame.groupby("date", sort=True).agg(events=("ping", "size"), pings=("ping", "sum"))
    return [(str(day), int(row.events), int(row.pings)) for day, row in grouped.iterrows()]
