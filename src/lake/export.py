"""Export the raw, cleaned and aggregated Parquet layers to S3-compatible storage.

Layout (Hive-style, one prefix per table, so each table can be read as one dataset):

    raw/<source>/date=<ingestion date>/batch-<NN>.parquet
    cleaned/drivers/part-0.parquet
    cleaned/<orders|delivery_events|shift_events>/date=<reporting date>/part-0.parquet
    aggregated/<daily_delivery_metrics|driver_performance>/date=<metric_date>/part-0.parquet

* The ingestion date of a raw batch is the reporting date (Asia/Kolkata) of its cutoff, the
  window end recorded in ``batch.json``: the day the batch is delivered. It comes from the
  data, never from the clock, so a re-export writes the same keys.
* The cleaned layer is uploaded byte for byte from ``make transform`` output, which already
  partitions orders and events by reporting date and leaves drivers unpartitioned.
* Aggregates come from the database (read-only role), one partition per ``metric_date``.

An export uploads every object first and only then deletes keys under the three layer prefixes
that the new export no longer contains, so the lake is never emptied part-way.
"""

import argparse
import io
import itertools
import sys
from collections.abc import Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import psycopg
import pyarrow as pa
import pyarrow.parquet as pq

from src.config import ConfigError, load_database_settings, load_s3_settings
from src.contract import SOURCE_COLUMNS, SOURCE_FILES, format_reporting_date
from src.database.postgres_state import Connection
from src.lake.s3 import (
    S3_ERRORS,
    LakeS3Error,
    S3Client,
    S3Object,
    bucket_exists,
    create_client,
    delete_keys,
    list_objects,
    put_bytes,
)
from src.transformations.records import Batch, BatchSchemaError, batch_folders, load_batch
from src.transformations.transform import (
    DEFAULT_PROCESSED_DIR,
    DEFAULT_RAW_DIR,
    TransformError,
    resolve_dataset,
)

PARQUET_CONTENT_TYPE = "application/vnd.apache.parquet"
LAYERS = ("raw", "cleaned", "aggregated")

RAW_BASE_COLUMNS = (
    ("batch_id", pa.string()),
    ("batch_sequence", pa.int32()),
    ("source_file", pa.string()),
    ("source_row_number", pa.int32()),
    ("record_key", pa.string()),
    ("parse_error", pa.bool_()),
    ("raw_payload", pa.string()),
)
# Raw values stay text, exactly as received; the contract columns are added for convenience.
RAW_SCHEMAS = {
    source: pa.schema([*RAW_BASE_COLUMNS, *((column, pa.string()) for column in columns)])
    for source, columns in SOURCE_COLUMNS.items()
}
# Column types follow sql/schema.sql.
AGGREGATE_SCHEMAS = {
    "daily_delivery_metrics": pa.schema(
        [
            ("metric_date", pa.date32()),
            ("city", pa.string()),
            ("total_orders", pa.int32()),
            ("avg_delivery_minutes", pa.decimal128(8, 2)),
            ("late_rate", pa.decimal128(5, 4)),
            ("cancelled_rate", pa.decimal128(5, 4)),
            ("delivered_orders", pa.int32()),
            ("late_orders", pa.int32()),
            ("cancelled_orders", pa.int32()),
        ]
    ),
    "driver_performance": pa.schema(
        [
            ("driver_id", pa.string()),
            ("metric_date", pa.date32()),
            ("deliveries", pa.int32()),
            ("avg_delivery_minutes", pa.decimal128(8, 2)),
            ("utilisation_pct", pa.decimal128(6, 2)),
            ("busy_minutes", pa.decimal128(8, 2)),
            ("online_minutes", pa.decimal128(8, 2)),
        ]
    ),
}
AGGREGATE_QUERIES = {
    "daily_delivery_metrics": """
        SELECT metric_date, city, total_orders, avg_delivery_minutes, late_rate,
               cancelled_rate, delivered_orders, late_orders, cancelled_orders
        FROM analytics.daily_delivery_metrics
        ORDER BY metric_date, city
    """,
    "driver_performance": """
        SELECT driver_id, metric_date, deliveries, avg_delivery_minutes, utilisation_pct,
               busy_minutes, online_minutes
        FROM analytics.driver_performance
        ORDER BY metric_date, driver_id
    """,
}

type AggregateRows = Mapping[str, Sequence[Mapping[str, Any]]]


class LakeExportError(RuntimeError):
    """The lake cannot be built or exported."""


@dataclass(frozen=True)
class LakeObject:
    """One object of the lake as it should be stored: key, Parquet bytes and row count."""

    key: str
    body: bytes
    rows: int


@dataclass(frozen=True)
class ExportResult:
    """Summary of one export."""

    bucket: str
    uploaded: tuple[S3Object, ...]
    rows: int
    stale_deleted: int

    @property
    def bytes_written(self) -> int:
        """Total bytes uploaded."""
        return sum(item.size for item in self.uploaded)


def validate_prefix(prefix: str) -> str:
    """Return ``prefix`` if it is empty or a relative folder ending in ``/``."""
    if prefix and (not prefix.endswith("/") or prefix.startswith("/") or "//" in prefix):
        raise LakeExportError(f"Lake prefix must be empty or end with '/': {prefix!r}")
    return prefix


def table_to_parquet_bytes(table: pa.Table) -> bytes:
    """Serialise ``table`` as Parquet bytes (deterministic for one pyarrow version)."""
    sink = io.BytesIO()
    pq.write_table(table, sink)
    return sink.getvalue()


def _table_object(key: str, table: pa.Table) -> LakeObject:
    return LakeObject(key=key, body=table_to_parquet_bytes(table), rows=table.num_rows)


def ingestion_date(batch: Batch) -> str:
    """The batch's ingestion date: the reporting date of its cutoff (window end)."""
    return format_reporting_date(batch.window_end)


def raw_table(batch: Batch, source: str, source_file: str) -> pa.Table:
    """One normalised raw object: batch metadata, the payload and the contract fields.

    Built column by column (no dictionary per row), so the only per-row objects in memory are
    the batch's own records.
    """
    records = batch.records[source]
    count = len(records)
    columns: dict[str, list[Any]] = {
        "batch_id": [batch.batch_id] * count,
        "batch_sequence": [batch.sequence] * count,
        "source_file": [source_file] * count,
        "source_row_number": [record.row_number for record in records],
        "record_key": [record.key for record in records],
        "parse_error": [record.parse_error for record in records],
        "raw_payload": [record.raw for record in records],
    }
    for column in SOURCE_COLUMNS[source]:
        columns[column] = [record.fields.get(column) for record in records]
    return pa.Table.from_pydict(columns, schema=RAW_SCHEMAS[source])


def raw_objects(dataset_dir: Path, prefix: str = "") -> Iterator[LakeObject]:
    """The raw layer: one object per batch and source, partitioned by ingestion date.

    Objects are built one at a time; only one batch's records are in memory.
    """
    for folder in batch_folders(dataset_dir):
        batch = load_batch(folder)
        day = ingestion_date(batch)
        for source, source_file in SOURCE_FILES:
            key = f"{prefix}raw/{source}/date={day}/batch-{batch.sequence:02d}.parquet"
            yield _table_object(key, raw_table(batch, source, source_file))
        del batch  # release this batch's records before loading the next


def cleaned_objects(cleaned_dir: Path, prefix: str = "") -> Iterator[LakeObject]:
    """The cleaned layer: every Parquet file of ``make transform`` output, unchanged."""
    if not cleaned_dir.is_dir():
        raise LakeExportError(f"Cleaned Parquet not found at {cleaned_dir}; run make transform.")
    paths = sorted(cleaned_dir.rglob("*.parquet"))
    if not paths:
        raise LakeExportError(f"No Parquet files under {cleaned_dir}; run make transform.")
    for path in paths:
        relative = path.relative_to(cleaned_dir).as_posix()
        yield LakeObject(
            key=f"{prefix}cleaned/{relative}",
            body=path.read_bytes(),
            rows=pq.read_metadata(path).num_rows,
        )


def aggregated_objects(aggregates: AggregateRows, prefix: str = "") -> Iterator[LakeObject]:
    """The aggregated layer: one object per table and ``metric_date``."""
    for table_name, schema in AGGREGATE_SCHEMAS.items():
        by_day: dict[str, list[Mapping[str, Any]]] = {}
        for row in aggregates.get(table_name, ()):
            by_day.setdefault(row["metric_date"].isoformat(), []).append(row)
        for day, rows in sorted(by_day.items()):
            table = pa.Table.from_pylist([dict(row) for row in rows], schema=schema)
            key = f"{prefix}aggregated/{table_name}/date={day}/part-0.parquet"
            yield _table_object(key, table)


def read_aggregates(conn: Connection) -> dict[str, list[dict[str, Any]]]:
    """Every row of the two aggregate tables, as dictionaries in a stable order."""
    aggregates: dict[str, list[dict[str, Any]]] = {}
    for table_name, query in AGGREGATE_QUERIES.items():
        columns = AGGREGATE_SCHEMAS[table_name].names
        rows = conn.execute(query).fetchall()
        aggregates[table_name] = [dict(zip(columns, row, strict=True)) for row in rows]
    return aggregates


def build_lake(
    dataset_dir: Path, cleaned_dir: Path, aggregates: AggregateRows, prefix: str = ""
) -> Iterator[LakeObject]:
    """Every object of the lake, built lazily one at a time (raw, then cleaned, then aggregated).

    Only the keys seen so far are kept, to refuse two objects with one key.
    """
    validate_prefix(prefix)
    if not cleaned_dir.is_dir():  # fail before any upload, not after the raw layer
        raise LakeExportError(f"Cleaned Parquet not found at {cleaned_dir}; run make transform.")
    seen: set[str] = set()
    for item in itertools.chain(
        raw_objects(dataset_dir, prefix),
        cleaned_objects(cleaned_dir, prefix),
        aggregated_objects(aggregates, prefix),
    ):
        if item.key in seen:
            raise LakeExportError(f"Two lake objects would share the key {item.key}.")
        seen.add(item.key)
        yield item


def layer_objects(client: S3Client, bucket: str, prefix: str = "") -> list[S3Object]:
    """Every stored object under the three layer prefixes."""
    return [item for layer in LAYERS for item in list_objects(client, bucket, f"{prefix}{layer}/")]


def export_lake(
    client: S3Client, bucket: str, objects: Iterable[LakeObject], prefix: str = ""
) -> ExportResult:
    """Upload ``objects`` one at a time, then delete the stale keys under the layer prefixes.

    Each object's bytes are released after its upload, so memory stays at one object. The
    bucket must already exist: the pipeline key may not create buckets (``make s3-up`` creates
    it with the admin key).
    """
    validate_prefix(prefix)
    if not bucket_exists(client, bucket):
        raise LakeExportError(f"Bucket {bucket!r} does not exist; run make s3-up to create it.")
    uploaded: list[S3Object] = []
    rows = 0
    for item in objects:
        uploaded.append(put_bytes(client, bucket, item.key, item.body, PARQUET_CONTENT_TYPE))
        rows += item.rows
    current = {item.key for item in uploaded}
    stale = [item.key for item in layer_objects(client, bucket, prefix) if item.key not in current]
    deleted = delete_keys(client, bucket, stale)
    return ExportResult(bucket=bucket, uploaded=tuple(uploaded), rows=rows, stale_deleted=deleted)


def main(argv: Sequence[str] | None = None) -> int:
    """Command-line entry point (`make lake-export`)."""
    parser = argparse.ArgumentParser(description="Export the DeliveryPulse lake layers to S3.")
    parser.add_argument("--raw-dir", type=Path, default=DEFAULT_RAW_DIR)
    parser.add_argument("--processed-dir", type=Path, default=DEFAULT_PROCESSED_DIR)
    parser.add_argument("--dataset", default=None, help="dataset folder name in --raw-dir")
    args = parser.parse_args(argv)
    try:
        settings = load_s3_settings("pipeline")
        dataset_dir = resolve_dataset(args.raw_dir, args.dataset)
        database = load_database_settings("readonly")
        with psycopg.connect(database.conninfo(), autocommit=True) as conn:
            aggregates = read_aggregates(conn)
        cleaned_dir = args.processed_dir / dataset_dir.name / "cleaned"
        objects = build_lake(dataset_dir, cleaned_dir, aggregates)
        result = export_lake(create_client(settings), settings.bucket, objects)
    except S3_ERRORS as exc:
        print(f"ERROR: S3 request failed: {exc}", file=sys.stderr)
        return 1
    except (
        BatchSchemaError,
        ConfigError,
        LakeExportError,
        LakeS3Error,
        OSError,
        TransformError,
        psycopg.Error,
    ) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    print(
        f"lake export complete: bucket={result.bucket} objects={len(result.uploaded)} "
        f"rows={result.rows} bytes={result.bytes_written} stale_deleted={result.stale_deleted}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
