"""Read the lake back through the S3 API and reconcile it with its sources (`make lake-check`).

Every layer is rebuilt from its source exactly as the export builds it (raw files, the cleaned
Parquet of ``make transform``, the database aggregates through the read-only role) and compared
with what the bucket holds:

* the stored keys under ``raw/``, ``cleaned/`` and ``aggregated/`` equal the expected keys (no
  missing and no extra object);
* every object is downloaded through the S3 API and its row count, read from the downloaded
  Parquet, equals the source row count;
* its bytes equal the expected bytes.
"""

import argparse
import io
import sys
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path

import psycopg
import pyarrow.parquet as pq

from src.config import ConfigError, load_database_settings, load_s3_settings
from src.lake.export import (
    LAYERS,
    LakeExportError,
    LakeObject,
    build_lake,
    layer_objects,
    read_aggregates,
    validate_prefix,
)
from src.lake.s3 import S3_ERRORS, S3Client, create_client, download_bytes
from src.transformations.records import BatchSchemaError
from src.transformations.transform import (
    DEFAULT_PROCESSED_DIR,
    DEFAULT_RAW_DIR,
    TransformError,
    resolve_dataset,
)

MAX_REPORTED_PROBLEMS = 20


class LakeCheckError(RuntimeError):
    """The lake does not match its sources."""


@dataclass(frozen=True)
class LayerReport:
    """Objects and rows read back for one layer."""

    layer: str
    objects: int
    rows: int


def check_lake(
    client: S3Client, bucket: str, expected: Iterable[LakeObject], prefix: str = ""
) -> list[LayerReport]:
    """Reconcile the stored lake with ``expected``; raise :class:`LakeCheckError` on a mismatch.

    ``expected`` is consumed one object at a time; each stored object is downloaded, compared
    and released before the next, so memory stays at one object.
    """
    validate_prefix(prefix)
    stored = {item.key for item in layer_objects(client, bucket, prefix)}
    wanted: set[str] = set()
    problems: list[str] = []
    objects = dict.fromkeys(LAYERS, 0)
    rows = dict.fromkeys(LAYERS, 0)
    for item in expected:
        wanted.add(item.key)
        if item.key not in stored:
            continue
        payload = download_bytes(client, bucket, item.key)
        stored_rows = pq.read_metadata(io.BytesIO(payload)).num_rows
        if stored_rows != item.rows:
            problems.append(f"{item.key}: {stored_rows} rows, source has {item.rows}")
        if payload != item.body:
            problems.append(f"{item.key}: content differs from the source")
        layer = item.key.removeprefix(prefix).split("/", 1)[0]
        objects[layer] += 1
        rows[layer] += stored_rows
    problems = [f"missing object: {key}" for key in sorted(wanted - stored)] + problems
    problems += [f"unexpected object: {key}" for key in sorted(stored - wanted)]
    if problems:
        shown = problems[:MAX_REPORTED_PROBLEMS]
        more = len(problems) - len(shown)
        suffix = f"\n  ... and {more} more" if more else ""
        raise LakeCheckError(f"{len(problems)} lake problem(s):\n  " + "\n  ".join(shown) + suffix)
    return [LayerReport(layer, objects[layer], rows[layer]) for layer in LAYERS]


def main(argv: Sequence[str] | None = None) -> int:
    """Command-line entry point (`make lake-check`)."""
    parser = argparse.ArgumentParser(description="Reconcile the S3 lake with its sources.")
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
        expected = build_lake(dataset_dir, cleaned_dir, aggregates)
        reports = check_lake(create_client(settings), settings.bucket, expected)
    except S3_ERRORS as exc:
        print(f"ERROR: S3 request failed: {exc}", file=sys.stderr)
        return 1
    except (
        BatchSchemaError,
        ConfigError,
        LakeCheckError,
        LakeExportError,
        OSError,
        TransformError,
        psycopg.Error,
    ) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    for report in reports:
        print(f"{report.layer}: {report.objects} objects, {report.rows} rows, all match the source")
    total_objects = sum(report.objects for report in reports)
    total_rows = sum(report.rows for report in reports)
    print(f"lake check passed: bucket={settings.bucket} objects={total_objects} rows={total_rows}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
