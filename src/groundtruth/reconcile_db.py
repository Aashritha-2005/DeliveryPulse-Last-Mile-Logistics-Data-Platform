"""Compare the dispositions stored in PostgreSQL with the defect manifest (tests and reports only).

Usage (from the project root)::

    uv run python -m src.groundtruth.reconcile_db [--dataset NAME]

Reads ``audit.record_dispositions`` and ``audit.pending_resolutions`` of the succeeded batches
of the dataset (as ``deliverypulse_etl``) and applies the same four comparisons as
``make reconcile``. Fails (exit 1) on any mismatch.
"""

import argparse
import logging
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Any, LiteralString

import psycopg

from src.config import ConfigError, load_database_settings
from src.database.postgres_state import STATUS_SUCCEEDED, Connection
from src.groundtruth.manifest import MANIFEST_FILE_NAME, load_manifest
from src.groundtruth.reconcile import DEFAULT_GROUND_TRUTH_DIR, compare_with_manifest, report
from src.groundtruth.verify import expected_reconciliation, load_dataset
from src.transformations.transform import DEFAULT_RAW_DIR, TransformError, resolve_dataset
from src.utils.logging import configure_logging

logger = logging.getLogger("deliverypulse.reconcile_db")

_BATCHES: LiteralString = (
    "SELECT DISTINCT batch_id, batch_sequence FROM audit.pipeline_runs "
    "WHERE dataset = %s AND status = %s"
)


def load_db_dispositions(
    conn: Connection, dataset: str
) -> tuple[dict[int, list[dict[str, Any]]], dict[tuple[str, int], int]]:
    """Stored dispositions by batch sequence, and resolved pending counts per (source, batch)."""
    batches = {
        str(row[0]): int(row[1])
        for row in conn.execute(_BATCHES, (dataset, STATUS_SUCCEEDED)).fetchall()
    }
    rows: dict[int, list[dict[str, Any]]] = {sequence: [] for sequence in batches.values()}
    dispositions = conn.execute(
        "SELECT batch_id, source, row_number, record_key, disposition, reason "
        "FROM audit.record_dispositions WHERE batch_id = ANY(%s) "
        "ORDER BY batch_id, source, row_number",
        (list(batches),),
    ).fetchall()
    for batch_id, source, row_number, record_key, disposition, reason in dispositions:
        rows[batches[batch_id]].append(
            {
                "source": source,
                "row_number": row_number,
                "record_key": record_key,
                "disposition": disposition,
                "reason": reason,
            }
        )
    resolved: dict[tuple[str, int], int] = {}
    counts = conn.execute(
        "SELECT resolved_in_batch_id, source, count(*) FROM audit.pending_resolutions "
        "WHERE outcome = 'resolved' AND resolved_in_batch_id = ANY(%s) "
        "GROUP BY resolved_in_batch_id, source",
        (list(batches),),
    ).fetchall()
    for batch_id, source, count in counts:
        resolved[(str(source), batches[str(batch_id)])] = int(count)
    return rows, resolved


def main(argv: Sequence[str] | None = None) -> int:
    """Command-line entry point; returns the process exit code."""
    parser = argparse.ArgumentParser(description="Reconcile stored dispositions.")
    parser.add_argument("--raw-dir", type=Path, default=DEFAULT_RAW_DIR)
    parser.add_argument("--ground-truth-dir", type=Path, default=DEFAULT_GROUND_TRUTH_DIR)
    parser.add_argument("--dataset", default=None)
    args = parser.parse_args(argv)
    configure_logging()
    raw_dir: Path = args.raw_dir
    ground_truth_dir: Path = args.ground_truth_dir
    try:
        dataset_dir = resolve_dataset(raw_dir, args.dataset)
        manifest_path = ground_truth_dir / dataset_dir.name / MANIFEST_FILE_NAME
        if not manifest_path.is_file():
            logger.error("Manifest %s not found.", manifest_path)
            return 1
        manifest = load_manifest(manifest_path)
        settings = load_database_settings("etl")
        with psycopg.connect(settings.conninfo(), autocommit=True) as conn:
            rows, resolved = load_db_dispositions(conn, dataset_dir.name)
    except (TransformError, ConfigError, psycopg.Error) as exc:
        logger.error("%s", exc)
        return 1
    if len(rows) != len(manifest.batch_ids):
        logger.error(
            "%d of %d batches are loaded; run `make load` first.",
            len(rows),
            len(manifest.batch_ids),
        )
        return 1
    problems = compare_with_manifest(dataset_dir, manifest, rows, resolved)
    if problems:
        for problem in problems[:50]:
            logger.error("mismatch: %s", problem)
        logger.error("%d mismatches between the database and the manifest.", len(problems))
        return 1
    report(expected_reconciliation(load_dataset(dataset_dir), manifest.cases))
    logger.info(
        "database reconciliation passed: %d cases and every source and batch match the manifest",
        len(manifest.cases),
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
