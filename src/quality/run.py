"""Run every data-quality check on the current database (`make quality`).

Usage (from the project root)::

    uv run python -m src.quality.run [--threshold CHECK=VALUE ...] [--output PATH]

Connects as ``deliverypulse_etl``. Scope: every succeeded batch of the loaded dataset and every
reporting date with data. Stores the run in ``audit.quality_runs`` / ``audit.quality_results``
(triggered_by ``manual``), prints a summary table, writes a JSON result and exits 1 if a
blocking check fails. Warnings never change the exit code.
"""

import argparse
import json
import logging
import sys
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import psycopg

from src.config import PROJECT_ROOT, ConfigError, load_database_settings
from src.database.metrics import all_dates
from src.database.postgres_state import STATUS_SUCCEEDED, Connection
from src.quality.checks import (
    TRIGGER_MANUAL,
    QualityConfig,
    QualityConfigError,
    QualityRun,
    Scope,
    parse_thresholds,
    result_table,
    run_quality,
    store_run,
)
from src.utils.logging import configure_logging

logger = logging.getLogger("deliverypulse.quality")

DEFAULT_OUTPUT = PROJECT_ROOT / "docs" / "quality" / "quality-latest.json"


class QualityRunError(RuntimeError):
    """The database holds nothing to check."""


def database_scope(conn: Connection) -> Scope:
    """Every succeeded batch (in sequence order) and every reporting date with data."""
    datasets = [
        str(row[0])
        for row in conn.execute(
            "SELECT DISTINCT dataset FROM audit.pipeline_runs WHERE status = %s",
            (STATUS_SUCCEEDED,),
        ).fetchall()
    ]
    if len(datasets) != 1:
        raise QualityRunError(
            f"expected exactly one loaded dataset, found {len(datasets)}; run `make load` first"
        )
    batches = conn.execute(
        "SELECT batch_id FROM audit.pipeline_runs WHERE dataset = %s AND status = %s "
        "GROUP BY batch_id ORDER BY min(batch_sequence)",
        (datasets[0], STATUS_SUCCEEDED),
    ).fetchall()
    with conn.cursor() as cur:
        dates = all_dates(cur)
    return Scope(datasets[0], tuple(str(row[0]) for row in batches), tuple(dates))


def run_database_quality(conn: Connection, config: QualityConfig) -> tuple[QualityRun, int]:
    """Run, store (one transaction) and return the run and its stored id."""
    scope = database_scope(conn)
    with conn.transaction(), conn.cursor() as cur:
        run = run_quality(cur, scope, config, TRIGGER_MANUAL)
        run_id = store_run(cur, run)
    return run, run_id


def run_json(run: QualityRun, run_id: int, scope_batches: int) -> dict[str, Any]:
    """Machine-readable result."""
    return {
        "quality_run_id": run_id,
        "dataset": run.dataset,
        "triggered_by": run.triggered_by,
        "generated_at": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "batches": scope_batches,
        "status": run.status,
        "checks_total": len(run.results),
        "blocking_failures": len(run.blocking_failures),
        "warnings": len(run.warnings),
        "duration_ms": run.duration_ms,
        "results": [
            {
                "check": r.check_name,
                "layer": r.layer,
                "table": r.table_name,
                "severity": r.severity,
                "status": r.status,
                "observed": r.observed_value,
                "threshold": r.threshold,
                "threshold_kind": r.threshold_kind,
                "failing_rows": r.failing_rows,
                "rows_checked": r.rows_checked,
                "sample_keys": list(r.sample_keys),
            }
            for r in run.results
        ],
    }


def main(argv: Sequence[str] | None = None) -> int:
    """Command-line entry point; returns the process exit code."""
    parser = argparse.ArgumentParser(description="Run every data-quality check.")
    parser.add_argument("--threshold", action="append", default=[], metavar="CHECK=VALUE")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args(argv)
    configure_logging()
    output: Path = args.output
    try:
        config = QualityConfig(thresholds=parse_thresholds(args.threshold))
        settings = load_database_settings("etl")
        with psycopg.connect(settings.conninfo(), autocommit=True) as conn:
            run, run_id = run_database_quality(conn, config)
            batches = len(database_scope(conn).batch_ids)
    except (ConfigError, QualityConfigError, QualityRunError, psycopg.Error) as exc:
        logger.error("%s", exc)
        return 1
    print("\n".join(result_table(run.results)))
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        json.dumps(run_json(run, run_id, batches), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    logger.info("quality run %d (%s, %d batches): %s", run_id, run.dataset, batches, run.summary())
    logger.info("result written to %s", output)
    if run.blocking_failures:
        logger.error(
            "blocking failures: %s", ", ".join(r.check_name for r in run.blocking_failures)
        )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
