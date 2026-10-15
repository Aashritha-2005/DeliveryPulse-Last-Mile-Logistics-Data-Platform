"""Container entry point: verify the existing dataset before any pipeline writes."""

import argparse
import hashlib
import json
import time
from pathlib import Path
from typing import Any

import psycopg
from psycopg import sql

from src.config import load_database_settings
from src.database import load_postgres, metrics
from src.ingestion import generate_data
from src.ingestion.generator_config import GeneratorConfig
from src.quality import run as quality
from src.transformations import transform

DATA = Path("/work")
DATASET = GeneratorConfig.create(orders=50000, seed=42).dataset_name()


def verify_identity(conn: Any, folder: Path) -> None:
    """Require the same three successful batches, including verified source hashes."""
    expected = {}
    for batch in sorted(folder.glob("batch-*")):
        metadata = json.loads((batch / "batch.json").read_text())
        for name, info in metadata["files"].items():
            path = batch / name
            if (
                path.parent != batch
                or hashlib.sha256(path.read_bytes()).hexdigest() != info["sha256"]
            ):
                raise ValueError("Generated source hash mismatch")
        expected[int(metadata["batch_sequence"])] = metadata["batch_id"]
    with conn.transaction():
        conn.execute("SET TRANSACTION READ ONLY")
        rows = conn.execute(
            "SELECT DISTINCT dataset, batch_sequence, batch_id FROM audit.pipeline_runs "
            "WHERE status = 'succeeded'"
        ).fetchall()
    actual = {(row[0], int(row[1]), row[2]) for row in rows}
    if len(expected) != 3 or actual != {(DATASET, k, v) for k, v in expected.items()}:
        raise ValueError("Dataset identity differs from the loaded database; refusing writes")


def snapshot(conn: Any) -> dict[str, Any]:
    """Hash complete analytics rows with bounded client memory; exclude mutable audit rows."""
    result = {}
    with conn.transaction():
        conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        tables = conn.execute(
            "SELECT tablename FROM pg_tables WHERE schemaname = 'analytics' ORDER BY tablename"
        ).fetchall()
        for (table,) in tables:
            digest = hashlib.sha256()
            count = 0
            with conn.cursor(name="snapshot_rows") as cursor:
                cursor.execute(
                    sql.SQL(
                        'SELECT to_jsonb(t)::text FROM {} t ORDER BY to_jsonb(t)::text COLLATE "C"'
                    ).format(sql.Identifier("analytics", table))
                )
                for (row,) in cursor:
                    digest.update(row.encode() + b"\n")
                    count += 1
            result[table] = {"rows": count, "sha256": digest.hexdigest()}
    return result


def main() -> int:
    """Generate, probe connectivity, snapshot, or run the verified pipeline."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=["generate", "pipeline", "connectivity", "snapshot"])
    args = parser.parse_args()
    begun = time.monotonic()
    try:
        if args.operation == "generate":
            return generate_data.main(
                [
                    "--orders",
                    "50000",
                    "--seed",
                    "42",
                    "--raw-dir",
                    str(DATA / "raw"),
                    "--ground-truth-dir",
                    str(DATA / "ground_truth"),
                ]
            )
        settings = load_database_settings("etl")
        with psycopg.connect(settings.conninfo(), autocommit=True) as conn:
            if args.operation == "connectivity":
                with conn.transaction():
                    conn.execute("SET TRANSACTION READ ONLY")
                    if conn.execute("SELECT 1").fetchone() != (1,):
                        raise RuntimeError("Connectivity probe failed")
                print("Kubernetes PostgreSQL connectivity: passed")
                return 0
            if args.operation == "snapshot":
                print(json.dumps(snapshot(conn), sort_keys=True))
                return 0
            verify_identity(conn, DATA / "raw" / DATASET)
        print("Kubernetes dataset identity: verified before writes")
        steps = [
            (
                transform.main,
                ["--raw-dir", str(DATA / "raw"), "--processed-dir", str(DATA / "processed")],
            ),
            (load_postgres.main, ["--raw-dir", str(DATA / "raw")]),
            (metrics.main, []),
            (quality.main, ["--output", str(DATA / "quality.json")]),
        ]
        for command, arguments in steps:
            if command(arguments) != 0:
                raise RuntimeError("Pipeline step failed")
        print(
            json.dumps(
                {"pipeline": "succeeded", "dataset": DATASET, "seconds": time.monotonic() - begun},
                sort_keys=True,
            )
        )
    except Exception:
        print(
            "ERROR: Kubernetes pipeline failed; check connectivity, credentials, dataset and gate"
        )
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
