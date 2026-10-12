"""Compare the pipeline's dispositions with the defect manifest (tests and reports only).

Usage (from the project root)::

    uv run python -m src.groundtruth.reconcile [--dataset NAME]

Fails (exit 1) on any mismatch in, per source and batch:

1. counts per disposition (and the number of input rows)
2. counts per reason, for non-accepted dispositions
3. the disposition (and, if not accepted, the reason) of every manifest case's record
4. pending records resolved in the batch after the one that held them
"""

import argparse
import json
import logging
import sys
from collections import Counter
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

from src.contract import ACCEPTED, DISPOSITIONS, PENDING, SOURCE_FILES
from src.groundtruth.manifest import MANIFEST_FILE_NAME, Manifest, load_manifest
from src.groundtruth.verify import expected_reconciliation, load_dataset
from src.transformations.transform import (
    DEFAULT_PROCESSED_DIR,
    DEFAULT_RAW_DIR,
    TransformError,
    resolve_dataset,
)
from src.utils.logging import configure_logging

logger = logging.getLogger("deliverypulse.reconcile")

DEFAULT_GROUND_TRUTH_DIR = Path("data/ground_truth")
SOURCES = tuple(source for source, _ in SOURCE_FILES)


def load_pipeline_output(
    processed_dataset_dir: Path,
) -> tuple[dict[int, list[dict[str, Any]]], dict[int, dict[str, Any]]]:
    """Dispositions and summaries written by the transform, by batch sequence."""
    rows: dict[int, list[dict[str, Any]]] = {}
    summaries: dict[int, dict[str, Any]] = {}
    for folder in sorted((processed_dataset_dir / "batches").iterdir()):
        if folder.name.startswith(".") or not folder.is_dir():
            continue
        summary: dict[str, Any] = json.loads((folder / "summary.json").read_text("utf-8"))
        sequence = int(summary["batch_sequence"])
        summaries[sequence] = summary
        table = pq.read_table(folder / "dispositions.parquet")
        batch_rows: list[dict[str, Any]] = table.to_pylist()
        rows[sequence] = batch_rows
    return rows, summaries


def reconcile(raw_dataset_dir: Path, processed_dataset_dir: Path, manifest: Manifest) -> list[str]:
    """Return every difference between the transform output and the manifest (empty = match)."""
    rows, summaries = load_pipeline_output(processed_dataset_dir)
    resolved = {
        (source, sequence): int(summary["sources"][source]["pending_resolved"])
        for sequence, summary in summaries.items()
        for source in SOURCES
    }
    return compare_with_manifest(raw_dataset_dir, manifest, rows, resolved)


def compare_with_manifest(
    raw_dataset_dir: Path,
    manifest: Manifest,
    rows: dict[int, list[dict[str, Any]]],
    resolved: dict[tuple[str, int], int],
) -> list[str]:
    """Every difference between pipeline dispositions and the manifest (empty = match).

    ``rows`` maps batch sequence to disposition rows (keys: source, record_key, disposition,
    reason); ``resolved`` maps (source, batch sequence) to pending records resolved there.
    """
    problems: list[str] = []
    raw_batches = load_dataset(raw_dataset_dir)
    expected = expected_reconciliation(raw_batches, manifest.cases)

    for (source, sequence), counts in expected.items():
        batch_rows = [r for r in rows.get(sequence, []) if r["source"] == source]
        if len(batch_rows) != counts["input"]:
            problems.append(
                f"{source} batch {sequence}: {len(batch_rows)} dispositions "
                f"for {counts['input']} input rows"
            )
        actual = Counter(r["disposition"] for r in batch_rows)
        for disposition in DISPOSITIONS:
            if actual[disposition] != counts[disposition]:
                problems.append(
                    f"{source} batch {sequence} {disposition}: pipeline {actual[disposition]}, "
                    f"manifest {counts[disposition]}"
                )

    expected_reasons = Counter(
        (c.source, c.batch, c.expected_disposition, c.reason)
        for c in manifest.cases
        if c.expected_disposition != ACCEPTED
    )
    actual_reasons = Counter(
        (r["source"], sequence, r["disposition"], r["reason"])
        for sequence, batch_rows in rows.items()
        for r in batch_rows
        if r["disposition"] != ACCEPTED
    )
    for key in sorted(set(expected_reasons) | set(actual_reasons)):
        if expected_reasons[key] != actual_reasons[key]:
            source, sequence, disposition, reason = key
            problems.append(
                f"{source} batch {sequence} {disposition}/{reason or '-'}: pipeline "
                f"{actual_reasons[key]}, manifest {expected_reasons[key]}"
            )

    index: dict[tuple[str, int, str], list[tuple[str, str]]] = {}
    for sequence, batch_rows in rows.items():
        for r in batch_rows:
            record_ref: tuple[str, int, str] = (r["source"], sequence, r["record_key"])
            index.setdefault(record_ref, []).append((r["disposition"], r["reason"]))
    for case in manifest.cases:
        outcomes = index.get((case.source, case.batch, case.record_key), [])
        matched = any(
            disposition == case.expected_disposition
            and (disposition == ACCEPTED or reason == case.reason)
            for disposition, reason in outcomes
        )
        if not matched:
            problems.append(
                f"{case.scenario}: {case.source} batch {case.batch} {case.record_key} expected "
                f"{case.expected_disposition}/{case.reason or '-'}, pipeline {outcomes or 'none'}"
            )

    expected_resolved = Counter(
        (c.source, c.batch + 1) for c in manifest.cases if c.expected_disposition == PENDING
    )
    for sequence in sorted(rows):
        for source in SOURCES:
            actual_resolved = resolved.get((source, sequence), 0)
            if actual_resolved != expected_resolved[(source, sequence)]:
                problems.append(
                    f"{source} batch {sequence} pending resolved: pipeline {actual_resolved}, "
                    f"manifest {expected_resolved[(source, sequence)]}"
                )
    return problems


def report(expected: dict[tuple[str, int], dict[str, int]]) -> None:
    """Print the expected reconciliation table (after a successful comparison)."""
    header = ["source", "batch", "input", *DISPOSITIONS]
    print(" | ".join(header))
    for (source, sequence), counts in expected.items():
        values = [str(counts["input"]), *(str(counts[d]) for d in DISPOSITIONS)]
        print(" | ".join([source, f"batch-{sequence}", *values]))


def main(argv: Sequence[str] | None = None) -> int:
    """Command-line entry point; returns the process exit code."""
    parser = argparse.ArgumentParser(description="Reconcile dispositions with the manifest.")
    parser.add_argument("--raw-dir", type=Path, default=DEFAULT_RAW_DIR)
    parser.add_argument("--processed-dir", type=Path, default=DEFAULT_PROCESSED_DIR)
    parser.add_argument("--ground-truth-dir", type=Path, default=DEFAULT_GROUND_TRUTH_DIR)
    parser.add_argument("--dataset", default=None)
    args = parser.parse_args(argv)
    configure_logging()
    raw_dir: Path = args.raw_dir
    processed_dir: Path = args.processed_dir
    ground_truth_dir: Path = args.ground_truth_dir
    try:
        dataset_dir = resolve_dataset(raw_dir, args.dataset)
    except TransformError as exc:
        logger.error("%s", exc)
        return 1
    name = dataset_dir.name
    manifest_path = ground_truth_dir / name / MANIFEST_FILE_NAME
    processed_dataset_dir = processed_dir / name
    if not manifest_path.is_file() or not (processed_dataset_dir / "batches").is_dir():
        logger.error("Missing manifest or transform output for %s; run make transform.", name)
        return 1
    manifest = load_manifest(manifest_path)
    problems = reconcile(dataset_dir, processed_dataset_dir, manifest)
    if problems:
        for problem in problems[:50]:
            logger.error("mismatch: %s", problem)
        logger.error("%d mismatches between the pipeline and the manifest.", len(problems))
        return 1
    report(expected_reconciliation(load_dataset(dataset_dir), manifest.cases))
    logger.info(
        "reconciliation passed: %d cases and every source and batch match the manifest",
        len(manifest.cases),
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
