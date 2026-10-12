"""Prove that every manifest defect type is detected by a quality check (`make quality-report`).

Usage (from the project root)::

    uv run python -m src.groundtruth.quality_coverage [--dataset NAME]

Reads the load-time quality results of every succeeded batch (``audit.quality_results``) and
the defect manifest. For every defect type and batch, the mapped raw-layer check's failing-row
count must equal the manifest count, plus the documented explained difference for the few
checks that also count another defect type. Writes the marked "Quality checks" section of
docs/data_quality_report.md and exits 1 on any undetected type or unexplained difference.
Tests and reports only: the pipeline never reads the ground truth.
"""

import argparse
import logging
import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path

import psycopg

from src.config import ConfigError, load_database_settings
from src.database.postgres_state import STATUS_SUCCEEDED, Connection
from src.groundtruth.manifest import MANIFEST_FILE_NAME, Manifest, case_counts, load_manifest
from src.groundtruth.reconcile import DEFAULT_GROUND_TRUTH_DIR
from src.groundtruth.report_sections import QUALITY_END, QUALITY_START, replace_section
from src.ingestion.generator_config import (
    DERIVED_CASE_TYPES,
    INJECTED_DEFECT_TYPES,
    NATURAL_CASE_TYPES,
)
from src.transformations.transform import DEFAULT_RAW_DIR, TransformError, resolve_dataset
from src.utils.logging import configure_logging

logger = logging.getLogger("deliverypulse.quality_coverage")

DEFAULT_REPORT = Path("docs/data_quality_report.md")
ALL_CASE_TYPES = (*INJECTED_DEFECT_TYPES, *DERIVED_CASE_TYPES, *NATURAL_CASE_TYPES)

# Defect type -> the stored name of the raw-layer check that detects it.
DEFECT_CHECKS: dict[str, str] = {
    "delayed_driver_record": "raw_parent_after_children.drivers",
    "invalid_driver_value": "raw_driver_invalid_values.drivers",
    "exact_duplicate_row": "raw_duplicate_rows.orders",
    "unchanged_order_resend": "raw_resent_unchanged.orders",
    "stale_order_version": "raw_stale_versions.orders",
    "missing_required_value": "raw_missing_required.orders",
    "invalid_status": "raw_invalid_status.orders",
    "status_needs_normalisation": "raw_status_not_canonical.orders",
    "delivery_before_pickup": "raw_delivery_before_pickup.orders",
    "unparseable_timestamp": "raw_unparseable_timestamps.orders",
    "invalid_numeric_range": "raw_numeric_out_of_range.orders",
    "delayed_order_record": "raw_parent_after_children.orders",
    "replayed_event_same_batch": "raw_duplicate_rows.delivery_events",
    "replayed_event_next_batch": "raw_replayed_from_earlier_batch.delivery_events",
    "missing_coordinates": "raw_gps_missing_coordinates.delivery_events",
    "out_of_range_coordinates": "raw_gps_invalid_coordinates.delivery_events",
    "impossible_gps_speed": "raw_gps_impossible_speed.delivery_events",
    "duplicate_timestamp": "raw_gps_duplicate_timestamps.delivery_events",
    "out_of_order_event": "raw_events_out_of_file_order.delivery_events",
    "gps_long_gap": "raw_gps_long_gaps.delivery_events",
    "late_event": "raw_late_events.delivery_events",
    "replayed_shift_event": "raw_duplicate_rows.shift_events",
    "orphan_event_missing_order": "raw_orphans.delivery_events",
    "orphan_shift_event_missing_driver": "raw_orphans.shift_events",
    "newer_order_update": "raw_version_updates.orders",
}


@dataclass(frozen=True)
class Explanation:
    """Why a check legitimately counts more rows than its defect type's manifest count."""

    reason: str
    also_counts: tuple[str, ...]  # defect types whose manifest counts make up the difference


# Checks allowed to count more than their own defect type: the difference must equal exactly
# the manifest counts of ``also_counts`` in the same batch, otherwise it is unexplained.
# Empty: on the default dataset every check counts exactly its defect type (the GPS-speed and
# out-of-file-order rules were tightened until they did; see docs/data_contracts.md, 11.2).
EXPLAINED_DIFFERENCES: dict[str, Explanation] = {}


@dataclass(frozen=True)
class CoverageRow:
    """One defect type in one batch."""

    defect_type: str
    check_name: str
    batch: int
    manifest: int
    observed: int
    expected: int
    reason: str = ""  # the explanation of an allowed difference, if any

    @property
    def difference(self) -> int:
        return self.observed - self.manifest

    @property
    def ok(self) -> bool:
        return self.observed == self.expected and (self.manifest == 0 or self.observed > 0)


def coverage_rows(
    counts: Mapping[str, Mapping[str, int]],
    observed: Mapping[int, Mapping[str, int]],
    batches: Sequence[int],
    explanations: Mapping[str, Explanation] | None = None,
) -> list[CoverageRow]:
    """Compare every case type with its check, per batch.

    ``counts``: manifest case counts per type and batch ("1", "2", ...); ``observed``: failing
    rows per batch sequence and stored check name; ``explanations``: allowed differences
    (default :data:`EXPLAINED_DIFFERENCES`).
    """
    explanations = EXPLAINED_DIFFERENCES if explanations is None else explanations
    rows: list[CoverageRow] = []
    for defect_type in ALL_CASE_TYPES:
        check_name = DEFECT_CHECKS[defect_type]
        explanation = explanations.get(check_name)
        for batch in batches:
            manifest = counts.get(defect_type, {}).get(str(batch), 0)
            extra = 0
            if explanation is not None:
                extra = sum(counts.get(t, {}).get(str(batch), 0) for t in explanation.also_counts)
            if check_name not in observed.get(batch, {}):
                raise KeyError(f"no load-time result for {check_name} in batch {batch}")
            rows.append(
                CoverageRow(
                    defect_type,
                    check_name,
                    batch,
                    manifest,
                    observed[batch][check_name],
                    manifest + extra,
                    "" if explanation is None else explanation.reason,
                )
            )
    return rows


def coverage_problems(rows: Sequence[CoverageRow]) -> list[str]:
    """Undetected types and unexplained differences."""
    problems: list[str] = []
    for row in rows:
        if row.ok:
            continue
        if row.manifest > 0 and row.observed == 0:
            problems.append(
                f"{row.defect_type} batch {row.batch}: not detected by {row.check_name}"
            )
        else:
            problems.append(
                f"{row.defect_type} batch {row.batch}: {row.check_name} observed {row.observed}, "
                f"manifest {row.manifest}, expected {row.expected} (unexplained difference)"
            )
    totals: dict[str, int] = {}
    for row in rows:
        totals[row.defect_type] = totals.get(row.defect_type, 0) + row.manifest
    for defect_type in INJECTED_DEFECT_TYPES:
        if totals.get(defect_type, 0) == 0:
            problems.append(f"{defect_type}: no manifest cases, so detection is not proven")
    return problems


def load_time_results(conn: Connection, dataset: str) -> dict[int, dict[str, int]]:
    """Failing rows per check of the successful load-time quality run of each batch."""
    succeeded = {
        str(row[0])
        for row in conn.execute(
            "SELECT DISTINCT batch_id FROM audit.pipeline_runs WHERE dataset = %s AND status = %s",
            (dataset, STATUS_SUCCEEDED),
        ).fetchall()
    }
    runs = conn.execute(
        "SELECT DISTINCT ON (batch_id) quality_run_id, batch_id, batch_sequence "
        "FROM audit.quality_runs WHERE dataset = %s AND triggered_by = 'load' "
        "AND status <> 'failed' ORDER BY batch_id, quality_run_id DESC",
        (dataset,),
    ).fetchall()
    observed: dict[int, dict[str, int]] = {}
    for run_id, batch_id, sequence in runs:
        if str(batch_id) not in succeeded:
            continue
        rows = conn.execute(
            "SELECT check_name, failing_rows FROM audit.quality_results WHERE quality_run_id = %s",
            (run_id,),
        ).fetchall()
        observed[int(sequence)] = {str(name): int(failing) for name, failing in rows}
    return observed


def latest_manual_run(conn: Connection, dataset: str) -> list[tuple[str, ...]]:
    """(check, severity, status, observed, threshold, failing/checked) of the latest manual run."""
    rows = conn.execute(
        "SELECT r.check_name, r.severity, r.status, r.observed_value, r.threshold, "
        "r.threshold_kind, r.failing_rows, r.rows_checked "
        "FROM audit.quality_results r "
        "WHERE r.quality_run_id = (SELECT max(quality_run_id) FROM audit.quality_runs "
        "WHERE dataset = %s AND triggered_by = 'manual') ORDER BY r.check_name",
        (dataset,),
    ).fetchall()
    table: list[tuple[str, ...]] = []
    for name, severity, status, observed, threshold, kind, failing, checked in rows:
        table.append(
            (
                f"`{name}`",
                str(severity),
                str(status),
                _value(observed, str(kind)),
                _value(threshold, str(kind)),
                f"{failing}/{checked}",
            )
        )
    return table


def _value(value: object, kind: str) -> str:
    if value is None:
        return "n/a"
    number = float(str(value))
    if kind == "share":
        return f"{number * 100:.2f}%"
    if kind == "hours":
        return f"{number:.2f} h"
    return f"{number:g}"


def _kind(defect_type: str) -> str:
    if defect_type in NATURAL_CASE_TYPES:
        return "natural"
    if defect_type in DERIVED_CASE_TYPES:
        return "derived"
    return "injected"


def _table(header: Sequence[str], rows: Sequence[Sequence[str]]) -> list[str]:
    lines = ["| " + " | ".join(header) + " |", "|" + "---|" * len(header)]
    lines += ["| " + " | ".join(row) + " |" for row in rows]
    return lines


def build_section(
    manifest: Manifest,
    rows: Sequence[CoverageRow],
    batches: Sequence[int],
    manual: Sequence[tuple[str, ...]],
) -> str:
    """The marked report section (deterministic for the same data)."""
    dispositions: dict[str, str] = {}
    reasons: dict[str, set[str]] = {}
    for case in manifest.cases:
        dispositions.setdefault(case.defect_type, case.expected_disposition)
        if case.reason:
            reasons.setdefault(case.defect_type, set()).add(case.reason)
    outcome = {
        defect_type: disposition
        + (
            " (" + ", ".join(f"`{r}`" for r in sorted(reasons[defect_type])) + ")"
            if defect_type in reasons
            else ""
        )
        for defect_type, disposition in dispositions.items()
    }
    by_type: dict[str, list[CoverageRow]] = {}
    for row in rows:
        by_type.setdefault(row.defect_type, []).append(row)
    detection_rows: list[list[str]] = []
    for defect_type in ALL_CASE_TYPES:
        group = by_type[defect_type]
        cells = [f"{r.observed} / {r.manifest}" for r in group]
        total_observed = sum(r.observed for r in group)
        total_manifest = sum(r.manifest for r in group)
        result = "detected" if all(r.ok for r in group) else "PROBLEM"
        if any(r.difference != 0 for r in group) and result == "detected":
            result = "detected (explained difference)"
        detection_rows.append(
            [
                f"`{defect_type}`",
                _kind(defect_type),
                outcome.get(defect_type, ""),
                f"`{group[0].check_name}`",
                *cells,
                f"{total_observed} / {total_manifest}",
                result,
            ]
        )
    difference_rows = [
        [
            f"`{r.check_name}`",
            f"batch-{r.batch}",
            str(r.observed),
            str(r.manifest),
            f"{r.difference:+d}",
            r.reason if r.reason and r.ok else "UNEXPLAINED",
        ]
        for r in rows
        if r.difference != 0
    ]
    batch_headers = [f"batch-{b} observed / manifest" for b in batches]
    lines = [
        QUALITY_START,
        "## Quality checks: defect detection",
        "",
        "> Generated by `make quality-report` from the load-time results in "
        "`audit.quality_results` and the defect manifest. Do not edit by hand.",
        "",
        f"Dataset `{manifest.dataset}`. Every case type is detected by a raw-layer check that "
        "re-derives the defect from the raw text, independently of the pipeline's "
        "dispositions. Observed = failing rows of that check at load time; manifest = cases "
        "of the type. The pipeline outcome is how the defect is then handled (quarantined as "
        "rejected, dropped as duplicate or stale, held as pending, or accepted under a "
        "documented rule).",
        "",
        *_table(
            [
                "Defect type",
                "Kind",
                "Pipeline outcome",
                "Detected by",
                *batch_headers,
                "Total",
                "Result",
            ],
            detection_rows,
        ),
        "",
        "### Differences between observed and manifest counts",
        "",
    ]
    if difference_rows:
        lines += [
            "A difference is accepted only when it equals exactly the manifest count of the "
            "defect types named in the reason, in the same batch.",
            "",
            *_table(
                ["Check", "Batch", "Observed", "Manifest", "Difference", "Reason"],
                difference_rows,
            ),
        ]
    else:
        lines.append("None: every check counted exactly its defect type's cases.")
    lines += ["", "### Latest `make quality` run", ""]
    if manual:
        lines += _table(
            ["Check", "Severity", "Status", "Observed", "Threshold", "Failing / checked"],
            [list(row) for row in manual],
        )
    else:
        lines.append("No manual run stored yet; run `make quality`.")
    lines += ["", QUALITY_END]
    return "\n".join(lines)


def main(argv: Sequence[str] | None = None) -> int:
    """Command-line entry point; returns the process exit code."""
    parser = argparse.ArgumentParser(description="Prove defect detection by the quality checks.")
    parser.add_argument("--raw-dir", type=Path, default=DEFAULT_RAW_DIR)
    parser.add_argument("--ground-truth-dir", type=Path, default=DEFAULT_GROUND_TRUTH_DIR)
    parser.add_argument("--dataset", default=None)
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    args = parser.parse_args(argv)
    configure_logging()
    raw_dir: Path = args.raw_dir
    ground_truth_dir: Path = args.ground_truth_dir
    report_path: Path = args.report
    try:
        dataset_dir = resolve_dataset(raw_dir, args.dataset)
        manifest_path = ground_truth_dir / dataset_dir.name / MANIFEST_FILE_NAME
        if not manifest_path.is_file():
            logger.error("Manifest %s not found.", manifest_path)
            return 1
        manifest = load_manifest(manifest_path)
        settings = load_database_settings("etl")
        with psycopg.connect(settings.conninfo(), autocommit=True) as conn:
            observed = load_time_results(conn, dataset_dir.name)
            manual = latest_manual_run(conn, dataset_dir.name)
    except (TransformError, ConfigError, psycopg.Error) as exc:
        logger.error("%s", exc)
        return 1
    batches = list(range(1, len(manifest.batch_ids) + 1))
    if sorted(observed) != batches:
        logger.error(
            "load-time quality results exist for batches %s of %s; run `make load` first.",
            sorted(observed),
            batches,
        )
        return 1
    rows = coverage_rows(case_counts(manifest.cases), observed, batches)
    problems = coverage_problems(rows)
    section = build_section(manifest, rows, batches, manual)
    text = report_path.read_text(encoding="utf-8") if report_path.is_file() else ""
    report_path.write_text(replace_section(text, section), encoding="utf-8", newline="\n")
    sys.stdout.write(section + "\n")
    if problems:
        for problem in problems:
            logger.error("coverage: %s", problem)
        logger.error("%d coverage problems; section written to %s", len(problems), report_path)
        return 1
    explained = sum(1 for row in rows if row.difference != 0)
    logger.info(
        "quality coverage passed: %d case types detected in %d batches "
        "(%d explained differences); section written to %s",
        len(ALL_CASE_TYPES),
        len(batches),
        explained,
        report_path,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
