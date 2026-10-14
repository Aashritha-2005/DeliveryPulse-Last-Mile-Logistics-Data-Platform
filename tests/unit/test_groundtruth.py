"""Unit tests for the defect manifest: it matches the files, isolation and contract alignment."""

import ast
import re
from collections import Counter
from dataclasses import replace
from pathlib import Path

import pytest

from src.groundtruth.manifest import ACCEPTED, DISPOSITIONS
from src.groundtruth.profile import build_report
from src.groundtruth.verify import (
    PER_CASE_ONLY_TYPES,
    expected_reconciliation,
    independent_counts,
    load_dataset,
    verify,
)
from src.ingestion.generator_config import (
    DELIVERY_EVENT_TYPES,
    DERIVED_CASE_TYPES,
    INJECTED_DEFECT_TYPES,
    ORDER_STATUSES,
    RATING_MAX,
    RATING_MIN,
    SHIFT_EVENT_TYPES,
    VEHICLE_TYPES,
)
from tests.unit.generated import GeneratedDataset

pytestmark = pytest.mark.unit

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SCHEMA_SQL = PROJECT_ROOT / "sql" / "schema.sql"

# Modules allowed to reference the ground truth (besides src/groundtruth/ and tests/).
# src/bench/shards.py only passes a destination directory to the generator, which always writes
# a manifest; it deletes that directory with the shard's raw files and never reads the manifest
# (test_shard_builder_discards_the_generators_ground_truth below).
GROUND_TRUTH_ALLOWLIST = frozenset(
    {
        "src/ingestion/generate_data.py",
        "src/ingestion/simulation.py",
        "src/ingestion/defects.py",
        "src/ingestion/writers.py",
        "src/ingestion/generator_config.py",
        "src/bench/shards.py",
    }
)


# ------------------------------------------------------- manifest vs files ---


def test_manifest_matches_the_files(dataset_a: GeneratedDataset) -> None:
    assert verify(dataset_a.dataset_dir, dataset_a.manifest()) == []


def test_every_defect_type_is_present_with_the_configured_count(
    dataset_a: GeneratedDataset,
) -> None:
    counts = Counter(case.defect_type for case in dataset_a.manifest().cases)
    for defect_type in INJECTED_DEFECT_TYPES:
        assert counts[defect_type] == dataset_a.config.defect_count(defect_type), defect_type
    for defect_type in DERIVED_CASE_TYPES:
        assert counts[defect_type] > 0, defect_type


def test_independent_counts_equal_manifest_counts(dataset_a: GeneratedDataset) -> None:
    manifest_counts = Counter(case.defect_type for case in dataset_a.manifest().cases)
    found = independent_counts(load_dataset(dataset_a.dataset_dir))
    checked = sorted((set(manifest_counts) | set(found)) - set(PER_CASE_ONLY_TYPES))
    assert checked
    assert {t: found[t] for t in checked} == {t: manifest_counts[t] for t in checked}


def test_no_record_carries_two_cases(dataset_a: GeneratedDataset) -> None:
    keys = Counter(
        (case.source, case.batch, case.record_key) for case in dataset_a.manifest().cases
    )
    assert [key for key, count in keys.items() if count > 1] == []


def test_injected_scenarios_use_distinct_orders(dataset_a: GeneratedDataset) -> None:
    manifest = dataset_a.manifest()
    owner: dict[str, str] = {}
    for case in manifest.cases:
        if case.source == "orders":
            assert owner.setdefault(case.record_key, case.scenario) == case.scenario


def test_reconciliation_balances_for_every_source_and_batch(dataset_a: GeneratedDataset) -> None:
    batches = load_dataset(dataset_a.dataset_dir)
    table = expected_reconciliation(batches, dataset_a.manifest().cases)
    assert len(table) == 4 * len(batches)
    for key, counts in table.items():
        assert counts[ACCEPTED] >= 0, key
        assert counts["input"] == sum(counts[d] for d in DISPOSITIONS), key


def test_verification_detects_a_missing_case(dataset_a: GeneratedDataset) -> None:
    manifest = dataset_a.manifest()
    removed = next(case for case in manifest.cases if case.defect_type == "invalid_status")
    trimmed = replace(manifest, cases=tuple(c for c in manifest.cases if c != removed))
    problems = verify(dataset_a.dataset_dir, trimmed)
    assert any("invalid_status" in problem for problem in problems)


def test_verification_detects_a_wrong_record_key(dataset_a: GeneratedDataset) -> None:
    manifest = dataset_a.manifest()
    original = next(case for case in manifest.cases if case.defect_type == "missing_coordinates")
    wrong = replace(original, record_key="EVT-999999999")
    cases = tuple(wrong if case == original else case for case in manifest.cases)
    problems = verify(dataset_a.dataset_dir, replace(manifest, cases=cases))
    assert any("missing_coordinates not found" in problem for problem in problems)


def test_profile_report_is_deterministic_and_balanced(dataset_a: GeneratedDataset) -> None:
    batches = load_dataset(dataset_a.dataset_dir)
    first = build_report(dataset_a.manifest(), batches)
    second = build_report(dataset_a.manifest(), batches)
    assert first == second
    assert dataset_a.config.dataset_name() in first
    assert "| NO |" not in first


# --------------------------------------------------------- contract vs SQL ---


def _check_values(constraint: str) -> list[str]:
    text = SCHEMA_SQL.read_text(encoding="utf-8")
    match = re.search(rf"CONSTRAINT {constraint}\s+CHECK \(\w+ IN \(([^)]*)\)\)", text)
    assert match is not None, constraint
    return sorted(re.findall(r"'([^']*)'", match.group(1)))


@pytest.mark.parametrize(
    ("constraint", "contract_values"),
    [
        ("orders_status_valid", ORDER_STATUSES),
        ("delivery_events_type_valid", DELIVERY_EVENT_TYPES),
        ("driver_shift_events_type_valid", SHIFT_EVENT_TYPES),
        ("drivers_vehicle_type_valid", VEHICLE_TYPES),
    ],
)
def test_contract_values_equal_schema_check_lists(
    constraint: str, contract_values: tuple[str, ...]
) -> None:
    assert _check_values(constraint) == sorted(contract_values)


def test_contract_rating_range_equals_schema() -> None:
    text = SCHEMA_SQL.read_text(encoding="utf-8")
    match = re.search(r"drivers_rating_range CHECK \(rating BETWEEN (\d+) AND (\d+)\)", text)
    assert match is not None
    assert (float(match.group(1)), float(match.group(2))) == (RATING_MIN, RATING_MAX)


# ---------------------------------------------------- ground-truth isolation ---


def references_ground_truth(source: str) -> bool:
    """True if Python ``source`` imports src.groundtruth or mentions ground truth paths."""
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Import):
            names = [alias.name for alias in node.names]
        elif isinstance(node, ast.ImportFrom):
            module = node.module or ""
            names = [module, *(f"{module}.{alias.name}" for alias in node.names)]
        elif isinstance(node, ast.Constant) and isinstance(node.value, str):
            names = [node.value]
        else:
            continue
        if any("groundtruth" in name or "ground_truth" in name for name in names):
            return True
    return False


@pytest.mark.parametrize(
    ("source", "expected"),
    [
        ("import src.groundtruth.manifest", True),
        ("from src.groundtruth import manifest", True),
        ("from src import groundtruth", True),
        ("PATH = 'data/ground_truth/x'", True),
        ("import json\nfrom src.config import load_database_settings", False),
    ],
)
def test_isolation_guard_detects_references(source: str, expected: bool) -> None:
    assert references_ground_truth(source) is expected


def test_only_allowlisted_modules_reference_the_ground_truth() -> None:
    offenders = []
    for path in sorted((PROJECT_ROOT / "src").rglob("*.py")):
        relative = path.relative_to(PROJECT_ROOT).as_posix()
        if relative in GROUND_TRUTH_ALLOWLIST or relative.startswith("src/groundtruth/"):
            continue
        if references_ground_truth(path.read_text(encoding="utf-8")):
            offenders.append(relative)
    assert offenders == []
