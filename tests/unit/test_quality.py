"""Unit tests (no database) of the data-quality framework and the defect-coverage logic."""

import pytest

from src.contract import REASON_CODES, SOURCE_FILES
from src.groundtruth.quality_coverage import (
    ALL_CASE_TYPES,
    DEFECT_CHECKS,
    EXPLAINED_DIFFERENCES,
    Explanation,
    coverage_problems,
    coverage_rows,
)
from src.groundtruth.report_sections import (
    QUALITY_END,
    QUALITY_START,
    extract_section,
    keep_section,
    replace_section,
)
from src.ingestion.generator_config import INJECTED_DEFECT_TYPES
from src.quality.checks import (
    BLOCKING,
    CHECKS,
    CHECKS_BY_NAME,
    FAILED,
    KIND_COUNT,
    KIND_HOURS,
    KIND_SHARE,
    LAYER_RAW,
    PASSED,
    WARNED,
    WARNING,
    CheckResult,
    QualityConfig,
    QualityConfigError,
    QualityGateError,
    QualityRun,
    evaluate_result,
    evaluate_status,
    format_value,
    parse_thresholds,
    result_names,
    result_table,
    slowest_checks,
)

pytestmark = pytest.mark.unit

SOURCES = {source for source, _ in SOURCE_FILES}


def result(name: str, status: str, severity: str = WARNING) -> CheckResult:
    return CheckResult(name, LAYER_RAW, "t", severity, status, 1.0, 0.0, KIND_COUNT, 1, 2, ())


# ------------------------------------------------------------------ status rules ---


@pytest.mark.parametrize(
    ("severity", "observed", "threshold", "expected"),
    [
        (BLOCKING, 0.0, 0.0, PASSED),
        (BLOCKING, 1.0, 0.0, FAILED),
        (WARNING, 1.0, 0.0, WARNED),
        (WARNING, 0.02, 0.02, PASSED),  # at the threshold still passes
        (WARNING, 0.021, 0.02, WARNED),
        (BLOCKING, None, 0.0, PASSED),  # no data to measure
    ],
)
def test_evaluate_status(
    severity: str, observed: float | None, threshold: float, expected: str
) -> None:
    assert evaluate_status(severity, observed, threshold) == expected


@pytest.mark.parametrize(
    ("kind", "rows_checked", "expected"),
    [
        (KIND_SHARE, 49, PASSED),  # below the minimum sample: the share is not judged
        (KIND_SHARE, 50, WARNED),  # at the minimum: judged
        (KIND_SHARE, 0, PASSED),
        (KIND_COUNT, 1, WARNED),  # counts are always judged
    ],
)
def test_share_threshold_needs_a_minimum_sample(
    kind: str, rows_checked: int, expected: str
) -> None:
    assert evaluate_result(kind, WARNING, 1.0, 0.02, rows_checked, 50) == expected


def test_minimum_sample_default_and_override() -> None:
    assert QualityConfig().share_min_rows() == 50
    overridden = QualityConfig(thresholds=parse_thresholds(["min_share_rows=10"]))
    assert overridden.share_min_rows() == 10
    with pytest.raises(QualityConfigError):
        parse_thresholds(["min_share_rows=2.5"])


def test_run_status_and_gate_message() -> None:
    run = QualityRun("ds", "load", [result("a", PASSED)], 1500)
    assert run.status == PASSED
    run.results.append(result("b", WARNED))
    assert run.status == WARNED
    run.results.append(result("c", FAILED, BLOCKING))
    assert run.status == FAILED
    assert run.summary() == "3 checks, 1 blocking failures, 1 warnings, 1.50 s"
    assert str(QualityGateError(run)) == "quality gate: c"


def test_slowest_checks_groups_per_source_results() -> None:
    def timed(name: str, ms: int) -> CheckResult:
        return CheckResult(
            name, LAYER_RAW, "t", WARNING, PASSED, 0.0, 0.0, KIND_COUNT, 0, 1, (), duration_ms=ms
        )

    results = [timed("a.orders", 900), timed("a.drivers", 900), timed("b", 2500), timed("c", 10)]
    run = QualityRun("ds", "load", results, 3410)
    assert slowest_checks(run, 2) == "b 2.50 s, a 0.90 s"


def test_format_value_and_table() -> None:
    assert format_value(0.0123, KIND_SHARE) == "1.23%"
    assert format_value(2.5, KIND_HOURS) == "2.50 h"
    assert format_value(3.0, KIND_COUNT) == "3"
    assert format_value(None, KIND_COUNT) == "n/a"
    assert result_table([result("a", PASSED)])[1].startswith("a | warning | passed | 1 | 0 |")


# -------------------------------------------------------------------- thresholds ---


def test_thresholds_default_override_by_check_and_by_instance() -> None:
    duplicate_rows = CHECKS_BY_NAME["raw_duplicate_rows"]
    config = QualityConfig()
    assert config.threshold(duplicate_rows, "orders") == 0.02
    assert config.threshold(CHECKS_BY_NAME["freshness_lag"], "") == 6.0
    assert config.threshold(CHECKS_BY_NAME["raw_version_updates"], "orders") == 0.05
    overridden = QualityConfig(
        thresholds=parse_thresholds(
            ["raw_duplicate_rows=0.1", "raw_duplicate_rows.orders=0.3", "freshness_lag=12"]
        )
    )
    assert overridden.threshold(duplicate_rows, "orders") == 0.3
    assert overridden.threshold(duplicate_rows, "drivers") == 0.1
    assert overridden.threshold(CHECKS_BY_NAME["freshness_lag"], "") == 12.0


@pytest.mark.parametrize(
    "item", ["nope=1", "raw_duplicate_rows", "freshness_lag=x", "known_cities=-1"]
)
def test_invalid_threshold_overrides_are_rejected(item: str) -> None:
    with pytest.raises(QualityConfigError):
        parse_thresholds([item])


# ---------------------------------------------------------------------- registry ---


def test_registry_is_well_formed() -> None:
    names = [check.name for check in CHECKS]
    assert len(names) == len(set(names))
    assert len(result_names()) == len(set(result_names()))
    for check in CHECKS:
        assert check.severity in (BLOCKING, WARNING), check.name
        assert check.kind in (KIND_COUNT, KIND_SHARE, KIND_HOURS), check.name
        assert check.description and check.table, check.name
        assert set(check.instances) <= SOURCES | {""}, check.name
        if check.layer == LAYER_RAW:
            # raw defects are expected: detection never blocks a batch
            assert check.severity == WARNING, check.name
        if check.severity == BLOCKING:
            assert check.default_threshold == 0, check.name
        assert not check.query.rstrip().endswith(";"), check.name


def test_requirement_areas_are_covered() -> None:
    names = set(CHECKS_BY_NAME)
    assert {"raw_missing_required", "orders_required_by_status"} <= names  # nulls
    assert {"raw_invalid_status", "disposition_values", "known_cities"} <= names  # values
    assert {"raw_numeric_out_of_range", "orders_positive_amounts"} <= names  # ranges
    assert {"raw_duplicate_rows", "gps_ping_unique"} <= names  # uniqueness
    assert {"accepted_loaded", "pending_parent_missing", "raw_orphans"} <= names  # RI
    assert {"freshness_lag", "no_future_timestamps"} <= names  # freshness
    assert {"reconciliation_identity", "rejected_quarantined", "metrics_match_facts"} <= names
    assert "utilisation_above_100" in names


def test_reason_codes_include_every_contract_reason() -> None:
    assert REASON_CODES[0] == ""
    assert len(REASON_CODES) == len(set(REASON_CODES))


# --------------------------------------------------------------- defect coverage ---


def test_every_case_type_maps_to_a_registered_raw_check() -> None:
    assert set(DEFECT_CHECKS) == set(ALL_CASE_TYPES)
    registered = set(result_names())
    for defect_type, check_name in DEFECT_CHECKS.items():
        assert check_name in registered, defect_type
        assert CHECKS_BY_NAME[check_name.split(".")[0]].layer == LAYER_RAW, defect_type
    for check_name, explanation in EXPLAINED_DIFFERENCES.items():
        assert check_name in registered
        assert set(explanation.also_counts) <= set(ALL_CASE_TYPES)
        assert explanation.reason


def _counts(value: int) -> dict[str, dict[str, int]]:
    return {t: {"1": value} for t in ALL_CASE_TYPES}


def _observed(value: int) -> dict[int, dict[str, int]]:
    return {1: {name: value for name in DEFECT_CHECKS.values()}}


# A test-only explanation: out-of-file-order may also count duplicate_timestamp cases.
OUT_OF_ORDER = "raw_events_out_of_file_order.delivery_events"
EXPLAINED = {OUT_OF_ORDER: Explanation("test reason", ("duplicate_timestamp",))}


def test_coverage_passes_when_every_count_matches_exactly() -> None:
    assert coverage_problems(coverage_rows(_counts(2), _observed(2), [1])) == []


def test_coverage_passes_when_a_difference_is_explained() -> None:
    observed = _observed(2)
    observed[1][OUT_OF_ORDER] = 4  # 2 + 2 duplicate_timestamp
    rows = coverage_rows(_counts(2), observed, [1], EXPLAINED)
    assert coverage_problems(rows) == []
    [row] = [r for r in rows if r.check_name == OUT_OF_ORDER]
    assert (row.difference, row.reason) == (2, "test reason")


def test_coverage_fails_on_unexplained_difference_and_on_missed_detection() -> None:
    observed = _observed(2)
    observed[1]["raw_duplicate_rows.orders"] = 3  # one more than the manifest, no explanation
    observed[1]["raw_invalid_status.orders"] = 0  # missed
    problems = coverage_problems(coverage_rows(_counts(2), observed, [1]))
    assert any("exact_duplicate_row" in p and "unexplained" in p for p in problems)
    assert any("invalid_status" in p and "not detected" in p for p in problems)
    assert len(problems) == 2


def test_coverage_fails_when_an_explained_difference_is_off_by_one() -> None:
    observed = _observed(2)
    observed[1][OUT_OF_ORDER] = 5
    problems = coverage_problems(coverage_rows(_counts(2), observed, [1], EXPLAINED))
    assert len(problems) == 1 and "out_of_order_event" in problems[0]


def test_coverage_requires_cases_for_every_injected_type() -> None:
    counts = _counts(1)
    counts[INJECTED_DEFECT_TYPES[0]] = {}
    observed = _observed(1)
    observed[1][DEFECT_CHECKS[INJECTED_DEFECT_TYPES[0]]] = 0
    problems = coverage_problems(coverage_rows(counts, observed, [1]))
    expected = f"{INJECTED_DEFECT_TYPES[0]}: no manifest cases, so detection is not proven"
    assert problems == [expected]


# ---------------------------------------------------------------- report section ---


def test_report_section_is_appended_replaced_and_kept() -> None:
    section = f"{QUALITY_START}\nA\n{QUALITY_END}"
    text = replace_section("# Report\n", section)
    assert text == f"# Report\n\n{section}\n"
    assert extract_section(text) == section
    newer = f"{QUALITY_START}\nB\n{QUALITY_END}"
    assert extract_section(replace_section(text, newer)) == newer
    regenerated = keep_section("# Report v2\n", text)
    assert regenerated.startswith("# Report v2\n") and extract_section(regenerated) == section
    assert keep_section("# Report v2\n", "# no section\n") == "# Report v2\n"
