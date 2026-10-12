"""Suite-wide rules that keep failures attributable to unit vs integration tests.

Every test must carry exactly one of the markers ``unit`` or ``integration``. A test with
neither would be silently skipped by both `make test` (``-m unit``) and `make test-integration`
(``-m integration``); a test with both would run in both. Either stops the run at collection.
"""

import pytest

TEST_KINDS = ("unit", "integration")


def marker_problems(items: list[pytest.Item]) -> list[str]:
    """Node ids of tests that do not carry exactly one of the two markers."""
    problems: list[str] = []
    for item in items:
        kinds = [kind for kind in TEST_KINDS if item.get_closest_marker(kind) is not None]
        if len(kinds) != 1:
            label = "both markers" if kinds else "neither marker"
            problems.append(f"{item.nodeid}: {label}")
    return problems


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    """Fail the run if any collected test is not exactly ``unit`` or ``integration``."""
    del config
    problems = marker_problems(items)
    if problems:
        raise pytest.UsageError(
            "every test must be marked exactly one of 'unit' or 'integration':\n  "
            + "\n  ".join(problems)
        )
