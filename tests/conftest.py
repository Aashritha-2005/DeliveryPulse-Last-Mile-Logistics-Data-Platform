"""Suite-wide rules that keep every failure attributable to exactly one test suite.

Every test carries exactly one marker: ``unit``, ``integration``, ``s3``, ``spark``, ``kafka``
or ``mongo``:

* ``unit``: no services (`make test`);
* ``integration``: needs PostgreSQL (`make test-integration`);
* ``s3``: needs PostgreSQL and the local object store (`make test-s3`);
* ``spark``: needs a Java 21 runtime and the bench dependency group (`make test-spark`).
* ``kafka``: needs Kafka and PostgreSQL (`make test-kafka`).
* ``mongo``: needs MongoDB and PostgreSQL (`make test-mongo`).

A test with none would be silently skipped by every suite; a test with several would run in
several. Either stops the run at collection.
"""

import pytest

TEST_KINDS = ("unit", "integration", "s3", "spark", "kafka", "mongo")


def marker_problems(items: list[pytest.Item]) -> list[str]:
    """Node ids of tests that do not carry exactly one of the suite markers."""
    problems: list[str] = []
    for item in items:
        kinds = [kind for kind in TEST_KINDS if item.get_closest_marker(kind) is not None]
        if len(kinds) != 1:
            label = f"markers {', '.join(kinds)}" if kinds else "no suite marker"
            problems.append(f"{item.nodeid}: {label}")
    return problems


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    """Fail the run if any collected test is not in exactly one suite."""
    del config
    problems = marker_problems(items)
    if problems:
        raise pytest.UsageError(
            "every test must be marked exactly one of "
            "'unit', 'integration', 's3', 'spark', 'kafka' or 'mongo':\n  " + "\n  ".join(problems)
        )
