"""The suite's attribution guards must actually work (each test fails if its guard is removed).

Both run pytest in a subprocess (pytester), so the outer session's own guards cannot mask a
missing one.
"""

from pathlib import Path

import pytest

from tests.unit.conftest import NO_DATABASE

pytest_plugins = ["pytester"]
pytestmark = pytest.mark.unit

PROJECT_ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture
def isolated(pytester: pytest.Pytester, monkeypatch: pytest.MonkeyPatch) -> pytest.Pytester:
    """A pytester whose subprocess can import the project's ``tests`` and ``src`` packages."""
    monkeypatch.setenv("PYTHONPATH", str(PROJECT_ROOT))
    pytester.makeini(
        "[pytest]\nmarkers =\n    unit: unit\n    integration: integration\n    s3: s3\n"
        "    spark: spark\n    kafka: kafka\n"
    )
    return pytester


def test_marker_guard_rejects_unmarked_and_double_marked_tests(isolated: pytest.Pytester) -> None:
    isolated.makeconftest("from tests.conftest import pytest_collection_modifyitems\n")
    isolated.makepyfile(
        test_sample="""
import pytest

@pytest.mark.unit
def test_marked():
    pass

def test_unmarked():
    pass

@pytest.mark.unit
@pytest.mark.integration
def test_both():
    pass

@pytest.mark.integration
@pytest.mark.s3
def test_database_and_store():
    pass
"""
    )
    result = isolated.runpytest_subprocess()
    output = result.stdout.str() + result.stderr.str()
    assert result.ret == pytest.ExitCode.USAGE_ERROR
    assert "'unit', 'integration', 's3', 'spark' or 'kafka'" in output
    assert "test_unmarked: no suite marker" in output
    assert "test_both: markers unit, integration" in output
    assert "test_database_and_store: markers integration, s3" in output


def test_marker_guard_accepts_correctly_marked_tests(isolated: pytest.Pytester) -> None:
    isolated.makeconftest("from tests.conftest import pytest_collection_modifyitems\n")
    isolated.makepyfile(
        test_sample="""
import pytest

@pytest.mark.unit
def test_unit():
    pass

@pytest.mark.integration
def test_integration():
    pass

@pytest.mark.s3
def test_object_store():
    pass

@pytest.mark.spark
def test_spark_engine():
    pass

@pytest.mark.kafka
def test_kafka_stream():
    pass
"""
    )
    isolated.runpytest_subprocess().assert_outcomes(passed=5)


def test_unit_guard_refuses_database_connections(isolated: pytest.Pytester) -> None:
    isolated.makeconftest("from tests.unit.conftest import no_database\n")
    isolated.makepyfile(
        test_sample="""
import psycopg

def test_connects():
    psycopg.connect("host=127.0.0.1 port=1 connect_timeout=1")

def test_connects_through_the_class():
    psycopg.Connection.connect("host=127.0.0.1 port=1 connect_timeout=1")
"""
    )
    result = isolated.runpytest_subprocess()
    result.assert_outcomes(failed=2)
    assert result.stdout.str().count(NO_DATABASE) >= 2
