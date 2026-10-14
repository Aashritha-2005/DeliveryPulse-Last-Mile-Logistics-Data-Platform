"""Integration tests of what the Airflow DAG relies on (test database, rolled back).

* ``--through-batch``: each scheduled run loads one more batch; reruns are no-ops.
* Rerunning a date: the tasks of one run (load, quality gate, aggregate refresh) executed twice
  leave the business tables and aggregates identical.
* The quality demo: planting the bad order makes the gate fail; removing it leaves no trace.
"""

from pathlib import Path

import psycopg
import pytest
from psycopg.rows import TupleRow

from src.database.load_postgres import LOADED, SKIPPED, LoadError, load_dataset
from src.database.metrics import rebuild_all
from src.quality.checks import FAILED, QualityConfig
from src.quality.demo import DEMO_ORDER_ID, DemoError, plant, remove
from src.quality.run import run_database_quality
from src.transformations.pipeline import TransformConfig
from tests.integration.test_load import BUSINESS_TABLES, all_tables, digests

pytestmark = pytest.mark.integration

Connection = psycopg.Connection[TupleRow]
CONFIG = TransformConfig()


def loaded_sequences(conn: Connection) -> list[int]:
    rows = conn.execute(
        "SELECT DISTINCT batch_sequence FROM audit.pipeline_runs WHERE status = 'succeeded' "
        "ORDER BY 1"
    ).fetchall()
    return [int(row[0]) for row in rows]


def run_date(conn: Connection, dataset: Path, through_batch: int) -> None:
    """The database-side tasks of one DAG run: load, quality gate, aggregate refresh."""
    load_dataset(conn, dataset, CONFIG, through_batch=through_batch)
    gate, _ = run_database_quality(conn, QualityConfig())
    assert gate.blocking_failures == []
    rebuild_all(conn)


# ----------------------------------------------------------------- --through-batch ---


def test_through_batch_loads_one_more_batch_per_run(
    test_db: Connection, fixture_dataset: Path
) -> None:
    first = load_dataset(test_db, fixture_dataset, CONFIG, through_batch=1)
    assert [(load.sequence, load.status) for load in first] == [(1, LOADED)]
    assert loaded_sequences(test_db) == [1]
    after_first = digests(test_db, BUSINESS_TABLES)

    again = load_dataset(test_db, fixture_dataset, CONFIG, through_batch=1)
    assert [load.status for load in again] == [SKIPPED]
    assert digests(test_db, BUSINESS_TABLES) == after_first

    rest = load_dataset(test_db, fixture_dataset, CONFIG, through_batch=3)
    assert [(load.sequence, load.status) for load in rest] == [
        (1, SKIPPED),
        (2, LOADED),
        (3, LOADED),
    ]
    assert loaded_sequences(test_db) == [1, 2, 3]


def test_through_batch_beyond_the_dataset_is_refused_before_writing(
    test_db: Connection, fixture_dataset: Path
) -> None:
    before = digests(test_db, all_tables(test_db))
    with pytest.raises(LoadError, match="outside 1..3"):
        load_dataset(test_db, fixture_dataset, CONFIG, through_batch=4)
    assert digests(test_db, all_tables(test_db)) == before


# ------------------------------------------------------------- rerunning a date ---


def test_rerunning_a_date_does_not_duplicate_data(
    test_db: Connection, fixture_dataset: Path
) -> None:
    run_date(test_db, fixture_dataset, through_batch=1)
    run_date(test_db, fixture_dataset, through_batch=2)
    after_run = digests(test_db, BUSINESS_TABLES)

    run_date(test_db, fixture_dataset, through_batch=2)  # the same date, cleared and rerun
    assert digests(test_db, BUSINESS_TABLES) == after_run
    assert loaded_sequences(test_db) == [1, 2]


# ---------------------------------------------------------------- quality demo ---


def test_quality_demo_fails_the_gate_and_removal_leaves_no_trace(
    test_db: Connection, fixture_dataset: Path
) -> None:
    load_dataset(test_db, fixture_dataset, CONFIG)
    clean_run, _ = run_database_quality(test_db, QualityConfig())
    assert clean_run.blocking_failures == []
    before = digests(test_db, BUSINESS_TABLES)

    assert plant(test_db) is True
    assert plant(test_db) is False  # idempotent
    blocked, _ = run_database_quality(test_db, QualityConfig())
    failed = {r.check_name: r for r in blocked.results if r.status == FAILED}
    assert "orders_positive_amounts" in failed
    assert DEMO_ORDER_ID in failed["orders_positive_amounts"].sample_keys

    assert remove(test_db) is True
    assert remove(test_db) is False  # idempotent
    assert digests(test_db, BUSINESS_TABLES) == before  # no trace in any business table
    again, _ = run_database_quality(test_db, QualityConfig())
    assert again.blocking_failures == [] and again.warnings == []


def test_quality_demo_refuses_an_empty_database(test_db: Connection) -> None:
    with pytest.raises(DemoError, match="run `make load` first"):
        plant(test_db)
