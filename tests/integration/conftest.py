"""Fixtures for integration tests: connections to the local PostgreSQL as each application role.

Nothing here reads .env or connects at import time; connections open lazily inside the
fixtures, so collecting this package (for example during `make test`) needs no database.
If PostgreSQL is unreachable the tests FAIL with a hint rather than being skipped, so a
stopped database can never produce a green run. Tests never connect as the superuser.

Load tests use the separate `<POSTGRES_DB>_test` database, and each runs inside an outer
transaction that is always rolled back (``test_db``), so nothing accumulates between runs.
"""

from collections.abc import Iterator
from pathlib import Path

import psycopg
import pytest
from psycopg.rows import TupleRow

from src.config import ConfigError, DatabaseRole, load_database_settings
from src.ingestion.generator_config import GeneratorConfig
from tests.unit.generated import SMALL_ORDERS, SMALL_SEED, generate_into


def _connect(role: DatabaseRole, test_database: bool = False) -> psycopg.Connection[TupleRow]:
    """Open an autocommit connection as ``role``, failing the test with a clear hint."""
    try:
        settings = load_database_settings(role, test_database=test_database)
    except ConfigError as exc:
        pytest.fail(f"Database settings are incomplete: {exc}", pytrace=False)
    try:
        return psycopg.connect(settings.conninfo(), autocommit=True)
    except psycopg.OperationalError as exc:
        pytest.fail(
            f"Cannot connect to PostgreSQL as {settings.user} at "
            f"{settings.host}:{settings.port}/{settings.dbname}. "
            f"Is it running? Start it with `make up`. Driver error: {exc}",
            pytrace=False,
        )


@pytest.fixture(scope="session")
def readonly_conn() -> Iterator[psycopg.Connection[TupleRow]]:
    """Autocommit connection as deliverypulse_readonly."""
    conn = _connect("readonly")
    yield conn
    conn.close()


@pytest.fixture(scope="session")
def etl_conn() -> Iterator[psycopg.Connection[TupleRow]]:
    """Autocommit connection as deliverypulse_etl."""
    conn = _connect("etl")
    yield conn
    conn.close()


@pytest.fixture(scope="session")
def etl_test_conn() -> Iterator[psycopg.Connection[TupleRow]]:
    """Autocommit connection as deliverypulse_etl to the `<POSTGRES_DB>_test` database."""
    conn = _connect("etl", test_database=True)
    yield conn
    conn.close()


@pytest.fixture
def test_db(
    etl_test_conn: psycopg.Connection[TupleRow],
) -> Iterator[psycopg.Connection[TupleRow]]:
    """The test-database connection inside an outer transaction that is always rolled back."""
    with etl_test_conn.transaction(force_rollback=True):
        yield etl_test_conn


@pytest.fixture(scope="session")
def fixture_dataset(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """The 1,500-order fixture dataset (raw folder), generated once per session."""
    config = GeneratorConfig.create(orders=SMALL_ORDERS, seed=SMALL_SEED)
    generated = generate_into(tmp_path_factory.mktemp("load_fixture"), config)
    return generated.dataset_dir
