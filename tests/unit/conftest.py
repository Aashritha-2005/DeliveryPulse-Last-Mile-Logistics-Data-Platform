"""Shared unit-test fixtures: small generated datasets in temporary folders (no services).

Unit tests never touch a database: an autouse fixture makes ``psycopg.connect`` fail loudly,
so a unit test that needs PostgreSQL fails with a clear message instead of passing or
failing depending on whether the database happens to be running.
"""

import importlib.util
from collections.abc import Iterator
from typing import NoReturn

import psycopg
import pytest

from src.ingestion.generator_config import GeneratorConfig
from tests.unit.generated import SMALL_ORDERS, SMALL_SEED, GeneratedDataset, generate_into

NO_DATABASE = "unit tests must not connect to a database"
NO_KAFKA = "unit tests must not create a Kafka client"


def _refuse_connection(*_: object, **__: object) -> NoReturn:
    raise AssertionError(NO_DATABASE)


@pytest.fixture(autouse=True)
def no_database(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Every unit test runs with database connections refused."""
    monkeypatch.setattr(psycopg, "connect", _refuse_connection)
    monkeypatch.setattr(psycopg.Connection, "connect", _refuse_connection)
    yield


@pytest.fixture(autouse=True)
def no_kafka(monkeypatch: pytest.MonkeyPatch) -> None:
    """Refuse live Kafka clients even when the optional group is installed locally."""
    if importlib.util.find_spec("confluent_kafka") is None:
        return
    import confluent_kafka

    def refuse(*args: object, **kwargs: object) -> NoReturn:
        raise AssertionError(NO_KAFKA)

    monkeypatch.setattr(confluent_kafka, "Producer", refuse)
    monkeypatch.setattr(confluent_kafka, "Consumer", refuse)


@pytest.fixture(scope="session")
def small_config() -> GeneratorConfig:
    return GeneratorConfig.create(orders=SMALL_ORDERS, seed=SMALL_SEED)


@pytest.fixture(scope="session")
def dataset_a(
    tmp_path_factory: pytest.TempPathFactory, small_config: GeneratorConfig
) -> GeneratedDataset:
    return generate_into(tmp_path_factory.mktemp("generated_a"), small_config)


@pytest.fixture(scope="session")
def dataset_b(
    tmp_path_factory: pytest.TempPathFactory, small_config: GeneratorConfig
) -> GeneratedDataset:
    return generate_into(tmp_path_factory.mktemp("generated_b"), small_config)
