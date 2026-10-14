"""Command results, credential safety and restricted connection selection."""

import json
import sys
from types import ModuleType
from typing import Any
from unittest.mock import MagicMock, Mock

import pytest

from src.bench.disk import DiskGuardError
from src.nosql import client, compare, load_mongo, stats
from src.nosql.config import MongoRole
from src.nosql.documents import SourceChangedError

pytestmark = pytest.mark.unit


@pytest.mark.parametrize("role", ["app", "readonly"])
@pytest.mark.parametrize("test_database", [False, True])
def test_connection_selects_only_requested_identity(
    monkeypatch: pytest.MonkeyPatch, role: MongoRole, test_database: bool
) -> None:
    values = {"MONGO_DB": "fixture", "MONGO_PORT": "27017"}
    for name, letter in [("ROOT", "r"), ("APP", "a"), ("READONLY", "q")]:
        values[f"MONGO_{name}_USER"] = name.lower()
        values[f"MONGO_{name}_PASSWORD"] = letter * 24
    monkeypatch.setattr(client, "read_environment", lambda: values)
    fake = MagicMock()
    constructor = Mock(return_value=fake)
    module = ModuleType("pymongo")
    monkeypatch.setattr(module, "MongoClient", constructor, raising=False)
    monkeypatch.setitem(sys.modules, "pymongo", module)
    actual, database = client.connect(role, test_database=test_database)
    name = "fixture_test" if test_database else "fixture"
    assert actual is fake and database is fake[name]
    kwargs = constructor.call_args.kwargs
    assert kwargs["username"] == role
    assert kwargs["password"] == values[f"MONGO_{role.upper()}_PASSWORD"]
    assert kwargs["authSource"] == name and kwargs["host"] == "127.0.0.1"
    assert kwargs["serverSelectionTimeoutMS"] == 5000 and kwargs["maxPoolSize"] == 2
    assert kwargs["retryWrites"] is False


@pytest.fixture
def commands(monkeypatch: pytest.MonkeyPatch) -> Any:
    """Intercept service boundaries and arguments without reading environment files."""
    connection, database = MagicMock(), MagicMock()
    connection.connectors = {}
    monkeypatch.setattr(sys, "argv", ["command"])
    for module in (stats, compare, load_mongo):
        connector = Mock(return_value=(connection, database))
        connection.connectors[module.__name__] = connector
        monkeypatch.setattr(module, "connect", connector)
    monkeypatch.setattr(load_mongo, "resolve_dataset", Mock(return_value="fixture"))
    monkeypatch.setattr(compare, "load_database_settings", Mock(return_value=Mock()))
    monkeypatch.setattr("src.nosql.compare.psycopg.connect", MagicMock())
    return connection, database


@pytest.mark.parametrize("failure", [None, "driver", "source", "disk"])
def test_loader_exit_status_and_sanitized_errors(
    commands: Any,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    failure: str | None,
) -> None:
    connection, _ = commands
    result = {"rows": 3, "inserted": 0, "duplicate": 3, "modified": 0}
    loader = Mock(return_value=result)
    detail = "private-driver-detail"
    if failure == "driver":
        loader.side_effect = RuntimeError(detail)
    elif failure == "source":
        loader.side_effect = SourceChangedError("Immutable source changed")
    elif failure == "disk":
        loader.side_effect = DiskGuardError("insufficient free space")
    monkeypatch.setattr(load_mongo, "load_dataset", loader)
    assert load_mongo.main() == (0 if failure is None else 1)
    output = capsys.readouterr().out
    if failure is None:
        assert json.loads(output) == result
    else:
        assert output.startswith("ERROR:") and detail not in output
    connector = connection.connectors[load_mongo.__name__]
    connector.assert_called_once_with("app")
    connection.__exit__.assert_called_once()


@pytest.mark.parametrize("failure", [False, True])
def test_stats_filters_output_and_closes_client(
    commands: Any, capsys: pytest.CaptureFixture[str], failure: bool
) -> None:
    connection, database = commands
    database.command.return_value = {"count": 7, "storageSize": 4096, "private": "hidden"}
    if failure:
        database.command.side_effect = RuntimeError("private-driver-detail")
    assert stats.main() == int(failure)
    output = capsys.readouterr().out
    assert "private" not in output and "hidden" not in output
    if not failure:
        data = json.loads(output)
        assert data["count"] == 7 and data["storageSize"] == 4096
        assert set(data) == {
            "count",
            "size",
            "storageSize",
            "totalIndexSize",
            "indexSizes",
            "nindexes",
        }
    connector = connection.connectors[stats.__name__]
    connector.assert_called_once_with("readonly")
    database.command.assert_called_once_with("collStats", "raw_events")
    connection.__exit__.assert_called_once()


@pytest.mark.parametrize("mode", ["explicit", "selected", "no_order", "empty", "driver"])
def test_comparison_requires_nonempty_results_and_hides_driver_errors(
    commands: Any, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], mode: str
) -> None:
    connection, database = commands
    if mode == "explicit":
        monkeypatch.setattr(sys, "argv", ["command", "--order-id", "chosen"])
    database.raw_events.find_one.return_value = (
        None if mode == "no_order" else {"order_id": "chosen"}
    )
    result = {"rows_mongo": 0 if mode == "empty" else 8, "rows_sql": 8, "equal": True}
    query = Mock(return_value=result)
    if mode == "driver":
        query.side_effect = RuntimeError("private-driver-detail")
    monkeypatch.setattr(compare, "compare_order", query)
    assert compare.main() == (0 if mode in ("explicit", "selected") else 1)
    output = capsys.readouterr().out
    assert "private-driver-detail" not in output
    if mode in ("explicit", "selected"):
        assert json.loads(output) == result
        assert query.call_args.args[2] == "chosen"
    if mode == "explicit":
        database.raw_events.find_one.assert_not_called()
    if mode == "no_order":
        query.assert_not_called()
    connector = connection.connectors[compare.__name__]
    connector.assert_called_once_with("readonly")
    connection.__exit__.assert_called_once()
