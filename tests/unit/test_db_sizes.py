"""Size totals and command output without a database connection."""

from unittest.mock import MagicMock, Mock

import psycopg
import pytest

from src.database import db_sizes
from src.database.db_counts import PROJECT_SCHEMAS

pytestmark = pytest.mark.unit


def test_schema_totals_include_empty_schemas_and_reconcile() -> None:
    rows = [
        db_sizes.TableSize("raw.a", 10, 20, 30, 60),
        db_sizes.TableSize("raw.b", 1, 2, 3, 6),
        db_sizes.TableSize("analytics.c", 4, 5, 6, 15),
    ]
    totals = db_sizes.schema_totals(rows)
    assert list(totals) == list(PROJECT_SCHEMAS)
    assert totals == {"raw": 66, "staging": 0, "analytics": 15, "audit": 0}
    assert sum(totals.values()) == sum(row.total_bytes for row in rows)
    assert db_sizes.schema_totals([]) == dict.fromkeys(PROJECT_SCHEMAS, 0)


@pytest.mark.parametrize("size,expected", [(0, "0.0"), (1048576, "1.0"), (1572864, "1.5")])
def test_mib_uses_binary_units(size: int, expected: str) -> None:
    assert db_sizes.mib(size) == expected


@pytest.mark.parametrize("fails", [False, True])
def test_command_reports_totals_or_failure(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], fails: bool
) -> None:
    conn = MagicMock()
    conn.__enter__.return_value = conn
    conn.execute.return_value.fetchall.return_value = [
        ("raw", "fixture", 1048576, 1048576, 0, 2097152)
    ]
    conn.execute.return_value.fetchone.return_value = ("3 MB",)
    if fails:
        conn.execute.side_effect = psycopg.OperationalError("connection unavailable")
    monkeypatch.setattr(db_sizes, "load_database_settings", Mock(return_value=Mock()))
    monkeypatch.setattr("src.database.db_sizes.psycopg.connect", Mock(return_value=conn))
    assert db_sizes.main() == int(fails)
    output = capsys.readouterr()
    if fails:
        assert "ERROR: connection unavailable" in output.err
    else:
        assert "raw.fixture" in output.out and "database size" in output.out
        assert "3 MB" in output.out
        line = next(line for line in output.out.splitlines() if line.startswith("raw.fixture"))
        assert line.split()[1:] == ["1.0", "1.0", "0.0", "2.0"]
    conn.__exit__.assert_called_once()
