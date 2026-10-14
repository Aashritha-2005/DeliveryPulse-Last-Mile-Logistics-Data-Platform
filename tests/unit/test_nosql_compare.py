"""Comparison fails on raw differences and measures equal projections without services."""

from typing import Any
from unittest.mock import MagicMock

import pytest

from src.nosql.compare import FIELDS, canonical_row, compare_order
from src.nosql.documents import event_document

pytestmark = pytest.mark.unit


def fixtures() -> tuple[Any, Any, dict[str, Any]]:
    """Two mocked drivers return the same raw row, including malformed text and NULL."""
    document = event_document('{"order_id":"o1","lat":"bad","lon":null}', "b", "f", 1)
    database = MagicMock()
    database.__getitem__.return_value.find.return_value.sort.return_value = [document]
    database.command.return_value = {"queryPlanner": {"winningPlan": {"stage": "IXSCAN"}}}
    connection = MagicMock()
    connection.execute.return_value.fetchall.return_value = [tuple(document[k] for k in FIELDS)]
    connection.execute.return_value.fetchone.return_value = ([{"Plan": {}}],)
    return database, connection, document


def test_equal_raw_rows_use_same_projection_and_read_only_transaction() -> None:
    database, connection, document = fixtures()
    result = compare_order(database, connection, "o1")
    assert result["equal"] and result["rows_sql"] == result["rows_mongo"] == 1
    assert len(result["mongo_seconds"]) == len(result["sql_seconds"]) == 5
    connection.execute.assert_any_call("SET TRANSACTION READ ONLY")
    projection = database.__getitem__.return_value.find.call_args.args[1]
    assert projection == {**dict.fromkeys(FIELDS, 1), "_id": 0}
    assert "raw_line" not in projection
    assert canonical_row(document) == canonical_row({**document, "ingested_at": "ignored"})


def test_mismatching_value_fails_instead_of_reporting_a_timing() -> None:
    database, connection, document = fixtures()
    database.__getitem__.return_value.find.return_value.sort.return_value = [
        {**document, "lat": "different"}
    ]
    with pytest.raises(ValueError, match="results differ"):
        compare_order(database, connection, "o1")


def test_missing_duplicate_is_detected() -> None:
    database, connection, document = fixtures()
    connection.execute.return_value.fetchall.return_value = [tuple(document[k] for k in FIELDS)] * 2
    with pytest.raises(ValueError, match="results differ"):
        compare_order(database, connection, "o1")
