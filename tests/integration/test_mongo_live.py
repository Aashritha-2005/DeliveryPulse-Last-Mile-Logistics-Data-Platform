"""Live authenticated storage tests; append-only fixtures remain in the test database."""

import json
import uuid
from collections.abc import Iterator
from typing import Any

import psycopg
import pytest
from psycopg import sql

from src.nosql.client import connect
from src.nosql.compare import FIELDS, compare_order
from src.nosql.documents import SourceChangedError, event_document
from src.nosql.load_mongo import insert_chunk

pytestmark = pytest.mark.mongo


@pytest.fixture
def mongo() -> Iterator[tuple[Any, Any]]:
    app_client, app = connect("app", test_database=True)
    query_client, query = connect("readonly", test_database=True)
    with app_client, query_client:
        app.command("ping")
        query.command("ping")
        yield app, query


def test_live_replay_and_changed_content(mongo: tuple[Any, Any]) -> None:
    app, query = mongo
    batch = "test-" + uuid.uuid4().hex
    rows = [
        event_document(raw, batch, "fixture.jsonl", i)
        for i, raw in enumerate(
            ['{"event_id":"duplicate"}', '{"event_id":"duplicate"}', "{malformed", "{}"], 1
        )
    ]
    collection = app.raw_events
    assert insert_chunk(collection, rows) == (4, 0)
    before = list(query.raw_events.find({"batch_id": batch}).sort("source_row_number", 1))
    assert len(before) == 4
    assert insert_chunk(collection, rows) == (0, 4)
    assert list(query.raw_events.find({"batch_id": batch}).sort("source_row_number", 1)) == before
    with pytest.raises(SourceChangedError):
        insert_chunk(collection, [{**rows[0], "event_id": "changed"}])
    assert query.raw_events.find_one({"_id": rows[0]["_id"]}) == before[0]


def test_live_roles_allow_reads_and_deny_mutation(mongo: tuple[Any, Any]) -> None:
    from pymongo.errors import OperationFailure

    app, query = mongo
    # Failed operations target a unique absent identity/collection, never real data.
    missing = "permission_probe_" + uuid.uuid4().hex
    for database, command in [
        (query, {"insert": "raw_events", "documents": [{"_id": missing}]}),
        (
            app,
            {"update": "raw_events", "updates": [{"q": {"_id": missing}, "u": {"$set": {"x": 1}}}]},
        ),
        (app, {"delete": "raw_events", "deletes": [{"q": {"_id": missing}, "limit": 1}]}),
        (app, {"drop": missing}),
        (app, {"createRole": missing, "privileges": [], "roles": []}),
        (query, {"drop": missing}),
    ]:
        with pytest.raises(OperationFailure) as exc:
            database.command(command)
        assert exc.value.code == 13
    for database in (app, query):
        assert isinstance(database.raw_events.count_documents({}), int)
        status = database.command({"connectionStatus": 1, "showPrivileges": True})
        actions = {
            action
            for privilege in status["authInfo"]["authenticatedUserPrivileges"]
            for action in privilege["actions"]
        }
        assert "dropDatabase" not in actions
        assert "createUser" not in actions


def test_live_indexes_and_query_plan(mongo: tuple[Any, Any]) -> None:
    app, query = mongo
    order = "plan-" + uuid.uuid4().hex
    insert_chunk(
        app.raw_events, [event_document(json.dumps({"order_id": order}), order, "probe", 1)]
    )
    indexes = {
        index["name"]: list(index["key"].items()) for index in query.raw_events.list_indexes()
    }
    assert indexes["_id_"] == [("_id", 1)]
    assert indexes["order_timeline"] == [
        ("order_id", 1),
        ("event_ts", 1),
        ("batch_id", 1),
        ("source_file", 1),
        ("source_row_number", 1),
    ]
    assert indexes["driver_timeline"][0] == ("driver_id", 1)
    assert indexes["batch_lookup"] == [("batch_id", 1)]
    plan = query.command(
        {
            "explain": {"find": "raw_events", "filter": {"order_id": order}},
            "verbosity": "executionStats",
        }
    )
    winning = json.dumps(plan["queryPlanner"]["winningPlan"])
    assert "IXSCAN" in winning
    assert "order_timeline" in winning


def test_live_sql_equivalence(mongo: tuple[Any, Any], etl_conn: psycopg.Connection[Any]) -> None:
    app, query = mongo
    # The CI job loads its fixture into PostgreSQL before this test. Local runs use existing data.
    with etl_conn.transaction():
        etl_conn.execute("SET TRANSACTION READ ONLY")
        chosen = etl_conn.execute(
            "SELECT order_id FROM raw.events_raw "
            "WHERE order_id IS NOT NULL AND order_id <> '' LIMIT 1"
        ).fetchone()
        assert chosen is not None, "PostgreSQL needs a loaded fixture before test-mongo"
        order_id = chosen[0]
        selected = etl_conn.execute(
            sql.SQL("SELECT {} FROM raw.events_raw WHERE order_id = %s").format(
                sql.SQL(", ").join(sql.Identifier(name) for name in FIELDS)
            ),
            (order_id,),
        ).fetchall()
    documents = []
    for row in selected:
        fields = dict(zip(FIELDS, row, strict=True))
        doc = event_document(
            "{}", fields["batch_id"], fields["source_file"], fields["source_row_number"]
        )
        doc.update(fields)
        documents.append(doc)
    insert_chunk(app.raw_events, documents)
    result = compare_order(query, etl_conn, order_id)
    assert result["equal"]
    assert result["rows_mongo"] == result["rows_sql"] == len(documents)
    assert len(result["mongo_seconds"]) == len(result["sql_seconds"]) == 5
