"""Compare raw order-event results and plans without modifying either database."""

import argparse
import json
import statistics
import time
from typing import Any

import psycopg
from psycopg import sql

from src.config import load_database_settings
from src.nosql.client import connect
from src.nosql.documents import IDENTITY_FIELDS, VALUE_FIELDS

SORT = [("event_ts", 1), ("batch_id", 1), ("source_file", 1), ("source_row_number", 1)]
FIELDS = IDENTITY_FIELDS + VALUE_FIELDS


def query_sql() -> sql.Composed:
    """Match Mongo simple-collation text ordering, including NULL before text."""
    return sql.SQL(
        "SELECT {} FROM raw.events_raw WHERE order_id = %s "
        'ORDER BY event_ts COLLATE "C" NULLS FIRST, batch_id COLLATE "C", '
        'source_file COLLATE "C", source_row_number'
    ).format(sql.SQL(", ").join(sql.Identifier(name) for name in FIELDS))


def canonical_row(row: dict[str, Any]) -> str:
    """Compare raw values and provenance, excluding independent ingestion timestamps."""
    return json.dumps({key: row.get(key) for key in FIELDS}, sort_keys=True, ensure_ascii=True)


def compare_order(database: Any, conn: psycopg.Connection[Any], order_id: str) -> dict[str, Any]:
    """Warm once, measure five materialized queries each, and require exact row equality."""
    collection = database["raw_events"]
    mongo_times: list[float] = []
    sql_times: list[float] = []
    mongo_rows: list[dict[str, Any]] = []
    sql_rows: list[dict[str, Any]] = []
    with conn.transaction():
        conn.execute("SET TRANSACTION READ ONLY")
        for run in range(6):
            # Alternate order to avoid consistently favoring the first query.
            for engine in ("mongo", "sql") if run % 2 == 0 else ("sql", "mongo"):
                start = time.perf_counter()
                if engine == "mongo":
                    projection = {field: 1 for field in FIELDS}
                    projection["_id"] = 0
                    mongo_rows = list(
                        collection.find({"order_id": order_id}, projection).sort(SORT)
                    )
                    elapsed = time.perf_counter() - start
                    if run:
                        mongo_times.append(elapsed)
                else:
                    fetched = conn.execute(query_sql(), (order_id,)).fetchall()
                    sql_rows = [dict(zip(FIELDS, row, strict=True)) for row in fetched]
                    elapsed = time.perf_counter() - start
                    if run:
                        sql_times.append(elapsed)
            if [canonical_row(row) for row in mongo_rows] != [
                canonical_row(row) for row in sql_rows
            ]:
                raise ValueError("MongoDB and SQL raw-event results differ")
        plan = conn.execute(
            sql.SQL("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) ") + query_sql(), (order_id,)
        ).fetchone()
    mongo_plan = database.command(
        {
            "explain": {
                "find": "raw_events",
                "filter": {"order_id": order_id},
                "sort": dict(SORT),
                "projection": {**dict.fromkeys(FIELDS, 1), "_id": 0},
            },
            "verbosity": "executionStats",
        }
    )
    return {
        "question": "all raw events for one order",
        "order_id": order_id,
        "rows_mongo": len(mongo_rows),
        "rows_sql": len(sql_rows),
        "equal": True,
        "mongo_seconds": mongo_times,
        "sql_seconds": sql_times,
        "mongo_median_seconds": statistics.median(mongo_times),
        "sql_median_seconds": statistics.median(sql_times),
        "mongo_plan": mongo_plan,
        "sql_plan": None if plan is None else plan[0],
    }


def main() -> int:
    """Run a bounded comparison with non-administrative credentials."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--order-id")
    args = parser.parse_args()
    try:
        client, database = connect("readonly")
        settings = load_database_settings("etl")
        with client, psycopg.connect(settings.conninfo(), autocommit=True) as conn:
            order_id = args.order_id
            if not order_id:
                row = database.raw_events.find_one({"order_id": {"$nin": [None, ""]}})
                if row is None:
                    raise ValueError("No order events loaded")
                order_id = row["order_id"]
            result = compare_order(database, conn, order_id)
            if not result["rows_mongo"]:
                raise ValueError("Comparison must include at least one raw event")
        print(json.dumps(result, sort_keys=True, default=str))
    except Exception:
        print("ERROR: MongoDB/SQL comparison failed; check services, credentials and matching data")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
