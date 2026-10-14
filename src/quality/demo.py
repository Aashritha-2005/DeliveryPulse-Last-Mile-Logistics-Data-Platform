"""Demonstrate the quality gate failing, then undo it without a trace (demo only).

`make quality-demo-plant` inserts one order that satisfies every database constraint but breaks
the blocking check ``orders_positive_amounts`` (``fare_amount = 0``). The next quality run, for
example the Airflow DAG's ``quality_gate`` task, fails and blocks the tasks after it.
`make quality-demo-remove` deletes exactly that order. Both are idempotent.

Because the planted order has its own creation date in the daily aggregates' range and no
aggregate refresh runs while it exists, ``metrics_match_facts`` also fails while it is planted.
Removing it restores the business tables exactly (a test compares their digests).

Usage::

    uv run python -m src.quality.demo plant
    uv run python -m src.quality.demo remove
"""

import argparse
import logging
import sys
from collections.abc import Sequence

import psycopg

from src.config import ConfigError, load_database_settings
from src.database.postgres_state import Connection
from src.utils.logging import configure_logging

logger = logging.getLogger("deliverypulse.quality_demo")

DEMO_ORDER_ID = "ORD-QUALITY-DEMO"


class DemoError(RuntimeError):
    """The demo cannot run (for example, nothing is loaded yet)."""


def plant(conn: Connection) -> bool:
    """Insert the demo order; True if it was inserted, False if it was already there."""
    driver = conn.execute(
        "SELECT driver_id, city FROM analytics.drivers ORDER BY driver_id LIMIT 1"
    ).fetchone()
    period = conn.execute(
        "SELECT min(order_created_ts) FROM analytics.orders WHERE order_id <> %s",
        (DEMO_ORDER_ID,),
    ).fetchone()
    if driver is None or period is None or period[0] is None:
        raise DemoError("analytics is empty; run `make load` first")
    created = period[0]
    row = conn.execute(
        "INSERT INTO analytics.orders (order_id, customer_id, driver_id, order_created_ts, "
        "pickup_ts, delivery_ts, promised_ts, distance_km, status, city, fare_amount, "
        "source_updated_at) "
        "VALUES (%s, 'CUST-QUALITY-DEMO', %s, %s, NULL, NULL, %s + interval '1 hour', 1.000, "
        "'assigned', %s, 0, %s) ON CONFLICT (order_id) DO NOTHING RETURNING order_id",
        (DEMO_ORDER_ID, driver[0], created, created, driver[1], created),
    ).fetchone()
    return row is not None


def remove(conn: Connection) -> bool:
    """Delete the demo order; True if it was deleted, False if it was not there."""
    row = conn.execute(
        "DELETE FROM analytics.orders WHERE order_id = %s RETURNING order_id", (DEMO_ORDER_ID,)
    ).fetchone()
    return row is not None


def main(argv: Sequence[str] | None = None) -> int:
    """Command-line entry point; returns the process exit code."""
    parser = argparse.ArgumentParser(description="Plant or remove the quality-gate demo row.")
    parser.add_argument("action", choices=("plant", "remove"))
    args = parser.parse_args(argv)
    configure_logging()
    try:
        settings = load_database_settings("etl")
        with psycopg.connect(settings.conninfo(), autocommit=True) as conn:
            changed = plant(conn) if args.action == "plant" else remove(conn)
    except (ConfigError, DemoError, psycopg.Error) as exc:
        logger.error("%s", exc)
        return 1
    state = "done" if changed else "nothing to do (already in that state)"
    logger.info("quality demo %s %s: %s", args.action, DEMO_ORDER_ID, state)
    return 0


if __name__ == "__main__":
    sys.exit(main())
