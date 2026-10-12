"""Print the row count of every table in the four project schemas, plus the database size.

Usage (from the project root)::

    uv run python -m src.database.db_counts

Connects as ``deliverypulse_etl`` (read-only queries). Used to show that a second ``make load``
changes nothing.
"""

import sys
from collections.abc import Sequence

import psycopg
from psycopg import sql

from src.config import ConfigError, load_database_settings
from src.database.postgres_state import Connection

PROJECT_SCHEMAS = ("raw", "staging", "analytics", "audit")


def table_counts(conn: Connection) -> list[tuple[str, int]]:
    """(schema.table, row count) for every table in the project schemas, sorted by name."""
    tables = conn.execute(
        "SELECT schemaname, tablename FROM pg_tables WHERE schemaname = ANY(%s)",
        (list(PROJECT_SCHEMAS),),
    ).fetchall()
    counts: list[tuple[str, int]] = []
    for schema, table in sorted((str(row[0]), str(row[1])) for row in tables):
        query = sql.SQL("SELECT count(*) FROM {}").format(sql.Identifier(schema, table))
        row = conn.execute(query).fetchone()
        counts.append((f"{schema}.{table}", 0 if row is None else int(row[0])))
    return counts


def database_size(conn: Connection) -> str:
    """Human-readable size of the current database."""
    row = conn.execute("SELECT pg_size_pretty(pg_database_size(current_database()))").fetchone()
    return "" if row is None else str(row[0])


def main(argv: Sequence[str] | None = None) -> int:
    """Command-line entry point; returns the process exit code."""
    del argv
    try:
        settings = load_database_settings("etl")
        with psycopg.connect(settings.conninfo(), autocommit=True) as conn:
            for name, count in table_counts(conn):
                print(f"{name:40} {count:>10}")
            print(f"{'database size':40} {database_size(conn):>10}")
    except (ConfigError, psycopg.Error) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
