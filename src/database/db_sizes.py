"""Print the on-disk size of every table in the four project schemas, largest first.

Usage (from the project root)::

    uv run python -m src.database.db_sizes

Connects as ``deliverypulse_etl`` (read-only catalog queries). For each table: heap (data),
indexes and TOAST plus the free-space and visibility maps, so the parts add up to the total;
then the total per schema and the database size. Used to explain where the database's size
comes from.
"""

import sys
from collections.abc import Sequence
from dataclasses import dataclass

import psycopg

from src.config import ConfigError, load_database_settings
from src.database.db_counts import PROJECT_SCHEMAS, database_size
from src.database.postgres_state import Connection

TABLE_SIZES_SQL = """
    SELECT n.nspname,
           c.relname,
           pg_relation_size(c.oid) AS heap_bytes,
           pg_indexes_size(c.oid) AS index_bytes,
           pg_total_relation_size(c.oid) - pg_relation_size(c.oid) - pg_indexes_size(c.oid)
               AS other_bytes,
           pg_total_relation_size(c.oid) AS total_bytes
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relkind IN ('r', 'p') AND n.nspname = ANY(%s)
    ORDER BY total_bytes DESC, n.nspname, c.relname
"""


@dataclass(frozen=True)
class TableSize:
    """On-disk size of one table, in bytes."""

    name: str
    heap_bytes: int
    index_bytes: int
    other_bytes: int
    total_bytes: int


def table_sizes(conn: Connection) -> list[TableSize]:
    """Size of every table in the project schemas, largest first."""
    rows = conn.execute(TABLE_SIZES_SQL, (list(PROJECT_SCHEMAS),)).fetchall()
    return [
        TableSize(f"{row[0]}.{row[1]}", int(row[2]), int(row[3]), int(row[4]), int(row[5]))
        for row in rows
    ]


def schema_totals(sizes: Sequence[TableSize]) -> dict[str, int]:
    """Total bytes per schema, in ``PROJECT_SCHEMAS`` order."""
    totals = dict.fromkeys(PROJECT_SCHEMAS, 0)
    for size in sizes:
        totals[size.name.split(".", 1)[0]] += size.total_bytes
    return totals


def mib(size: int) -> str:
    """Bytes as MiB with one decimal."""
    return f"{size / 1024 / 1024:.1f}"


def main(argv: Sequence[str] | None = None) -> int:
    """Command-line entry point; returns the process exit code."""
    del argv
    try:
        settings = load_database_settings("etl")
        with psycopg.connect(settings.conninfo(), autocommit=True) as conn:
            sizes = table_sizes(conn)
            print(f"{'table (MiB)':40} {'heap':>8} {'indexes':>8} {'other':>8} {'total':>8}")
            for size in sizes:
                print(
                    f"{size.name:40} {mib(size.heap_bytes):>8} {mib(size.index_bytes):>8} "
                    f"{mib(size.other_bytes):>8} {mib(size.total_bytes):>8}"
                )
            for schema, total in schema_totals(sizes).items():
                print(f"{'schema ' + schema:40} {mib(total):>35}")
            print(f"{'database size':40} {database_size(conn):>35}")
    except (ConfigError, psycopg.Error) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
