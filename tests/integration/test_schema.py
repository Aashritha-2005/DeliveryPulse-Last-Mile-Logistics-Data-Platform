"""Integration tests: schemas, tables, constraints and ownership created on first initialisation."""

import psycopg
import pytest
from psycopg.rows import TupleRow

from tests.integration.expected import (
    EXPECTED_FOREIGN_KEYS,
    EXPECTED_TABLES,
    OWNER_ROLE,
    PROJECT_SCHEMAS,
)

pytestmark = pytest.mark.integration

Connection = psycopg.Connection[TupleRow]


def test_project_schemas_exist(readonly_conn: Connection) -> None:
    rows = readonly_conn.execute(
        "SELECT nspname FROM pg_namespace WHERE nspname = ANY(%s)", (list(PROJECT_SCHEMAS),)
    ).fetchall()
    assert {row[0] for row in rows} == set(PROJECT_SCHEMAS)


@pytest.mark.parametrize("schema", PROJECT_SCHEMAS)
def test_schema_contains_exactly_expected_tables(readonly_conn: Connection, schema: str) -> None:
    rows = readonly_conn.execute(
        """
        SELECT c.relname
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = %s AND c.relkind IN ('r', 'p')
        """,
        (schema,),
    ).fetchall()
    assert {row[0] for row in rows} == EXPECTED_TABLES[schema]


def test_check_constraints_exist_only_in_analytics(readonly_conn: Connection) -> None:
    # contype 'c' = CHECK. PostgreSQL 18 also records NOT NULL in pg_constraint (contype 'n');
    # those are allowed in every schema and deliberately excluded here.
    rows = readonly_conn.execute(
        """
        SELECT n.nspname, count(*)
        FROM pg_constraint con
        JOIN pg_namespace n ON n.oid = con.connamespace
        WHERE con.contype = 'c' AND n.nspname = ANY(%s)
        GROUP BY n.nspname
        """,
        (list(PROJECT_SCHEMAS),),
    ).fetchall()
    check_counts = {row[0]: row[1] for row in rows}
    assert set(check_counts) == {"analytics"}
    assert check_counts["analytics"] > 0


def test_analytics_foreign_keys(readonly_conn: Connection) -> None:
    rows = readonly_conn.execute(
        """
        SELECT child.relname, parent.relname
        FROM pg_constraint con
        JOIN pg_class child ON child.oid = con.conrelid
        JOIN pg_class parent ON parent.oid = con.confrelid
        JOIN pg_namespace n ON n.oid = con.connamespace
        WHERE con.contype = 'f' AND n.nspname = 'analytics'
        """
    ).fetchall()
    assert {(row[0], row[1]) for row in rows} == EXPECTED_FOREIGN_KEYS


def test_every_analytics_table_has_primary_key(readonly_conn: Connection) -> None:
    rows = readonly_conn.execute(
        """
        SELECT c.relname
        FROM pg_constraint con
        JOIN pg_class c ON c.oid = con.conrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE con.contype = 'p' AND n.nspname = 'analytics'
        """
    ).fetchall()
    assert {row[0] for row in rows} == EXPECTED_TABLES["analytics"]


def test_schemas_owned_by_owner_role(readonly_conn: Connection) -> None:
    rows = readonly_conn.execute(
        "SELECT nspname, pg_get_userbyid(nspowner) FROM pg_namespace WHERE nspname = ANY(%s)",
        (list(PROJECT_SCHEMAS),),
    ).fetchall()
    assert {row[1] for row in rows} == {OWNER_ROLE}


def test_tables_owned_by_owner_role(readonly_conn: Connection) -> None:
    rows = readonly_conn.execute(
        """
        SELECT DISTINCT pg_get_userbyid(c.relowner)
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = ANY(%s) AND c.relkind IN ('r', 'p', 'S')
        """,
        (list(PROJECT_SCHEMAS),),
    ).fetchall()
    assert {row[0] for row in rows} == {OWNER_ROLE}


def test_session_timezone_is_utc(readonly_conn: Connection) -> None:
    row = readonly_conn.execute("SELECT current_setting('TimeZone')").fetchone()
    assert row is not None
    assert row[0] == "UTC"
