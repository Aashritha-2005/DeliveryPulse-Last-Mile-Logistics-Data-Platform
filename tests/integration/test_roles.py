"""Integration tests: least-privilege roles (sql/grants.sql), tested in both directions.

Every statement that is expected to be denied runs inside a transaction that is always
rolled back, so even an unexpected success could not change the database.
"""

from typing import LiteralString

import psycopg
import pytest
from psycopg import sql
from psycopg.rows import TupleRow

from tests.integration.expected import (
    ALL_TABLE_PRIVILEGES,
    ANALYTICS_TABLES,
    APPLICATION_ROLES,
    ETL_ROLE,
    OWNER_ROLE,
    PROJECT_SCHEMAS,
    READONLY_ROLE,
    expected_table_grants,
)

pytestmark = pytest.mark.integration

Connection = psycopg.Connection[TupleRow]


def granted_table_privileges(conn: Connection, role: str) -> set[tuple[str, str, str]]:
    """Return every (schema, table, privilege) ``role`` holds on tables in project schemas."""
    rows = conn.execute(
        """
        SELECT n.nspname, c.relname, p.privilege
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        CROSS JOIN unnest(%s::text[]) AS p(privilege)
        WHERE n.nspname = ANY(%s)
          AND c.relkind IN ('r', 'p', 'v', 'm')
          AND has_table_privilege(%s, c.oid, p.privilege)
        """,
        (list(ALL_TABLE_PRIVILEGES), list(PROJECT_SCHEMAS), role),
    ).fetchall()
    return {(row[0], row[1], row[2]) for row in rows}


def assert_denied(conn: Connection, statement: LiteralString) -> None:
    """Assert ``statement`` fails with insufficient privilege; always roll back."""
    with (
        pytest.raises(psycopg.errors.InsufficientPrivilege),
        conn.transaction(force_rollback=True),
    ):
        conn.execute(statement)


# ------------------------------------------------------------------ readonly ---


@pytest.mark.parametrize("table", ANALYTICS_TABLES)
def test_readonly_can_select_analytics_table(readonly_conn: Connection, table: str) -> None:
    query = sql.SQL("SELECT count(*) FROM {}").format(sql.Identifier("analytics", table))
    row = readonly_conn.execute(query).fetchone()
    assert row is not None


@pytest.mark.parametrize(
    "statement",
    [
        pytest.param(
            "INSERT INTO analytics.drivers (driver_id, city, vehicle_type, joined_date) "
            "VALUES ('privilege-test', 'test', 'car', DATE '2026-01-01')",
            id="insert",
        ),
        pytest.param("UPDATE analytics.drivers SET city = 'privilege-test'", id="update"),
        pytest.param("DELETE FROM analytics.drivers", id="delete"),
        pytest.param("TRUNCATE analytics.daily_delivery_metrics", id="truncate"),
        pytest.param("CREATE TABLE analytics.privilege_test (id integer)", id="create-table"),
        pytest.param("CREATE TABLE public.privilege_test (id integer)", id="create-in-public"),
        pytest.param("CREATE SCHEMA privilege_test", id="create-schema"),
        pytest.param("CREATE TEMP TABLE privilege_test (id integer)", id="create-temp-table"),
        pytest.param("ALTER TABLE analytics.drivers ADD COLUMN privilege_test integer", id="alter"),
        pytest.param("DROP TABLE analytics.drivers", id="drop"),
        pytest.param("SELECT 1 FROM raw.orders_raw LIMIT 1", id="select-raw"),
        pytest.param("SELECT 1 FROM raw.stream_events LIMIT 1", id="select-stream"),
        pytest.param("SELECT 1 FROM staging.rejected_records LIMIT 1", id="select-staging"),
        pytest.param("SELECT 1 FROM audit.pipeline_runs LIMIT 1", id="select-audit"),
    ],
)
def test_readonly_is_denied(readonly_conn: Connection, statement: LiteralString) -> None:
    assert_denied(readonly_conn, statement)


def test_readonly_table_privileges_are_exactly_select_on_analytics(
    readonly_conn: Connection,
) -> None:
    granted = granted_table_privileges(readonly_conn, READONLY_ROLE)
    assert granted == expected_table_grants(READONLY_ROLE)


def test_readonly_has_no_sequence_privileges(readonly_conn: Connection) -> None:
    rows = readonly_conn.execute(
        """
        SELECT n.nspname, c.relname, p.privilege
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        CROSS JOIN unnest(ARRAY['USAGE', 'SELECT', 'UPDATE']) AS p(privilege)
        WHERE n.nspname = ANY(%s)
          AND c.relkind = 'S'
          AND has_sequence_privilege(%s, c.oid, p.privilege)
        """,
        (list(PROJECT_SCHEMAS), READONLY_ROLE),
    ).fetchall()
    assert rows == []


def test_readonly_schema_privileges(readonly_conn: Connection) -> None:
    rows = readonly_conn.execute(
        """
        SELECT nspname,
               has_schema_privilege(%s, oid, 'USAGE'),
               has_schema_privilege(%s, oid, 'CREATE')
        FROM pg_namespace
        WHERE nspname = ANY(%s)
        """,
        (READONLY_ROLE, READONLY_ROLE, [*PROJECT_SCHEMAS, "public"]),
    ).fetchall()
    usage = {row[0] for row in rows if row[1]}
    create = {row[0] for row in rows if row[2]}
    assert usage == {"analytics"}
    assert create == set()


def test_readonly_database_privileges(readonly_conn: Connection) -> None:
    row = readonly_conn.execute(
        """
        SELECT has_database_privilege(%s, current_database(), 'CONNECT'),
               has_database_privilege(%s, current_database(), 'CREATE'),
               has_database_privilege(%s, current_database(), 'TEMPORARY')
        """,
        (READONLY_ROLE, READONLY_ROLE, READONLY_ROLE),
    ).fetchone()
    assert row == (True, False, False)


def test_readonly_default_privileges_cover_only_future_analytics_tables(
    readonly_conn: Connection,
) -> None:
    rows = readonly_conn.execute(
        """
        SELECT coalesce(n.nspname, '<all schemas>'), d.defaclobjtype, a.privilege_type
        FROM pg_default_acl d
        LEFT JOIN pg_namespace n ON n.oid = d.defaclnamespace
        CROSS JOIN LATERAL aclexplode(d.defaclacl) AS a
        WHERE a.grantee = %s::regrole
        """,
        (READONLY_ROLE,),
    ).fetchall()
    assert {(row[0], row[1], row[2]) for row in rows} == {("analytics", "r", "SELECT")}


# ----------------------------------------------------------------------- etl ---


def test_etl_table_privileges_are_exactly_as_designed(etl_conn: Connection) -> None:
    assert granted_table_privileges(etl_conn, ETL_ROLE) == expected_table_grants(ETL_ROLE)


def test_etl_can_insert_into_staging(etl_conn: Connection) -> None:
    with etl_conn.transaction(force_rollback=True):
        row = etl_conn.execute(
            """
            INSERT INTO staging.rejected_records
                (batch_id, source, record_key, record_payload, reason)
            VALUES (%s, %s, %s, %s::jsonb, %s)
            RETURNING rejected_id
            """,
            ("privilege-test", "orders", "privilege-test", "{}", "privilege_test"),
        ).fetchone()
    assert row is not None


@pytest.mark.parametrize(
    "statement",
    [
        pytest.param("UPDATE raw.orders_raw SET status = 'privilege-test'", id="update-raw"),
        pytest.param("DELETE FROM raw.orders_raw", id="delete-raw"),
        pytest.param("UPDATE raw.stream_events SET record_key = 'changed'", id="update-stream"),
        pytest.param("DELETE FROM raw.stream_events", id="delete-stream"),
        pytest.param("TRUNCATE raw.stream_events", id="truncate-stream"),
        pytest.param("TRUNCATE analytics.orders", id="truncate-analytics"),
        pytest.param("CREATE TABLE analytics.privilege_test (id integer)", id="create-analytics"),
        pytest.param("CREATE TABLE staging.privilege_test (id integer)", id="create-staging"),
        pytest.param("CREATE SCHEMA privilege_test", id="create-schema"),
        pytest.param("ALTER TABLE analytics.orders ADD COLUMN privilege_test integer", id="alter"),
        pytest.param("DROP TABLE raw.orders_raw", id="drop"),
    ],
)
def test_etl_is_denied(etl_conn: Connection, statement: LiteralString) -> None:
    assert_denied(etl_conn, statement)


ANALYZE_FUNCTION = "staging.analyze_quality_tables()"


def test_etl_has_no_maintain_privilege_on_any_table(etl_conn: Connection) -> None:
    granted = granted_table_privileges(etl_conn, ETL_ROLE)
    assert {grant for grant in granted if grant[2] == "MAINTAIN"} == set()


def executable_project_functions(conn: Connection, role: str) -> set[str]:
    """Every function in the project schemas that ``role`` may execute, as schema.name()."""
    rows = conn.execute(
        """
        SELECT n.nspname || '.' || p.proname || '()'
        FROM pg_proc p
        JOIN pg_namespace n ON n.oid = p.pronamespace
        WHERE n.nspname = ANY(%s) AND has_function_privilege(%s, p.oid, 'EXECUTE')
        """,
        (list(PROJECT_SCHEMAS), role),
    ).fetchall()
    return {str(row[0]) for row in rows}


def test_etl_may_execute_only_the_analyze_function(etl_conn: Connection) -> None:
    assert executable_project_functions(etl_conn, ETL_ROLE) == {ANALYZE_FUNCTION}


def test_readonly_and_public_may_not_execute_the_analyze_function(
    readonly_conn: Connection,
) -> None:
    assert executable_project_functions(readonly_conn, READONLY_ROLE) == set()
    # Looked up by name: the read-only role cannot even resolve names in the staging schema.
    row = readonly_conn.execute(
        """
        SELECT count(*) FILTER (WHERE a.grantee = 0), count(*)
        FROM pg_proc p
        JOIN pg_namespace n ON n.oid = p.pronamespace
        CROSS JOIN LATERAL aclexplode(p.proacl) AS a
        WHERE n.nspname = 'staging' AND p.proname = 'analyze_quality_tables'
        """
    ).fetchone()
    assert row is not None and row[0] == 0 and row[1] > 0  # explicit ACL, no PUBLIC grant
    assert_denied(readonly_conn, "SELECT staging.analyze_quality_tables()")


def test_analyze_function_runs_as_owner_with_a_fixed_search_path(etl_conn: Connection) -> None:
    row = etl_conn.execute(
        """
        SELECT pg_get_userbyid(p.proowner), p.prosecdef, p.proconfig
        FROM pg_proc p WHERE p.oid = %s::regprocedure
        """,
        (ANALYZE_FUNCTION,),
    ).fetchone()
    assert row == (OWNER_ROLE, True, ["search_path=pg_catalog"])


def test_etl_cannot_create_in_any_schema(etl_conn: Connection) -> None:
    rows = etl_conn.execute(
        """
        SELECT nspname FROM pg_namespace
        WHERE nspname = ANY(%s) AND has_schema_privilege(%s, oid, 'CREATE')
        """,
        ([*PROJECT_SCHEMAS, "public"], ETL_ROLE),
    ).fetchall()
    assert rows == []


# --------------------------------------------------------------- all roles ---


@pytest.mark.parametrize("role", [OWNER_ROLE, *APPLICATION_ROLES])
def test_role_has_no_elevated_attributes(readonly_conn: Connection, role: str) -> None:
    row = readonly_conn.execute(
        """
        SELECT rolsuper, rolcreaterole, rolcreatedb, rolreplication, rolbypassrls
        FROM pg_roles WHERE rolname = %s
        """,
        (role,),
    ).fetchone()
    assert row == (False, False, False, False, False)


def test_only_application_roles_can_log_in(readonly_conn: Connection) -> None:
    rows = readonly_conn.execute(
        "SELECT rolname, rolcanlogin FROM pg_roles WHERE rolname = ANY(%s)",
        ([OWNER_ROLE, *APPLICATION_ROLES],),
    ).fetchall()
    can_log_in = {row[0]: row[1] for row in rows}
    assert can_log_in == {OWNER_ROLE: False, ETL_ROLE: True, READONLY_ROLE: True}


def test_project_roles_belong_to_no_other_roles(readonly_conn: Connection) -> None:
    rows = readonly_conn.execute(
        """
        SELECT member.rolname, granted.rolname
        FROM pg_auth_members m
        JOIN pg_roles member ON member.oid = m.member
        JOIN pg_roles granted ON granted.oid = m.roleid
        WHERE member.rolname = ANY(%s)
        """,
        ([OWNER_ROLE, *APPLICATION_ROLES],),
    ).fetchall()
    assert rows == []
