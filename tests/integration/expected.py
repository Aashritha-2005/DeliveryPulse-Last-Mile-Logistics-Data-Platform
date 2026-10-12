"""Expected database objects and privileges: the single source of truth for integration tests."""

OWNER_ROLE = "deliverypulse_owner"
ETL_ROLE = "deliverypulse_etl"
READONLY_ROLE = "deliverypulse_readonly"
APPLICATION_ROLES = (ETL_ROLE, READONLY_ROLE)

EXPECTED_TABLES: dict[str, frozenset[str]] = {
    "raw": frozenset({"drivers_raw", "orders_raw", "events_raw", "shift_events_raw"}),
    "staging": frozenset({"rejected_records", "pending_events", "record_state", "gps_reference"}),
    "analytics": frozenset(
        {
            "dim_date",
            "dim_city",
            "drivers",
            "orders",
            "delivery_events",
            "driver_shift_events",
            "daily_delivery_metrics",
            "driver_performance",
        }
    ),
    "audit": frozenset(
        {
            "pipeline_runs",
            "record_dispositions",
            "pending_resolutions",
            "quality_runs",
            "quality_results",
        }
    ),
}
PROJECT_SCHEMAS: tuple[str, ...] = tuple(sorted(EXPECTED_TABLES))
ANALYTICS_TABLES: tuple[str, ...] = tuple(sorted(EXPECTED_TABLES["analytics"]))

# (child table, parent table) for every foreign key in analytics.
EXPECTED_FOREIGN_KEYS: frozenset[tuple[str, str]] = frozenset(
    {
        ("orders", "drivers"),
        ("delivery_events", "orders"),
        ("delivery_events", "drivers"),
        ("driver_shift_events", "drivers"),
        ("driver_performance", "drivers"),
    }
)

# Every table-level privilege PostgreSQL 18 knows about.
ALL_TABLE_PRIVILEGES = (
    "SELECT",
    "INSERT",
    "UPDATE",
    "DELETE",
    "TRUNCATE",
    "REFERENCES",
    "TRIGGER",
    "MAINTAIN",
)

# Table privileges each application role must hold, per schema (and nothing else).
EXPECTED_TABLE_PRIVILEGES: dict[str, dict[str, frozenset[str]]] = {
    READONLY_ROLE: {
        "analytics": frozenset({"SELECT"}),
    },
    ETL_ROLE: {
        "raw": frozenset({"SELECT", "INSERT"}),
        "staging": frozenset({"SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE"}),
        "analytics": frozenset({"SELECT", "INSERT", "UPDATE", "DELETE"}),
        "audit": frozenset({"SELECT", "INSERT", "UPDATE"}),
    },
}


def expected_table_grants(role: str) -> set[tuple[str, str, str]]:
    """Return the exact (schema, table, privilege) set ``role`` should hold."""
    return {
        (schema, table, privilege)
        for schema, privileges in EXPECTED_TABLE_PRIVILEGES[role].items()
        for table in EXPECTED_TABLES[schema]
        for privilege in privileges
    }
