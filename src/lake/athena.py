"""Run the AWS proof's single Athena query and compare it with PostgreSQL (`make aws-athena`).

The query (sql/athena/late_rate_by_city.sql) reads the cleaned orders that `make aws-export`
wrote to S3. The same late-delivery rule is evaluated in the database the lake was exported
from, as the read-only role, and the two results must match. Evidence goes to
``data/aws/athena-<UTC timestamp>.json``: counts, timings, bytes scanned and the cost estimate,
never an account ID, ARN or credential.
"""

import argparse
import json
import sys
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol, cast

import psycopg
from botocore.exceptions import BotoCoreError, ClientError

from src.config import PROJECT_ROOT, ConfigError, load_database_settings
from src.lake.aws import (
    AthenaSettings,
    AwsProofError,
    aws_session,
    contains_account_identifiers,
    load_athena_settings,
    load_aws_settings,
    redact,
)

QUERY_FILE = PROJECT_ROOT / "sql" / "athena" / "late_rate_by_city.sql"
EVIDENCE_DIR = PROJECT_ROOT / "data" / "aws"
# Athena SQL price in ap-southeast-2 (AWS Price List, published 2026-09-11): USD 5.00 per TB
# scanned, minimum 10 MB per successful or cancelled query; failed queries are free.
PRICE_PER_TB_USD = 5.0
MIN_BILLED_BYTES = 10 * 1024**2
BYTES_PER_TB = 1024**4
POLL_SECONDS = 1.0
TIMEOUT_SECONDS = 300.0
FINAL_STATES = frozenset({"SUCCEEDED", "FAILED", "CANCELLED"})
POSTGRES_QUERY = """
    SELECT city,
           count(*) AS delivered_orders,
           count(*) FILTER (WHERE delivery_ts > promised_ts) AS late_orders
    FROM analytics.orders
    WHERE status = 'delivered'
    GROUP BY city
    ORDER BY city
"""


class AthenaClient(Protocol):
    """Subset of the boto3 Athena client used here."""

    def start_query_execution(self, **kwargs: Any) -> Mapping[str, Any]: ...

    def get_query_execution(self, **kwargs: Any) -> Mapping[str, Any]: ...

    def get_query_results(self, **kwargs: Any) -> Mapping[str, Any]: ...


class AthenaQueryError(RuntimeError):
    """The Athena query did not succeed, or its result does not match PostgreSQL."""


@dataclass(frozen=True)
class CityRate:
    """Delivered and late orders of one city."""

    city: str
    delivered_orders: int
    late_orders: int

    @property
    def late_rate(self) -> float:
        """Late delivered / delivered, rounded to four places (the SQL definition)."""
        return round(self.late_orders / self.delivered_orders, 4)


@dataclass(frozen=True)
class QueryStats:
    """Final state and statistics of one Athena query execution."""

    state: str
    data_scanned_bytes: int
    engine_ms: int
    queue_ms: int
    total_ms: int


def estimated_cost_usd(state: str, data_scanned_bytes: int) -> float:
    """Athena's charge for one query: failed queries are free, others bill at least 10 MB."""
    if state == "FAILED":
        return 0.0
    billed = max(data_scanned_bytes, MIN_BILLED_BYTES)
    return billed / BYTES_PER_TB * PRICE_PER_TB_USD


def wait_for_query(
    client: AthenaClient,
    execution_id: str,
    *,
    poll_seconds: float = POLL_SECONDS,
    timeout_seconds: float = TIMEOUT_SECONDS,
    sleep: Callable[[float], None] = time.sleep,
) -> QueryStats:
    """Poll until the query reaches a final state; raise on timeout."""
    waited = 0.0
    while True:
        execution = client.get_query_execution(QueryExecutionId=execution_id)["QueryExecution"]
        state = str(execution["Status"]["State"])
        if state in FINAL_STATES:
            stats = execution.get("Statistics", {})
            return QueryStats(
                state=state,
                data_scanned_bytes=int(stats.get("DataScannedInBytes", 0)),
                engine_ms=int(stats.get("EngineExecutionTimeInMillis", 0)),
                queue_ms=int(stats.get("QueryQueueTimeInMillis", 0)),
                total_ms=int(stats.get("TotalExecutionTimeInMillis", 0)),
            )
        if waited >= timeout_seconds:
            raise AthenaQueryError(f"Athena query still {state} after {timeout_seconds:.0f} s.")
        sleep(poll_seconds)
        waited += poll_seconds


def read_results(client: AthenaClient, execution_id: str) -> list[CityRate]:
    """Every result row (all pages), skipping Athena's header row."""
    rows: list[list[str]] = []
    token: str | None = None
    while True:
        kwargs: dict[str, Any] = {"QueryExecutionId": execution_id}
        if token is not None:
            kwargs["NextToken"] = token
        page = client.get_query_results(**kwargs)
        for row in page["ResultSet"]["Rows"]:
            rows.append([str(cell.get("VarCharValue", "")) for cell in row["Data"]])
        token = page.get("NextToken")
        if token is None:
            break
    if not rows or rows[0][:3] != ["city", "delivered_orders", "late_orders"]:
        raise AthenaQueryError("Unexpected Athena result header.")
    return [CityRate(row[0], int(row[1]), int(row[2])) for row in rows[1:]]


def run_query(
    client: AthenaClient, settings: AthenaSettings, sql: str, **wait: Any
) -> tuple[QueryStats, list[CityRate]]:
    """Start ``sql`` in the proof workgroup and database, wait, and read the result."""
    started = client.start_query_execution(
        QueryString=sql,
        WorkGroup=settings.workgroup,
        QueryExecutionContext={"Database": settings.database},
    )
    execution_id = str(started["QueryExecutionId"])
    stats = wait_for_query(client, execution_id, **wait)
    if stats.state != "SUCCEEDED":
        raise AthenaQueryError(f"Athena query ended {stats.state}; see the Athena console.")
    return stats, read_results(client, execution_id)


def postgres_rates(conn: psycopg.Connection[Any]) -> list[CityRate]:
    """The same rule evaluated in PostgreSQL."""
    return [CityRate(str(c), int(d), int(late)) for c, d, late in conn.execute(POSTGRES_QUERY)]


def evidence(
    settings: AthenaSettings,
    stats: QueryStats,
    athena: Sequence[CityRate],
    postgres: Sequence[CityRate],
) -> dict[str, Any]:
    """The JSON evidence record. Raises if anything account-identifying slipped in."""
    record = {
        "date": datetime.now(UTC).isoformat(),
        "region": "ap-southeast-2",
        "workgroup": settings.workgroup,
        "database": settings.database,
        "query_file": QUERY_FILE.relative_to(PROJECT_ROOT).as_posix(),
        "stats": asdict(stats),
        "estimated_cost_usd": estimated_cost_usd(stats.state, stats.data_scanned_bytes),
        "athena": [{**asdict(rate), "late_rate": rate.late_rate} for rate in athena],
        "matches_postgres": list(athena) == list(postgres),
    }
    if contains_account_identifiers(json.dumps(record)):
        raise AthenaQueryError("Evidence would contain an account ID or ARN; not written.")
    return record


def write_evidence(record: Mapping[str, Any], directory: Path = EVIDENCE_DIR) -> Path:
    """Write ``record`` to a new timestamped file under ``directory``."""
    directory.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    path = directory / f"athena-{stamp}.json"
    path.write_text(json.dumps(record, indent=2) + "\n", encoding="utf-8")
    return path


def print_report(stats: QueryStats, athena: Sequence[CityRate], matches: bool) -> None:
    """Terminal output for the screenshot: no account ID, ARN, bucket or credential."""
    print("Athena: late-delivery rate by city (ap-southeast-2)")
    print(f"{'city':<12}{'delivered':>10}{'late':>7}{'late_rate':>11}")
    for rate in athena:
        line = f"{rate.city:<12}{rate.delivered_orders:>10}{rate.late_orders:>7}"
        print(f"{line}{rate.late_rate:>11.4f}")
    cost = estimated_cost_usd(stats.state, stats.data_scanned_bytes)
    print(
        f"state={stats.state} scanned={stats.data_scanned_bytes} bytes "
        f"engine={stats.engine_ms} ms total={stats.total_ms} ms estimated_cost=${cost:.6f}"
    )
    print(f"matches PostgreSQL: {'yes' if matches else 'NO'}")


def main(argv: Sequence[str] | None = None) -> int:
    """Command-line entry point (`make aws-athena`)."""
    parser = argparse.ArgumentParser(description="Run the AWS proof's Athena query.")
    parser.parse_args(argv)
    try:
        s3_settings = load_aws_settings()
        settings = load_athena_settings()
        client = cast(AthenaClient, aws_session(s3_settings.region).client("athena"))
        stats, athena = run_query(client, settings, QUERY_FILE.read_text(encoding="utf-8"))
        database = load_database_settings("readonly")
        with psycopg.connect(database.conninfo(), autocommit=True) as conn:
            postgres = postgres_rates(conn)
        record = evidence(settings, stats, athena, postgres)
        path = write_evidence(record)
    except (BotoCoreError, ClientError) as exc:
        print(f"ERROR: AWS request failed: {redact(str(exc))}", file=sys.stderr)
        return 1
    except (AthenaQueryError, AwsProofError, ConfigError, OSError, psycopg.Error) as exc:
        print(f"ERROR: {redact(str(exc))}", file=sys.stderr)
        return 1
    print_report(stats, athena, bool(record["matches_postgres"]))
    print(f"evidence: {path.relative_to(PROJECT_ROOT)}")
    return 0 if record["matches_postgres"] else 1


if __name__ == "__main__":
    sys.exit(main())
