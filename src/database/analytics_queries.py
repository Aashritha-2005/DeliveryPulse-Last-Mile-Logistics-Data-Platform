"""Run the analyst queries of ``sql/analytics.sql`` and measure query plans.

Usage (from the project root)::

    uv run python -m src.database.analytics_queries run
    uv run python -m src.database.analytics_queries explain --label before

Both connect as ``deliverypulse_readonly`` and need no arguments: date ranges are derived from
the loaded data and printed. ``explain`` runs ``EXPLAIN (ANALYZE, BUFFERS)`` per query: one
unmeasured warm-up, then three measured runs (median reported). It writes the text plans and a
JSON summary to ``docs/benchmarks/explain-<label>.{txt,json}``.
"""

import argparse
import json
import logging
import re
import statistics
import sys
from collections.abc import Sequence
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import psycopg

from src.config import PROJECT_ROOT, ConfigError, load_database_settings
from src.contract import REPORTING_TIMEZONE, reporting_date
from src.database.metrics import DRIVER_PERFORMANCE_SELECT, metric_parameters
from src.database.postgres_state import Connection
from src.transformations.transform import DEFAULT_RAW_DIR, TransformError, resolve_dataset
from src.utils.logging import configure_logging

logger = logging.getLogger("deliverypulse.analytics")

ANALYTICS_SQL = PROJECT_ROOT / "sql" / "analytics.sql"
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "docs" / "benchmarks"
REFRESH_QUERY = "refresh_driver_performance"
EXPLAIN_QUERIES = (
    "peak_hours",
    "avg_delivery_time_by_city",
    "top_bottom_drivers",
    "driver_utilisation",
    "order_event_summary",
    REFRESH_QUERY,
)
MEASURED_RUNS = 3
WEEK_DAYS = 7
MIN_DELIVERIES = 5
LIMIT = 10
# Queries that use the full loaded period; the others use the last WEEK_DAYS days.
FULL_PERIOD_QUERIES = ("late_rate_by_day", "peak_hours", "rolling_7_day")
_NAME = re.compile(r"--\s*name:\s*(\w+)\s*$")


def load_queries(path: Path = ANALYTICS_SQL) -> dict[str, str]:
    """Named query blocks of ``analytics.sql``, in file order (trailing semicolons removed)."""
    queries: dict[str, str] = {}
    name: str | None = None
    lines: list[str] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        match = _NAME.match(line)
        if match:
            if name is not None:
                queries[name] = "\n".join(lines).strip().rstrip(";")
            name = match.group(1)
            lines = []
        elif name is not None:
            lines.append(line)
    if name is not None:
        queries[name] = "\n".join(lines).strip().rstrip(";")
    return queries


def data_period(conn: Connection) -> tuple[date, date]:
    """First and last reporting date of the loaded orders (by order_created_ts)."""
    row = conn.execute(
        "SELECT min(order_created_ts), max(order_created_ts) FROM analytics.orders"
    ).fetchone()
    if row is None or row[0] is None:
        raise ValueError("analytics.orders is empty; run `make load` first")
    return reporting_date(int(row[0].timestamp())), reporting_date(int(row[1].timestamp()))


def default_parameters(first: date, last: date) -> dict[str, dict[str, Any]]:
    """Parameters per query: full period, or the last 7 days, or the last day."""
    week_start = max(first, last - timedelta(days=WEEK_DAYS - 1))
    common = {"tz": REPORTING_TIMEZONE, "min_deliveries": MIN_DELIVERIES, "limit": LIMIT}
    parameters: dict[str, dict[str, Any]] = {}
    for name in [*load_queries(), REFRESH_QUERY]:
        if name == REFRESH_QUERY:
            parameters[name] = metric_parameters([last])
        elif name == "order_event_summary":
            parameters[name] = {**common, "day": last}
        elif name in FULL_PERIOD_QUERIES:
            parameters[name] = {**common, "start_date": first, "end_date": last}
        else:
            parameters[name] = {**common, "start_date": week_start, "end_date": last}
    return parameters


def query_text(name: str) -> str:
    """The SQL of a named query; ``analytics.sql`` is trusted repository code, not input.

    The text is read from a file, so it is typed ``str``, not ``LiteralString``. Values are
    always bound as parameters, never formatted into it.
    """
    if name == REFRESH_QUERY:
        return DRIVER_PERFORMANCE_SELECT
    return load_queries()[name]


def _describe(parameters: dict[str, Any]) -> str:
    shown = {k: (str(v) if isinstance(v, date) else v) for k, v in parameters.items()}
    if "dates" in shown:
        shown["dates"] = [str(d) for d in parameters["dates"]]
    return json.dumps(shown, sort_keys=True)


def run_all(conn: Connection, max_rows: int = 12) -> None:
    """Run every analyst query and print its parameters and first rows."""
    first, last = data_period(conn)
    parameters = default_parameters(first, last)
    for name in load_queries():
        cur = conn.execute(query_text(name), parameters[name])
        columns = [c.name for c in cur.description or []]
        rows = cur.fetchall()
        print(f"\n== {name} ({len(rows)} rows) params={_describe(parameters[name])}")
        print(" | ".join(columns))
        for row in rows[:max_rows]:
            print(" | ".join("" if v is None else str(v) for v in row))


def _scan_nodes(plan: dict[str, Any]) -> list[str]:
    found: list[str] = []
    node = str(plan.get("Node Type", ""))
    if "Scan" in node:
        target = plan.get("Index Name") or plan.get("Relation Name") or ""
        found.append(f"{node} {target}".strip())
    for child in plan.get("Plans", []):
        found += _scan_nodes(child)
    return found


def explain_query(conn: Connection, name: str, parameters: dict[str, Any]) -> dict[str, Any]:
    """Warm-up plus MEASURED_RUNS measured EXPLAIN (ANALYZE, BUFFERS) runs of one query."""
    text = query_text(name)
    json_explain = "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + text
    text_explain = "EXPLAIN (ANALYZE, BUFFERS) " + text
    conn.execute(json_explain, parameters).fetchall()  # warm-up, not measured
    runs: list[dict[str, Any]] = []
    for _ in range(MEASURED_RUNS):
        row = conn.execute(json_explain, parameters).fetchone()
        if row is None:
            raise RuntimeError(f"EXPLAIN returned nothing for {name}")
        result = row[0][0]
        plan = result["Plan"]
        runs.append(
            {
                "execution_ms": float(result["Execution Time"]),
                "planning_ms": float(result["Planning Time"]),
                "shared_hit": int(plan.get("Shared Hit Blocks", 0)),
                "shared_read": int(plan.get("Shared Read Blocks", 0)),
                "scans": sorted(set(_scan_nodes(plan))),
            }
        )
    plan_text = "\n".join(str(r[0]) for r in conn.execute(text_explain, parameters).fetchall())
    return {
        "query": name,
        "parameters": _describe(parameters),
        "median_execution_ms": round(statistics.median(r["execution_ms"] for r in runs), 3),
        "execution_ms_runs": [round(r["execution_ms"], 3) for r in runs],
        "median_planning_ms": round(statistics.median(r["planning_ms"] for r in runs), 3),
        "shared_hit_blocks": runs[-1]["shared_hit"],
        "shared_read_blocks": runs[-1]["shared_read"],
        "scans": runs[-1]["scans"],
        "plan_text": plan_text,
    }


def explain_all(conn: Connection, label: str, dataset: str, output_dir: Path) -> list[Any]:
    """Measure every query in EXPLAIN_QUERIES and write the label's text and JSON files."""
    first, last = data_period(conn)
    parameters = default_parameters(first, last)
    version_row = conn.execute("SHOW server_version").fetchone()
    version = "" if version_row is None else str(version_row[0])
    results = [explain_query(conn, name, parameters[name]) for name in EXPLAIN_QUERIES]
    output_dir.mkdir(parents=True, exist_ok=True)
    header = {
        "label": label,
        "dataset": dataset,
        "postgresql_version": version,
        "data_period": [str(first), str(last)],
        "measured_runs_per_query": MEASURED_RUNS,
        "warm_up_runs_per_query": 1,
    }
    queries = [{k: v for k, v in r.items() if k != "plan_text"} for r in results]
    summary = {**header, "queries": queries}
    (output_dir / f"explain-{label}.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    text_parts = [json.dumps(header, indent=2, sort_keys=True)]
    for r in results:
        text_parts.append(f"\n===== {r['query']}  params={r['parameters']}\n{r['plan_text']}")
    plans = "\n".join(text_parts) + "\n"
    (output_dir / f"explain-{label}.txt").write_text(plans, encoding="utf-8")
    print(json.dumps(header, sort_keys=True))
    print("query | median ms | runs ms | planning ms | shared hit | shared read | scans")
    for r in results:
        print(
            f"{r['query']} | {r['median_execution_ms']} | {r['execution_ms_runs']} | "
            f"{r['median_planning_ms']} | {r['shared_hit_blocks']} | {r['shared_read_blocks']} | "
            f"{', '.join(r['scans'])}"
        )
        print(f"    params={r['parameters']}")
    return results


def main(argv: Sequence[str] | None = None) -> int:
    """Command-line entry point; returns the process exit code."""
    parser = argparse.ArgumentParser(description="Run or explain the analyst queries.")
    parser.add_argument("command", choices=("run", "explain"))
    parser.add_argument("--label", default="current", help="explain output label")
    parser.add_argument("--raw-dir", type=Path, default=DEFAULT_RAW_DIR)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    args = parser.parse_args(argv)
    configure_logging()
    label: str = args.label
    if not re.fullmatch(r"[A-Za-z0-9_-]+", label):
        logger.error("--label may contain only letters, digits, '_' and '-'")
        return 1
    try:
        dataset = resolve_dataset(args.raw_dir, None).name
        settings = load_database_settings("readonly")
        with psycopg.connect(settings.conninfo(), autocommit=True) as conn:
            if args.command == "run":
                run_all(conn)
            else:
                explain_all(conn, label, dataset, args.output_dir)
    except (TransformError, ConfigError, ValueError, psycopg.Error) as exc:
        logger.error("%s", exc)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
