"""DeliveryPulse pipeline DAG: generate -> clean -> load -> quality_gate -> refresh_aggregates.

Schedule and catchup: one run per generated batch window. The windows are weekly, starting
Thursday 00:00 Asia/Kolkata (2026-09-10, 09-17, 09-24), and ``start_date`` / ``end_date`` bound
the DAG to exactly those three. With ``catchup=True``, unpausing the DAG backfills the three
runs, oldest first (``max_active_runs=1``). Run k loads the dataset through batch k
(``load`` depends on its previous run), so the batches enter the database in order. Nothing is
scheduled outside the generated windows.

Every task is idempotent, so clearing a run ("rerunning a date") cannot duplicate data:
generate is a no-op for an existing dataset, clean rebuilds the processed layer, load skips
batches whose batch_id already succeeded, the quality gate only reads (plus an audit row), and
the aggregate refresh deterministically recomputes every date.

Tasks run the project's own command-line entry points in the project's virtual environment
inside the image (see src/orchestration/commands.py). Database settings come from the container
environment; this file holds no credentials. SLA-style alerts are not used: Airflow 3 removed
SLAs, and their replacement (deadline alerts) is experimental in 3.3.2.
"""

import logging
from datetime import timedelta
from typing import Any

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG
from airflow.timetables.interval import CronDataIntervalTimetable

from src.orchestration import commands
from src.orchestration.batches import batch_for_interval, batch_windows

log = logging.getLogger("deliverypulse.dag")

REPORTING_TIMEZONE = "Asia/Kolkata"
_WINDOWS = batch_windows()
START = pendulum.instance(_WINDOWS[0][0]).in_timezone(REPORTING_TIMEZONE)
# The last run's data interval starts at the last window's start.
END = pendulum.instance(_WINDOWS[-1][0]).in_timezone(REPORTING_TIMEZONE)


def _describe(context: dict[str, Any]) -> str:
    ti = context.get("ti")
    run = context.get("dag_run")
    task = getattr(ti, "task_id", "?")
    attempt = getattr(ti, "try_number", "?")
    run_id = getattr(run, "run_id", "?")
    return f"task {task} (try {attempt}) of run {run_id}"


def on_failure(context: dict[str, Any]) -> None:
    """Failure callback: one structured log line with the task, run, try and error."""
    log.error("FAILED: %s: %s", _describe(context), context.get("exception"))


def on_retry(context: dict[str, Any]) -> None:
    """Retry callback: log every retry so flaky steps are visible."""
    log.warning("RETRY: %s after: %s", _describe(context), context.get("exception"))


default_args: dict[str, Any] = {
    "owner": "deliverypulse",
    "retries": 2,
    "retry_delay": timedelta(seconds=30),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=5),
    "on_failure_callback": on_failure,
    "on_retry_callback": on_retry,
    "cwd": commands.PROJECT_DIR,
}

with DAG(
    dag_id="delivery_pipeline",
    description="Generate, clean, load, quality-gate and aggregate DeliveryPulse batches.",
    schedule=CronDataIntervalTimetable("0 0 * * 4", timezone=REPORTING_TIMEZONE),
    start_date=START,
    end_date=END,
    catchup=True,
    max_active_runs=1,
    default_args=default_args,
    user_defined_macros={"batch_for": batch_for_interval},
    tags=["deliverypulse"],
    doc_md=__doc__,
) as dag:
    generate = BashOperator(
        task_id="generate",
        bash_command=commands.bash(commands.generate()),
        execution_timeout=timedelta(minutes=10),
    )
    clean = BashOperator(
        task_id="clean",
        bash_command=commands.bash(commands.clean()),
        execution_timeout=timedelta(minutes=15),
    )
    load = BashOperator(
        task_id="load",
        bash_command=commands.bash(commands.load(commands.BATCH_TEMPLATE)),
        execution_timeout=timedelta(minutes=20),
        depends_on_past=True,
    )
    quality_gate = BashOperator(
        task_id="quality_gate",
        bash_command=commands.bash(commands.quality_gate()),
        execution_timeout=timedelta(minutes=15),
        retries=0,  # a blocking data failure is not transient: fail at once
    )
    refresh_aggregates = BashOperator(
        task_id="refresh_aggregates",
        bash_command=commands.bash(commands.refresh_aggregates()),
        execution_timeout=timedelta(minutes=10),
    )

    generate >> clean >> load >> quality_gate >> refresh_aggregates
