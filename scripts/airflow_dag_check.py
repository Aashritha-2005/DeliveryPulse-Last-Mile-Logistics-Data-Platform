"""Parse and verify the Airflow DAG inside the pinned Airflow image (`make airflow-dag-check`, CI).

Runs with the image's own Python and Airflow; the project is mounted read-only at
/opt/deliverypulse. Every assertion below fails if the corresponding DAG setting is removed or
changed, and the script exits non-zero on the first failure.
"""

import sys
from datetime import timedelta

from airflow.dag_processing.dagbag import DagBag
from airflow.timetables.base import TimeRestriction
from airflow.timetables.interval import CronDataIntervalTimetable

DAGS_FOLDER = "/opt/deliverypulse/dags"
EXPECTED_TASKS = ["generate", "clean", "load", "quality_gate", "refresh_aggregates"]


def check() -> list[str]:
    """Every problem found (empty when the DAG is as designed)."""
    from src.orchestration.batches import batch_for_interval, batch_windows

    problems: list[str] = []
    # Example DAGs are excluded by AIRFLOW__CORE__LOAD_EXAMPLES=False (docker-compose.airflow.yml).
    bag = DagBag(dag_folder=DAGS_FOLDER)
    if set(bag.dag_ids) != {"delivery_pipeline"}:
        problems.append(f"unexpected DAGs parsed: {sorted(bag.dag_ids)}")
    if bag.import_errors:
        return [f"import error in {path}: {error}" for path, error in bag.import_errors.items()]
    # The parsed DAG itself (bag.get_dag would query a metadata database; none is used here).
    dag = bag.dags.get("delivery_pipeline")
    if dag is None:
        return ["DAG delivery_pipeline not found"]

    order = [task.task_id for task in dag.topological_sort()]
    if order != EXPECTED_TASKS:
        problems.append(f"task order {order} != {EXPECTED_TASKS}")
    for upstream, downstream in zip(EXPECTED_TASKS, EXPECTED_TASKS[1:], strict=False):
        if downstream not in dag.get_task(upstream).downstream_task_ids:
            problems.append(f"{upstream} is not directly upstream of {downstream}")

    if not dag.catchup:
        problems.append("catchup must be True (it backfills the batch windows)")
    if dag.max_active_runs != 1:
        problems.append(f"max_active_runs {dag.max_active_runs} != 1")
    if not isinstance(dag.timetable, CronDataIntervalTimetable):
        problems.append(f"timetable is {type(dag.timetable).__name__}, not a data-interval cron")

    # The schedule must produce exactly the generated batch windows, and each must map to its
    # batch (this is what makes "rerunning a date" well defined).
    windows = batch_windows()
    intervals = []
    restriction = TimeRestriction(earliest=dag.start_date, latest=dag.end_date, catchup=True)
    last = None
    for _ in range(len(windows) + 2):
        info = dag.timetable.next_dagrun_info(
            last_automated_data_interval=last, restriction=restriction
        )
        if info is None:
            break
        last = info.data_interval
        intervals.append((last.start, last.end))
    if [(s.timestamp(), e.timestamp()) for s, e in intervals] != [
        (s.timestamp(), e.timestamp()) for s, e in windows
    ]:
        problems.append(f"scheduled intervals {intervals} != batch windows {windows}")
    for sequence, (start, end) in enumerate(intervals, start=1):
        if batch_for_interval(start, end) != sequence:
            problems.append(f"interval {start} maps to the wrong batch")

    for task in dag.tasks:
        name = task.task_id
        if task.execution_timeout is None or task.execution_timeout <= timedelta(0):
            problems.append(f"{name}: no execution_timeout")
        if not task.on_failure_callback:
            problems.append(f"{name}: no on_failure_callback")
        if name == "quality_gate":
            if task.retries != 0:
                problems.append("quality_gate must not retry (a data failure is not transient)")
            continue
        if task.retries < 2:
            problems.append(f"{name}: retries {task.retries} < 2")
        if not task.retry_exponential_backoff:
            problems.append(f"{name}: no exponential retry backoff")
        if not task.on_retry_callback:
            problems.append(f"{name}: no on_retry_callback")
    if not dag.get_task("load").depends_on_past:
        problems.append("load must depend on its previous run (batches load in order)")
    if "--through-batch" not in dag.get_task("load").bash_command:
        problems.append("load must pass --through-batch")
    return problems


def main() -> int:
    problems = check()
    for problem in problems:
        print(f"FAIL: {problem}", file=sys.stderr)
    if problems:
        return 1
    print(
        "PASS: delivery_pipeline parses; 5 tasks in order; schedule = the 3 batch windows; "
        "retries, backoff, timeouts and callbacks set"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
