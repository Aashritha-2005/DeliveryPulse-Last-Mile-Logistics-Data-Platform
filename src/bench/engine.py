"""One benchmark run in its own process: one engine, one task, one size (scripts/benchmark.py).

Writes a JSON file with the engine-side timings and the result, so the harness can time the
whole process from outside and compare engines. Tasks:

* ``workload``: the cleaning and aggregation workload (src/bench/spec.py) over the first n shards;
* ``read-single`` / ``read-partitioned``: the one-week query on the typed events copies.
"""

import argparse
import json
import os
import sys
import time
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from src.bench import pandas_engine, reads
from src.bench.disk import BENCH_ROOT, SPARK_LOCAL_DIR, TMP_DIR
from src.bench.shards import shard_dir

ENGINES = ("pandas", "spark")
TASKS = ("workload", "read-single", "read-partitioned")


def read_path(reads_dir: Path, task: str) -> Path:
    """The read-benchmark copy for ``task``."""
    if task == "read-single":
        return reads_dir / reads.SINGLE_FILE
    return reads_dir / reads.PARTITIONED_DIR


def run(engine: str, task: str, shards: int, root: Path) -> dict[str, Any]:
    """Run one task; return timings (seconds) and the result."""
    shard_paths = [shard_dir(root, index) for index in range(1, shards + 1)]
    reads_dir = root / "reads"
    output: dict[str, Any] = {"engine": engine, "task": task, "shards": shards}
    if engine == "pandas":
        started = time.perf_counter()
        if task == "workload":
            output["result"] = pandas_engine.run_workload(shard_paths).to_json()
        else:
            output["result"] = reads.pandas_week_counts(read_path(reads_dir, task))
        output["engine_seconds"] = time.perf_counter() - started
        output["startup_seconds"] = 0.0
        return output
    from src.bench import spark_engine  # imported only here: PySpark is the bench group

    startup = time.perf_counter()
    spark = spark_engine.session(root / SPARK_LOCAL_DIR, root / TMP_DIR)
    output["startup_seconds"] = time.perf_counter() - startup
    try:
        started = time.perf_counter()
        if task == "workload":
            output["result"] = spark_engine.run_workload(spark, shard_paths).to_json()
        else:
            output["result"] = spark_engine.week_counts(
                spark, read_path(reads_dir, task), reads.WEEK
            )
        output["engine_seconds"] = time.perf_counter() - started
    finally:
        spark.stop()
    return output


def child(engine: str, task: str, shards: int, root: str, output: str, log: str) -> None:
    """Entry point of one run started by the harness with ``multiprocessing`` (spawn).

    Standard output and error, including the Spark JVM's, go to ``log``; the result goes to
    ``output`` as JSON.
    """
    with open(log, "w", encoding="utf-8") as log_file:
        os.dup2(log_file.fileno(), 1)
        os.dup2(log_file.fileno(), 2)
        result = run(engine, task, shards, Path(root))
    Path(output).write_text(json.dumps(result, sort_keys=True), encoding="utf-8")


def main(argv: Sequence[str] | None = None) -> int:
    """Command-line entry point."""
    parser = argparse.ArgumentParser(description="One benchmark run.")
    parser.add_argument("--engine", choices=ENGINES, required=True)
    parser.add_argument("--task", choices=TASKS, required=True)
    parser.add_argument("--shards", type=int, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--root", type=Path, default=BENCH_ROOT)
    args = parser.parse_args(argv)
    result = run(args.engine, args.task, args.shards, args.root)
    args.output.write_text(json.dumps(result, sort_keys=True), encoding="utf-8")
    return 0


if __name__ == "__main__":
    sys.exit(main())
