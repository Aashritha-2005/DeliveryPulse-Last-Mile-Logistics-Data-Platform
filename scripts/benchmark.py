"""pandas vs PySpark benchmark (`make benchmark`), kept separate from the normal ETL.

For each size (shards of 50,000 orders, smallest first, up to ``--largest``):
  1. project the disk use (measured shard sizes, the Spark spill factor) and stop before the
     size if it would exceed the budget (default 4 GiB, never higher) or leave less than 10 GiB
     free; build the missing shards (src/bench/shards.py, guarded per shard);
  2. per engine still running: one warm-up and ``--runs`` measured runs, each in its own
     process (src/bench/engine.py, started with ``multiprocessing``). Every 0.2 s the harness
     reads the resident memory of the run's whole process tree (Python + Spark JVM) with
     psutil; every second the disk use under
     data/bench. A run above the memory limit (4 GiB) is killed and that engine stops for larger
     sizes; a disk violation stops the whole benchmark;
  3. after each run, Spark's scratch space and the temporary files are deleted.
Then the read benchmark (single file vs partitioned) runs on the largest completed size.

Output: docs/benchmarks/scale-<date>.json (machine-readable) and the generated section of
docs/benchmarks.md. Machine details: hardware model, cores, RAM, OS version, Python, JDK and
package versions only; no hostname, user name or absolute path is recorded.

Run from the project root with the project on PYTHONPATH (the Makefile does this). No shell
command is run: runs are ``multiprocessing`` children, the machine record comes from
``sysctlbyname`` and ``$JAVA_HOME/release``.
"""

import argparse
import ctypes
import json
import multiprocessing
import os
import platform
import sys
import time
from datetime import date
from importlib import metadata
from multiprocessing.process import BaseProcess
from pathlib import Path
from typing import Any

import psutil

from src.bench.disk import (
    BENCH_ROOT,
    GIB,
    READS_DIR,
    SPARK_LOCAL_DIR,
    TMP_DIR,
    DiskGuard,
    DiskGuardError,
    allocated_bytes,
    budget_bytes,
    projected_bytes,
    remove_tree,
)
from src.bench.engine import child
from src.bench.monitor import (
    MEMORY_LIMIT_BYTES,
    jdk_version,
    machine_record,
    median,
    sizes_up_to,
)
from src.bench.reads import write_read_copies
from src.bench.shards import MEASURED_SHARD_SIZES, SHARD_ORDERS, ensure_shards, shard_dir

MEMORY_POLL_SECONDS = 0.2
DISK_POLL_SECONDS = 1.0
INITIAL_SPILL_FACTOR = 2.0  # estimate until the first Spark run measures it
RESULTS_DIR = Path("docs/benchmarks")
REPORT = Path("docs/benchmarks.md")
START_MARKER = "<!-- scale-benchmark:start -->"
END_MARKER = "<!-- scale-benchmark:end -->"
PACKAGES = ("numpy", "pandas", "pyarrow", "pyspark", "py4j", "psutil")


class RunAbortedError(Exception):
    """A run was stopped: ``kind`` is memory, disk or error."""

    def __init__(self, kind: str, message: str) -> None:
        super().__init__(message)
        self.kind = kind


def sanitize(text: str) -> str:
    """Remove absolute paths of this machine from a message."""
    return text.replace(str(Path.cwd()), ".").replace(str(Path.home()), "~")


def sysctl(name: str) -> bytes:
    """The raw value of a macOS ``sysctl`` entry, read with ``sysctlbyname`` (no shell)."""
    libc = ctypes.CDLL(None)
    size = ctypes.c_size_t()
    if libc.sysctlbyname(name.encode(), None, ctypes.byref(size), None, 0) != 0:
        return b""
    buffer = ctypes.create_string_buffer(size.value)
    if libc.sysctlbyname(name.encode(), buffer, ctypes.byref(size), None, 0) != 0:
        return b""
    return buffer.raw[: size.value]


def machine() -> dict[str, Any]:
    """Hardware model, cores, RAM, OS, Python, JDK and package versions (nothing else)."""
    release = Path(os.environ.get("JAVA_HOME", "")) / "release"
    memsize = sysctl("hw.memsize")
    return machine_record(
        {
            "hardware_model": sysctl("hw.model").rstrip(b"\x00").decode(errors="replace"),
            "cpu_cores": os.cpu_count(),
            "ram_bytes": int.from_bytes(memsize, sys.byteorder) if memsize else 0,
            "os": f"macOS {platform.mac_ver()[0]}",
            "python": platform.python_version(),
            "jdk": jdk_version(release.read_text(encoding="utf-8")) if release.is_file() else "",
            "packages": {name: metadata.version(name) for name in PACKAGES},
        }
    )


def process_tree_rss(process: psutil.Process) -> int:
    """Resident bytes of ``process`` and all its descendants (for Spark: Python + JVM)."""
    total = 0
    for member in [process, *process.children(recursive=True)]:
        try:
            total += member.memory_info().rss
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    return total


def clean_work_dirs(root: Path) -> None:
    """Delete Spark's scratch space and the temporary files of the last run."""
    remove_tree(root / SPARK_LOCAL_DIR)
    remove_tree(root / TMP_DIR)
    (root / TMP_DIR).mkdir(parents=True, exist_ok=True)


def run_once(
    engine: str, task: str, shards: int, guard: DiskGuard, memory_limit: int
) -> dict[str, Any]:
    """One run in its own process, watched for memory and disk; returns its record."""
    root = guard.root
    clean_work_dirs(root)
    output = root / TMP_DIR / "result.json"
    log = root / TMP_DIR / "run.log"
    os.environ["TMPDIR"] = str((root / TMP_DIR).resolve())  # inherited by the child and the JVM
    context = multiprocessing.get_context("spawn")
    process = context.Process(
        target=child,
        args=(engine, task, shards, str(root), str(output), str(log)),
        daemon=False,
    )
    peak_rss = peak_work = 0
    next_disk_check = 0.0
    started = time.perf_counter()
    try:
        process.start()
        watched = psutil.Process(process.pid)
        while process.is_alive():
            rss = process_tree_rss(watched)
            peak_rss = max(peak_rss, rss)
            if rss > memory_limit:
                stop(process)
                raise RunAbortedError(
                    "memory",
                    f"killed at {rss / GIB:.2f} GiB (limit {memory_limit / GIB:.0f} GiB)",
                )
            if time.perf_counter() >= next_disk_check:
                work = allocated_bytes(root / SPARK_LOCAL_DIR) + allocated_bytes(root / TMP_DIR)
                peak_work = max(peak_work, work)
                try:
                    guard.check(0, f"{engine} {task} at {shards} shards")
                except DiskGuardError as exc:
                    stop(process)
                    raise RunAbortedError("disk", str(exc)) from exc
                next_disk_check = time.perf_counter() + DISK_POLL_SECONDS
            time.sleep(MEMORY_POLL_SECONDS)
        process.join()
        wall = time.perf_counter() - started
        if process.exitcode != 0:
            tail = log.read_text(encoding="utf-8", errors="replace").strip().splitlines()[-5:]
            raise RunAbortedError("error", sanitize(" | ".join(tail)))
        record: dict[str, Any] = json.loads(output.read_text(encoding="utf-8"))
    finally:
        clean_work_dirs(root)
    record.update(wall_seconds=wall, peak_rss_bytes=peak_rss, peak_work_bytes=peak_work)
    return record


def stop(process: BaseProcess) -> None:
    """Kill a run's process and every descendant (Python and the Spark JVM)."""
    if process.pid is None:
        return
    try:
        watched = psutil.Process(process.pid)
    except psutil.NoSuchProcess:
        return
    for member in [*watched.children(recursive=True), watched]:
        try:
            member.kill()
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    process.join(timeout=30)


def measure(
    engine: str, task: str, shards: int, guard: DiskGuard, memory_limit: int, runs: int
) -> dict[str, Any]:
    """One warm-up and ``runs`` measured runs; medians and peaks."""
    run_once(engine, task, shards, guard, memory_limit)  # warm-up, not counted
    records = [run_once(engine, task, shards, guard, memory_limit) for _ in range(runs)]
    return {
        "status": "completed",
        "runs": runs,
        "wall_seconds": [round(r["wall_seconds"], 3) for r in records],
        "median_wall_seconds": round(median([r["wall_seconds"] for r in records]), 3),
        "median_engine_seconds": round(median([r["engine_seconds"] for r in records]), 3),
        "median_startup_seconds": round(median([r["startup_seconds"] for r in records]), 3),
        "peak_rss_bytes": max(r["peak_rss_bytes"] for r in records),
        "peak_work_bytes": max(r["peak_work_bytes"] for r in records),
        "result": records[-1]["result"],
    }


def engine_command(engine: str, task: str, shards: int) -> str:
    """The equivalent standalone command for one run (the harness starts it as a child process)."""
    return (
        f"uv run python -m src.bench.engine --engine {engine} --task {task} "
        f"--shards {shards} --output data/bench/tmp/result.json"
    )


def run_benchmark(args: argparse.Namespace) -> dict[str, Any]:
    """The whole benchmark; returns the machine-readable report."""
    guard = DiskGuard(BENCH_ROOT, budget_bytes(args.budget_gib))
    memory_limit = int(args.memory_gib * GIB)
    if memory_limit > MEMORY_LIMIT_BYTES:
        raise SystemExit("The memory limit may not exceed 4 GiB.")
    BENCH_ROOT.mkdir(parents=True, exist_ok=True)
    clean_work_dirs(BENCH_ROOT)
    sizes = MEASURED_SHARD_SIZES
    spill_factor, spill_measured = INITIAL_SPILL_FACTOR, False
    engines = ["pandas", "spark"]
    report: dict[str, Any] = {
        "date": date.today().isoformat(),
        "command": f"make benchmark BENCH_LARGEST={args.largest} BENCH_RUNS={args.runs}",
        "machine": machine(),
        "budget_bytes": guard.budget,
        "memory_limit_bytes": memory_limit,
        "shard_orders": SHARD_ORDERS,
        "sizes": [],
        "stopped": None,
    }
    completed: list[tuple[str, int]] = []
    for label, shards in sizes_up_to(args.largest):
        projected = projected_bytes(shards, sizes, spill_factor, reads=True)
        entry: dict[str, Any] = {"size": label, "shards": shards, "projected_bytes": projected}
        report["sizes"].append(entry)
        try:
            if projected > guard.budget:
                raise DiskGuardError(
                    f"projected {projected / GIB:.2f} GiB (spill factor {spill_factor:.2f}"
                    f"{' measured' if spill_measured else ' estimated'}) exceeds the "
                    f"{guard.budget / GIB:.0f} GiB budget"
                )
            guard.check(0, f"size {label}")
            ensure_shards(BENCH_ROOT, shards, guard, sizes)
        except DiskGuardError as exc:
            entry["status"] = "not run: disk budget"
            entry["reason"] = str(exc)
            report["stopped"] = f"{label}: not run because of the disk budget ({exc})"
            break
        entry["parquet_bytes_on_disk"] = sum(
            allocated_bytes(shard_dir(BENCH_ROOT, index)) for index in range(1, shards + 1)
        )
        entry["status"] = "run"
        disk_stop = False
        for engine in list(engines):
            entry[f"{engine}_command"] = engine_command(engine, "workload", shards)
            try:
                entry[engine] = measure(engine, "workload", shards, guard, memory_limit, args.runs)
            except RunAbortedError as exc:
                entry[engine] = {"status": f"stopped: {exc.kind}", "reason": str(exc)}
                engines.remove(engine)
                if exc.kind == "disk":
                    disk_stop = True
                    report["stopped"] = f"{label}: {engine} stopped by the disk guard ({exc})"
                    break
                continue
            if engine == "spark":
                factor = entry[engine]["peak_work_bytes"] / max(entry["parquet_bytes_on_disk"], 1)
                spill_factor = factor if not spill_measured else max(spill_factor, factor)
                spill_measured = True
                entry["spill_factor"] = round(factor, 3)
        if "pandas" in entry and "spark" in entry:
            pandas_result = entry["pandas"].get("result")
            spark_result = entry["spark"].get("result")
            if pandas_result is not None and spark_result is not None:
                entry["results_equal"] = pandas_result == spark_result
        if disk_stop:
            break
        completed.append((label, shards))
        if not engines:
            report["stopped"] = f"after {label}: every engine stopped"
            break
    report["spill_factor"] = {"value": round(spill_factor, 3), "measured": spill_measured}
    report["reads"] = run_reads(completed, guard, memory_limit, args.runs) if completed else None
    return report


def run_reads(
    completed: list[tuple[str, int]], guard: DiskGuard, memory_limit: int, runs: int
) -> dict[str, Any]:
    """The read benchmark on the largest completed size; the copies are deleted afterwards."""
    label, shards = completed[-1]
    reads_dir = BENCH_ROOT / READS_DIR
    record: dict[str, Any] = {"size": label, "shards": shards}
    try:
        guard.check(shards * MEASURED_SHARD_SIZES.read_copy_bytes, "the read-benchmark copies")
        shard_paths = [shard_dir(BENCH_ROOT, index) for index in range(1, shards + 1)]
        single, partitioned = write_read_copies(shard_paths, reads_dir)
        record["single_file_bytes_on_disk"] = allocated_bytes(single)
        record["partitioned_bytes_on_disk"] = allocated_bytes(partitioned)
        for engine in ("pandas", "spark"):
            for task in ("read-single", "read-partitioned"):
                key = f"{engine}_{task.replace('-', '_')}"
                record[f"{key}_command"] = engine_command(engine, task, shards)
                try:
                    record[key] = measure(engine, task, shards, guard, memory_limit, runs)
                except RunAbortedError as exc:
                    record[key] = {"status": f"stopped: {exc.kind}", "reason": str(exc)}
    except DiskGuardError as exc:
        record["status"] = f"not run: {exc}"
    finally:
        remove_tree(reads_dir)
    return record


def render(report: dict[str, Any]) -> str:
    """The generated Markdown section of docs/benchmarks.md."""
    host = report["machine"]
    packages = ", ".join(f"{name} {version}" for name, version in host.get("packages", {}).items())
    lines = [
        START_MARKER,
        f"Generated by `{report['command']}` on {report['date']}. Machine: "
        f"{host.get('hardware_model')}, {host.get('cpu_cores')} cores, "
        f"{host.get('ram_bytes', 0) / GIB:.0f} GiB RAM, {host.get('os')}, "
        f"Python {host.get('python')}, JDK {host.get('jdk')}, {packages}. "
        f"Limits: memory {report['memory_limit_bytes'] / GIB:.0f} GiB per run (process tree), "
        f"disk {report['budget_bytes'] / GIB:.0f} GiB under `data/bench/`.",
        "",
        "| Size | Orders (input rows) | Parquet on disk | Engine | Median wall s | Median engine s "
        "| Spark start s | Peak memory | Peak spill/temp | Status |",
        "|---|---|---|---|---|---|---|---|---|---|",
    ]
    for entry in report["sizes"]:
        if entry.get("status") != "run":
            lines.append(
                f"| {entry['size']} | | | | | | | | | {entry.get('status')}: "
                f"{entry.get('reason', '')} |"
            )
            continue
        for engine in ("pandas", "spark"):
            data = entry.get(engine)
            if data is None:
                lines.append(
                    f"| {entry['size']} | | {_mib(entry.get('parquet_bytes_on_disk'))} | {engine} "
                    "| | | | | | not attempted: stopped at a smaller size |"
                )
                continue
            if data.get("status") != "completed":
                lines.append(
                    f"| {entry['size']} | | {_mib(entry.get('parquet_bytes_on_disk'))} | {engine} "
                    f"| | | | | | {data['status']} ({data.get('reason', '')}) |"
                )
                continue
            rows = data["result"]["orders_input"]
            lines.append(
                f"| {entry['size']} | {rows:,} | {_mib(entry['parquet_bytes_on_disk'])} | {engine} "
                f"| {data['median_wall_seconds']} | {data['median_engine_seconds']} "
                f"| {data['median_startup_seconds'] if engine == 'spark' else ''} "
                f"| {_mib(data['peak_rss_bytes'])} | {_mib(data['peak_work_bytes'])} | completed |"
            )
        if "results_equal" in entry:
            equal = entry["results_equal"]
            lines.append(f"| {entry['size']} | | | both | | | | | | results equal: {equal} |")
    reads = report.get("reads") or {}
    if reads:
        lines += [
            "",
            f"Read benchmark at {reads['size']}: one week of the 21 generated days, events and "
            f"GPS pings per day. Single file {_mib(reads.get('single_file_bytes_on_disk'))}, "
            f"partitioned {_mib(reads.get('partitioned_bytes_on_disk'))}.",
            "",
            "| Engine | Layout | Median wall s | Median engine s | Peak memory | Status |",
            "|---|---|---|---|---|---|",
        ]
        for engine in ("pandas", "spark"):
            for layout in ("read_single", "read_partitioned"):
                data = reads.get(f"{engine}_{layout}", {})
                if data.get("status") == "completed":
                    lines.append(
                        f"| {engine} | {layout.removeprefix('read_')} "
                        f"| {data['median_wall_seconds']} | {data['median_engine_seconds']} "
                        f"| {_mib(data['peak_rss_bytes'])} | completed |"
                    )
                else:
                    lines.append(f"| {engine} | {layout} | | | | {data.get('status')} |")
    lines += [
        "",
        f"Stopping point: {report.get('stopped') or 'every planned size completed'}.",
        END_MARKER,
    ]
    return "\n".join(lines)


def _mib(value: Any) -> str:
    return f"{value / 1024 / 1024:,.1f} MiB" if isinstance(value, int) else ""


def write_report(report: dict[str, Any]) -> Path:
    """Write the JSON file and replace the generated section of docs/benchmarks.md."""
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    target = RESULTS_DIR / f"scale-{report['date']}.json"
    target.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    text = REPORT.read_text(encoding="utf-8")
    section = render(report)
    if START_MARKER in text and END_MARKER in text:
        before = text.split(START_MARKER)[0]
        after = text.split(END_MARKER, 1)[1]
        text = before + section + after
    else:
        text = text.rstrip("\n") + "\n\n## Scale benchmark: pandas vs PySpark\n\n" + section + "\n"
    REPORT.write_text(text, encoding="utf-8")
    return target


def main(argv: list[str] | None = None) -> int:
    """Command-line entry point."""
    parser = argparse.ArgumentParser(description="pandas vs PySpark benchmark.")
    parser.add_argument("--largest", default="2m", help="largest size to attempt (50k ... 5m)")
    parser.add_argument("--runs", type=int, default=3)
    parser.add_argument("--budget-gib", type=float, default=4.0)
    parser.add_argument("--memory-gib", type=float, default=4.0)
    parser.add_argument(
        "--render",
        type=Path,
        default=None,
        help="re-render docs/benchmarks.md from a saved JSON report; runs no benchmark",
    )
    args = parser.parse_args(argv)
    if args.render is not None:
        write_report(json.loads(args.render.read_text(encoding="utf-8")))
        print(f"re-rendered {REPORT} from {args.render}")
        return 0
    if not os.environ.get("JAVA_HOME"):
        print("ERROR: JAVA_HOME is not set; run through make benchmark (Java 21).", file=sys.stderr)
        return 1
    try:
        report = run_benchmark(args)
    except DiskGuardError as exc:
        print(f"STOPPED: {exc}", file=sys.stderr)
        return 1
    target = write_report(report)
    print(f"benchmark written: {target}; stopping point: {report['stopped'] or 'none'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
