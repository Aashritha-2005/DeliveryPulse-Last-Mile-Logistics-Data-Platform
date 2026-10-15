"""User-run, isolated core demo; no main-database or volume-reset operations."""

import argparse
import asyncio
import json
import os
import shutil
import socket
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import psycopg

from src.config import ConfigError, load_database_settings, read_environment
from src.ingestion.generator_config import GeneratorConfig

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data/demo"
PROJECT = "deliverypulse-demo"
CONTAINER = "deliverypulse-demo-postgres-1"
VOLUME = "deliverypulse_demo_pgdata"
DATABASE = "deliverypulse_demo"
IMAGE = (
    "postgres:18.6-trixie@sha256:74935e72241653ca55e0414067e6d8763aceb8a810eb51b452253ec3dcfc4336"
)
GIB = 1024**3
DATASET = GeneratorConfig.create(orders=5000, seed=42, drivers=500).dataset_name()
STEPS = (
    (
        "generate",
        "src.ingestion.generate_data",
        (
            "--orders",
            "5000",
            "--seed",
            "42",
            "--drivers",
            "500",
            "--raw-dir",
            "data/demo/raw",
            "--ground-truth-dir",
            "data/demo/ground_truth",
        ),
    ),
    (
        "transform",
        "src.transformations.transform",
        (
            "--raw-dir",
            "data/demo/raw",
            "--processed-dir",
            "data/demo/processed",
            "--dataset",
            DATASET,
        ),
    ),
    ("load", "src.database.load_postgres", ("--raw-dir", "data/demo/raw", "--dataset", DATASET)),
    (
        "reconcile",
        "src.groundtruth.reconcile_db",
        (
            "--raw-dir",
            "data/demo/raw",
            "--ground-truth-dir",
            "data/demo/ground_truth",
            "--dataset",
            DATASET,
        ),
    ),
    ("quality", "src.quality.run", ("--output", "data/demo/quality-latest.json")),
    ("analytics", "src.database.analytics_queries", ("run", "--raw-dir", "data/demo/raw")),
    ("dashboard-check", "src.dashboard.data", ()),
)


def checkpoint(stage: str, reserve: float = 0.0) -> int:
    """Refuse work below the 11 GiB cancellation point plus the remaining reserve."""
    free = shutil.disk_usage(ROOT).free
    required = 11.0 + reserve
    print(f"Demo disk {stage}: {free / GIB:.3f} GiB free; require {required:.3f} GiB", flush=True)
    if free < required * GIB:
        raise RuntimeError("Demo disk checkpoint failed; stop without deleting anything")
    return free


def validate_layout() -> None:
    """Reject shared volumes, extra services, mounts or alternate endpoints before Docker."""
    obj = json.loads((ROOT / "docker-compose.demo.yml").read_text())
    service = obj["services"]["postgres"]
    expected_mounts = [
        "demo_pgdata:/var/lib/postgresql",
        "./docker/postgres/init-deliverypulse.sh:/docker-entrypoint-initdb.d/10-init-deliverypulse.sh:ro",
        "./sql:/deliverypulse/sql:ro",
    ]
    if (
        obj.get("name") != PROJECT
        or set(obj["services"]) != {"postgres"}
        or obj["volumes"] != {"demo_pgdata": {"name": VOLUME}}
        or service["volumes"] != expected_mounts
        or service["ports"] != ["127.0.0.1:${DEMO_PORT:-55432}:5432"]
        or service["environment"]["POSTGRES_DB"] != DATABASE
        or service["image"] != IMAGE
        or any(k in service for k in ("extends", "include", "network_mode", "volumes_from"))
        or any(k in obj for k in ("include", "networks"))
    ):
        raise ValueError("Demo Compose isolation differs from the approved layout")
    for folder in (ROOT / "data", DATA, DATA / "raw", DATA / "processed", DATA / "ground_truth"):
        if folder.is_symlink():
            raise ValueError("Demo data directories must not be symbolic links")
    if DATA.exists() and any(p.is_symlink() for p in DATA.rglob("*")):
        raise ValueError("Demo data must not contain symbolic links")


def environment() -> dict[str, str]:
    """Select core credentials privately; force the isolated database and loopback endpoint."""
    values = read_environment()
    port = int(os.environ.get("DEMO_PORT", "55432"))
    if not 1024 <= port <= 65535 or port == int(values.get("POSTGRES_PORT", "5432")):
        raise ValueError("DEMO_PORT must be an unprivileged port distinct from POSTGRES_PORT")
    result = dict(os.environ)
    for name in ("COMPOSE_FILE", "COMPOSE_PROFILES", "COMPOSE_PROJECT_NAME"):
        result.pop(name, None)
    for name in (
        "POSTGRES_SUPERUSER",
        "POSTGRES_SUPERUSER_PASSWORD",
        "DELIVERYPULSE_ETL_PASSWORD",
        "DELIVERYPULSE_READONLY_PASSWORD",
    ):
        value = values.get(name, "").strip()
        if not value or "CHANGE_ME" in value:
            raise ConfigError(f"Set the core setting {name} locally before the demo")
        result[name] = value
    result.update(
        POSTGRES_HOST="127.0.0.1",
        POSTGRES_PORT=str(port),
        DEMO_PORT=str(port),
        POSTGRES_DB=DATABASE,
        PYTHONPATH=str(ROOT),
    )
    return result


def compose(*args: str) -> list[str]:
    """Always select exactly the standalone demo file and explicit project."""
    return [
        "docker",
        "compose",
        "--project-name",
        PROJECT,
        "--file",
        str(ROOT / "docker-compose.demo.yml"),
        *args,
    ]


async def command(args: list[str], env: dict[str, str], *, show: bool = False) -> str:
    """Execute without a shell; suppress external error details that may contain credentials."""
    executable = shutil.which(args[0])
    if executable is None:
        raise RuntimeError(f"Required executable missing: {args[0]}")
    proc = await asyncio.create_subprocess_exec(
        executable,
        *args[1:],
        cwd=ROOT,
        env=env,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await proc.communicate()
    if proc.returncode:
        raise RuntimeError(f"Demo command failed ({args[0]}, exit {proc.returncode}); stop")
    output = stdout.decode()
    if show:
        print(output, end="", flush=True)
        print(stderr.decode(), end="", flush=True)
    return output


async def container_verified(env: dict[str, str]) -> None:
    """Check only nonsecret fields of the actual container before database connections."""
    raw = await command(
        [
            "docker",
            "inspect",
            "--format",
            '{{json .Mounts}}\n{{index .Config.Labels "com.docker.compose.project"}}\n'
            "{{json .HostConfig.PortBindings}}",
            CONTAINER,
        ],
        env,
    )
    mounts, project, ports = raw.strip().splitlines()
    actual = json.loads(mounts)
    published = json.loads(ports).get("5432/tcp")
    if (
        project != PROJECT
        or [m.get("Name") for m in actual if m.get("Type") == "volume"] != [VOLUME]
        or not any(
            m.get("Name") == VOLUME and m.get("Destination") == "/var/lib/postgresql"
            for m in actual
        )
        or published != [{"HostIp": "127.0.0.1", "HostPort": env["DEMO_PORT"]}]
    ):
        raise ValueError("Running demo container identity mismatch; no database writes allowed")


def database_state() -> str:
    """Allow only an empty demo or the identical demo dataset, in a read-only transaction."""
    settings = load_database_settings("etl")
    if settings.dbname != DATABASE or settings.host != "127.0.0.1":
        raise ValueError("Refusing a non-demo database connection")
    with psycopg.connect(settings.conninfo()) as conn:
        conn.execute("SET TRANSACTION READ ONLY")
        datasets = {
            row[0] for row in conn.execute("SELECT DISTINCT dataset FROM audit.pipeline_runs")
        }
        if datasets - {DATASET}:
            raise ValueError("Demo database holds another dataset; no writes allowed")
        if not datasets:
            for table in (
                "raw.orders_raw",
                "raw.events_raw",
                "raw.drivers_raw",
                "raw.shift_events_raw",
                "raw.stream_events",
                "analytics.orders",
            ):
                query = psycopg.sql.SQL("SELECT EXISTS (SELECT 1 FROM {})").format(
                    psycopg.sql.Identifier(*table.split("."))
                )
                row = conn.execute(query).fetchone()
                if row and row[0]:
                    raise ValueError("Unattributed data in demo database; no writes allowed")
    return "replay-or-retry" if datasets else "empty"


async def preflight(env: dict[str, str]) -> dict[str, Any]:
    """Read-only checks; never pull, start or stop a service."""
    checkpoint("preflight", 2.0)
    validate_layout()
    if not os.access(ROOT / "docker/postgres/init-deliverypulse.sh", os.X_OK):
        raise ValueError("PostgreSQL init script must be executable")
    await command(["docker", "info", "--format", "{{.Architecture}}"], env)
    images = await command(["docker", "image", "ls", "--digests", "--format", "{{.Digest}}"], env)
    containers = await command(compose("ps", "--quiet", "postgres"), env)
    if containers.strip():
        await container_verified(env)
    else:
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", int(env["DEMO_PORT"])))
    result = {
        "image_cached": IMAGE.split("@")[1] in images.splitlines(),
        "container_running": bool(containers.strip()),
        "raw_files_present": (DATA / "raw" / DATASET).exists(),
    }
    print(json.dumps(result), flush=True)
    print("Demo preflight passed: isolated project, volume and port; no service started")
    return result


async def run_demo(env: dict[str, str]) -> None:
    """Run sequentially, stop at the first failure, and save only nonsecret measurements."""
    started = time.monotonic()
    before = checkpoint("start", 2.0)
    context = await preflight(env)
    DATA.mkdir(parents=True, exist_ok=True)
    result: dict[str, Any] = {
        "date": datetime.now(UTC).isoformat(),
        "dataset": DATASET,
        "before_free_bytes": before,
        **context,
        "steps": {},
    }
    timed_commands = [
        ("pull", compose("pull", "postgres")),
        ("up", compose("up", "-d", "--no-deps", "--wait", "--wait-timeout", "120", "postgres")),
        *(
            (name, ["uv", "run", "--locked", "python", "-m", module, *args])
            for name, module, args in STEPS
        ),
    ]
    try:
        for name, args in timed_commands:
            checkpoint(name, 0.6 if name not in {"pull", "up"} else 2.0)
            if name == "generate":
                await container_verified(env)
                result["database_before"] = database_state()
            begun = time.monotonic()
            await command(args, env, show=name not in {"pull", "up"})
            result["steps"][name] = time.monotonic() - begun
            print(f"Demo step {name}: {result['steps'][name]:.3f} s", flush=True)
        result["status"] = "passed"
        print("Demo passed; open the separate terminal with make demo-dashboard")
    finally:
        result["total_seconds"] = time.monotonic() - started
        result["after_free_bytes"] = shutil.disk_usage(ROOT).free
        result.setdefault("status", "failed")
        path = DATA / ("run-" + datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ") + ".json")
        path.write_text(json.dumps(result, indent=2) + "\n")
        print(
            f"Demo {result['status']}: {result['total_seconds']:.3f} s; "
            f"evidence {path.relative_to(ROOT)}"
        )
    checkpoint("finished")


async def stage(name: str) -> None:
    """Dispatch user-requested actions; the disk check needs neither credentials nor Docker."""
    checkpoint(name)
    if name == "disk":
        return
    validate_layout()
    env = environment()
    if name == "preflight":
        await preflight(env)
    elif name == "run":
        # Existing configuration helpers read process variables before file defaults.
        os.environ.update({k: env[k] for k in ("POSTGRES_HOST", "POSTGRES_PORT", "POSTGRES_DB")})
        await run_demo(env)
    elif name == "down":
        await container_verified(env)
        await command(compose("stop", "postgres"), env)
        print("Demo PostgreSQL stopped; demo and main volumes kept; main container untouched")
    elif name == "reset":
        await reset_demo(env)
    elif name == "dashboard":
        await container_verified(env)
        await command(
            [
                "uv",
                "run",
                "--locked",
                "streamlit",
                "run",
                "src/dashboard/app.py",
                "--server.address",
                "127.0.0.1",
                "--server.port",
                "8502",
                "--server.headless",
                "true",
                "--server.fileWatcherType",
                "none",
                "--client.toolbarMode",
                "minimal",
                "--browser.gatherUsageStats",
                "false",
            ],
            env,
            show=True,
        )


async def reset_demo(env: dict[str, str]) -> None:
    """User-only destructive reset of the verified demo; never remove a shared image."""
    await container_verified(env)
    owner = await command(
        [
            "docker",
            "volume",
            "inspect",
            "--format",
            '{{index .Labels "com.docker.compose.project"}}',
            VOLUME,
        ],
        env,
    )
    if owner.strip() != PROJECT:
        raise ValueError("Demo volume ownership differs; no deletion allowed")
    print("Deletes ONLY deliverypulse_demo_pgdata and its container; all demo DB data is lost.")
    print("Generated demo files are archived; main data and all images are kept.")
    if input("Type DELETE DEMO: ") != "DELETE DEMO":
        raise ValueError("Demo reset cancelled")
    await command(compose("down"), env)
    await command(["docker", "volume", "rm", VOLUME], env)
    archive = DATA / ("archive-" + datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ"))
    archive.mkdir(parents=True)
    for name in ("raw", "processed", "ground_truth"):
        path = DATA / name
        if path.exists():
            path.rename(archive / name)
    print("Demo reset complete: only the demo database volume deleted; input files archived")


def main() -> int:
    """Return a nonzero status on failed checks without logging credentials."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("stage", choices=("disk", "preflight", "run", "down", "dashboard", "reset"))
    args = parser.parse_args()
    try:
        asyncio.run(stage(args.stage))
    except (RuntimeError, ValueError, OSError, psycopg.Error):
        print(
            "Demo failed: check prerequisites, isolation, disk and local credentials; "
            "no cleanup attempted"
        )
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
