"""Demo safety and observable behaviour, with external commands replaced in every test."""

import asyncio
import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import pytest

from scripts import demo
from src.config import PLACEHOLDER
from src.groundtruth.manifest import load_manifest
from src.ingestion.generate_data import main as generate_main

pytestmark = pytest.mark.unit


@pytest.fixture
def layout(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Use a private copy of the config; no test can mutate project data."""
    source = demo.ROOT / "docker-compose.demo.yml"
    (tmp_path / source.name).write_text(source.read_text())
    monkeypatch.setattr(demo, "ROOT", tmp_path)
    monkeypatch.setattr(demo, "DATA", tmp_path / "data/demo")
    return tmp_path


@pytest.mark.parametrize("change", ["volume", "service", "port", "mount", "database", "image"])
def test_layout_rejects_escape_to_main(change: str, layout: Path) -> None:
    path = layout / "docker-compose.demo.yml"
    obj = json.loads(path.read_text())
    service = obj["services"]["postgres"]
    if change == "volume":
        obj["volumes"]["demo_pgdata"]["name"] = "deliverypulse_pgdata"
    elif change == "service":
        obj["services"]["kafka"] = {}
    elif change == "port":
        service["ports"] = ["0.0.0.0:5432:5432"]
    elif change == "mount":
        service["volumes"].append("deliverypulse_pgdata:/other")
    elif change == "database":
        service["environment"]["POSTGRES_DB"] = "deliverypulse"
    else:
        service["image"] = "postgres:latest"
    path.write_text(json.dumps(obj))
    with pytest.raises(ValueError, match="isolation"):
        demo.validate_layout()


def test_approved_layout_and_symlink_guard(layout: Path) -> None:
    demo.validate_layout()
    (layout / "data").mkdir()
    (layout / "data/demo").symlink_to(layout / "elsewhere")
    with pytest.raises(ValueError, match="symbolic"):
        demo.validate_layout()


def test_credentials_are_private_and_endpoint_is_forced(monkeypatch: pytest.MonkeyPatch) -> None:
    values = {
        name: "synthetic-test-value"
        for name in (
            "POSTGRES_SUPERUSER",
            "POSTGRES_SUPERUSER_PASSWORD",
            "DELIVERYPULSE_ETL_PASSWORD",
            "DELIVERYPULSE_READONLY_PASSWORD",
        )
    }
    values.update(POSTGRES_DB="main", POSTGRES_PORT="5432", POSTGRES_HOST="remote")
    monkeypatch.setattr(demo, "read_environment", lambda: values)
    monkeypatch.setenv("DEMO_PORT", "55432")
    monkeypatch.setenv("COMPOSE_FILE", "docker-compose.kafka.yml")
    env = demo.environment()
    assert env["POSTGRES_DB"] == "deliverypulse_demo"
    assert env["POSTGRES_HOST"] == "127.0.0.1"
    assert env["POSTGRES_PORT"] == "55432"
    assert "COMPOSE_FILE" not in env
    monkeypatch.setenv("DEMO_PORT", "5432")
    with pytest.raises(ValueError, match="distinct"):
        demo.environment()
    monkeypatch.setenv("DEMO_PORT", "55432")
    values["DELIVERYPULSE_ETL_PASSWORD"] = PLACEHOLDER
    with pytest.raises(ValueError, match="core setting"):
        demo.environment()


def test_low_disk_prevents_any_external_command(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "scripts.demo.shutil.disk_usage", lambda _: SimpleNamespace(free=10 * demo.GIB)
    )
    unexpected = MagicMock(side_effect=AssertionError("external command reached"))
    monkeypatch.setattr(demo, "command", unexpected)
    with pytest.raises(RuntimeError, match="checkpoint"):
        asyncio.run(demo.stage("run"))
    unexpected.assert_not_called()


@pytest.mark.parametrize("mismatch", [None, "volume", "project", "port"])
def test_actual_container_must_match_before_db_access(
    mismatch: str | None, monkeypatch: pytest.MonkeyPatch
) -> None:
    mounts = [{"Type": "volume", "Name": demo.VOLUME, "Destination": "/var/lib/postgresql"}]
    project = demo.PROJECT
    ports = {"5432/tcp": [{"HostIp": "127.0.0.1", "HostPort": "55432"}]}
    if mismatch == "volume":
        mounts[0]["Name"] = "deliverypulse_pgdata"
    elif mismatch == "project":
        project = "deliverypulse"
    elif mismatch == "port":
        ports["5432/tcp"][0]["HostPort"] = "5432"

    async def inspect(args: list[str], env: dict[str, str]) -> str:
        assert args[-1] == demo.CONTAINER
        assert ".Config.Env" not in " ".join(args)
        return "\n".join([json.dumps(mounts), project, json.dumps(ports)])

    monkeypatch.setattr(demo, "command", inspect)
    if mismatch:
        with pytest.raises(ValueError, match="identity mismatch"):
            asyncio.run(demo.container_verified({"DEMO_PORT": "55432"}))
    else:
        asyncio.run(demo.container_verified({"DEMO_PORT": "55432"}))


@pytest.mark.parametrize(
    "datasets,rows,expected",
    [
        ([], False, "empty"),
        ([(demo.DATASET,)], True, "replay-or-retry"),
        ([("main-dataset",)], True, "error"),
        ([("seed-42_orders-5000_cfg-99b76f46",)], True, "error"),
        ([], True, "error"),
    ],
)
def test_other_dataset_or_unattributed_rows_are_rejected(
    datasets: list[tuple[str]], rows: bool, expected: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    conn = MagicMock()
    conn.execute.return_value.__iter__.return_value = iter(datasets)
    conn.execute.return_value.fetchone.return_value = (rows,)
    context = MagicMock()
    context.__enter__.return_value = conn
    monkeypatch.setattr("scripts.demo.psycopg.connect", lambda _: context)
    settings = MagicMock(dbname=demo.DATABASE, host="127.0.0.1")
    monkeypatch.setattr(demo, "load_database_settings", lambda _: settings)
    if expected == "error":
        with pytest.raises(ValueError, match="no writes"):
            demo.database_state()
    else:
        assert demo.database_state() == expected
    assert conn.execute.call_args_list[0].args == ("SET TRANSACTION READ ONLY",)
    assert not any("INSERT" in str(c) or "UPDATE" in str(c) for c in conn.execute.call_args_list)


def test_failed_step_stops_pipeline_and_records_failure(
    layout: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[list[str]] = []

    async def preflight(env: dict[str, str]) -> dict[str, Any]:
        return {"image_cached": True}

    async def verify(env: dict[str, str]) -> None:
        calls.append(["identity"])

    async def command(args: list[str], env: dict[str, str], **kwargs: Any) -> str:
        calls.append(args)
        if "src.transformations.transform" in args:
            raise RuntimeError("synthetic failure")
        return ""

    monkeypatch.setattr(demo, "preflight", preflight)
    monkeypatch.setattr(demo, "container_verified", verify)
    monkeypatch.setattr(demo, "database_state", lambda: "empty")
    monkeypatch.setattr(demo, "checkpoint", lambda *a: 20 * demo.GIB)
    monkeypatch.setattr(demo, "command", command)
    with pytest.raises(RuntimeError, match="synthetic failure"):
        asyncio.run(demo.run_demo({}))
    assert not any("src.database.load_postgres" in c or "stop" in c or "delete" in c for c in calls)
    assert calls.index(["identity"]) < next(
        i for i, c in enumerate(calls) if "src.ingestion.generate_data" in c
    )
    record = json.loads(next((layout / "data/demo").glob("run-*.json")).read_text())
    assert record["status"] == "failed"
    assert "load" not in record["steps"]


def test_down_only_stops_verified_demo_and_keeps_volumes(
    layout: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[list[str]] = []

    async def verify(env: dict[str, str]) -> None:
        calls.append(["verified"])

    async def command(args: list[str], env: dict[str, str], **kwargs: Any) -> str:
        calls.append(args)
        return ""

    monkeypatch.setattr(demo, "checkpoint", lambda *a: 20 * demo.GIB)
    monkeypatch.setattr(demo, "environment", lambda: {})
    monkeypatch.setattr(demo, "container_verified", verify)
    monkeypatch.setattr(demo, "command", command)
    asyncio.run(demo.stage("down"))
    assert calls == [["verified"], demo.compose("stop", "postgres")]
    assert "--volumes" not in calls[1]


def test_readme_lists_actual_demo_steps() -> None:
    readme = (demo.ROOT / "README.md").read_text()
    for name, _, _ in demo.STEPS:
        assert f"`{name}`" in readme
    assert "deliverypulse_demo_pgdata" in readme
    assert "data/demo/" in readme


def test_demo_reports_and_inputs_stay_under_demo_directory() -> None:
    steps = {name: args for name, _, args in demo.STEPS}
    assert steps["quality"] == ("--output", "data/demo/quality-latest.json")
    for name in ("generate", "transform", "load", "reconcile", "analytics"):
        args = steps[name]
        assert args[args.index("--raw-dir") + 1] == "data/demo/raw"
    for name in ("generate", "reconcile"):
        args = steps[name]
        assert args[args.index("--ground-truth-dir") + 1] == "data/demo/ground_truth"
    args = steps["transform"]
    assert args[args.index("--processed-dir") + 1] == "data/demo/processed"


def test_demo_fleet_generates_without_capacity_warning(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    args = list(next(args for name, _, args in demo.STEPS if name == "generate"))
    assert args[args.index("--drivers") + 1] == "500"
    args[args.index("--raw-dir") + 1] = str(tmp_path / "raw")
    args[args.index("--ground-truth-dir") + 1] = str(tmp_path / "ground_truth")
    assert generate_main(args) == 0
    manifest = load_manifest(tmp_path / "ground_truth" / demo.DATASET / "defect_manifest.json")
    assert manifest.config["n_orders"] == 5000
    assert manifest.config["n_drivers"] == 500
    assert manifest.config["seed"] == 42
    summary = manifest.simulation_summary
    assert summary["orders_cancelled_no_driver"] / summary["orders_total"] < 0.05
    assert not any("no-free-driver cancellations exceed" in r.message for r in caplog.records)


@pytest.mark.parametrize(
    "owner,answer",
    [("deliverypulse", "DELETE DEMO"), (demo.PROJECT, "no"), (demo.PROJECT, "DELETE DEMO")],
)
def test_reset_requires_owned_volume_and_exact_confirmation(
    owner: str, answer: str, layout: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[list[str]] = []

    async def verify(env: dict[str, str]) -> None:
        return None

    async def command(args: list[str], env: dict[str, str]) -> str:
        calls.append(args)
        return owner if "inspect" in args else ""

    monkeypatch.setattr(demo, "container_verified", verify)
    monkeypatch.setattr(demo, "command", command)
    monkeypatch.setattr("builtins.input", lambda _: answer)
    raw = layout / "data/demo/raw"
    raw.mkdir(parents=True)
    (raw / "evidence.txt").write_text("keep")
    if owner != demo.PROJECT or answer != "DELETE DEMO":
        with pytest.raises(ValueError):
            asyncio.run(demo.reset_demo({}))
        assert not any("rm" in c or "down" in c for c in calls)
        assert raw.exists()
    else:
        asyncio.run(demo.reset_demo({}))
        assert calls[-2:] == [demo.compose("down"), ["docker", "volume", "rm", demo.VOLUME]]
        assert next((layout / "data/demo").glob("archive-*/raw/evidence.txt")).read_text() == "keep"
