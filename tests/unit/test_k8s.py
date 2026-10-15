"""Static manifests and observable stage guards, with no cluster or credentials."""

import asyncio
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import pytest

from scripts import k8s, k8s_pipeline

pytestmark = pytest.mark.unit


def image_pair() -> tuple[dict[str, Any], dict[str, Any]]:
    """Different store identifiers, identical filesystem content."""
    layers = ["sha256:" + "a" * 64, "sha256:" + "b" * 64]
    return (
        {"Id": "sha256:" + "c" * 64, "RootFS": {"Layers": layers}},
        {
            "status": {"id": "sha256:" + "d" * 64, "repoTags": ["same:tag"]},
            "info": {"imageSpec": {"rootfs": {"diff_ids": list(layers)}}},
        },
    )


def test_matching_layers_pass_despite_different_store_ids() -> None:
    host, node = image_pair()
    result = k8s.verify_image_layers(host, node)
    assert result["diff_ids"] == host["RootFS"]["Layers"]
    assert result["config_digest_checked"] is False


@pytest.mark.parametrize("change", ["different", "missing", "reordered", "empty", "absent"])
def test_layer_mismatch_keeps_host_image(change: str) -> None:
    host, node = image_pair()
    rootfs = node["info"]["imageSpec"]["rootfs"]
    if change == "different":
        rootfs["diff_ids"][1] = "sha256:" + "e" * 64
    elif change == "missing":
        rootfs["diff_ids"].pop()
    elif change == "reordered":
        rootfs["diff_ids"].reverse()
    elif change == "empty":
        rootfs["diff_ids"] = []
    else:
        rootfs.clear()
    with pytest.raises(ValueError, match="keep host image"):
        k8s.verify_image_layers(host, node)


def test_tag_only_agreement_fails() -> None:
    with pytest.raises(ValueError, match="layers missing"):
        k8s.verify_image_layers({"RepoTags": ["same:tag"]}, {"status": {"repoTags": ["same:tag"]}})


def test_exposed_config_digest_must_match() -> None:
    host, node = image_pair()
    host["ConfigDigest"] = node["status"]["id"]
    assert k8s.verify_image_layers(host, node)["config_digest_checked"] is True
    host["ConfigDigest"] = "sha256:" + "e" * 64
    with pytest.raises(ValueError, match="config digests differ"):
        k8s.verify_image_layers(host, node)


@pytest.mark.parametrize("operation", ["load", "host-image-clean"])
def test_both_stages_reject_mismatched_layers_before_cleanup(
    operation: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    host, node = image_pair()
    node["info"]["imageSpec"]["rootfs"]["diff_ids"].pop()
    calls: list[list[str]] = []

    async def fake_command(args: list[str], **kwargs: Any) -> str:
        calls.append(args)
        if "inspect" in args:
            return json.dumps([host])
        if "inspecti" in args:
            return json.dumps(node)
        return ""

    info = {"tag": "same:tag", "id": host["Id"], "bytes": 1, "loaded": True}
    monkeypatch.setattr(k8s, "evidence", lambda _: info)
    monkeypatch.setattr(k8s, "record", lambda *a: None)
    monkeypatch.setattr(k8s, "checkpoint", lambda *a, **k: 20.0)
    monkeypatch.setattr(k8s, "command", fake_command)
    with pytest.raises(ValueError, match="keep host image"):
        asyncio.run(k8s.stage(operation))
    assert not any("rm" in args for args in calls)
    if operation == "load":
        assert info["loaded"] is False


@pytest.mark.parametrize("name", ["job", "cronjob"])
def test_restricted_pods_and_bounded_storage(name: str) -> None:
    obj = k8s.manifest(name)
    job = obj["spec"] if name == "job" else obj["spec"]["jobTemplate"]["spec"]
    assert job["backoffLimit"] == 1
    assert job["activeDeadlineSeconds"] == 900
    pod = job["template"]["spec"]
    assert pod["automountServiceAccountToken"] is False
    assert pod["securityContext"]["runAsNonRoot"] is True
    assert pod["securityContext"]["runAsUser"] == 10001
    assert pod["securityContext"]["seccompProfile"]["type"] == "RuntimeDefault"
    assert not any(pod.get(k) for k in ["hostNetwork", "hostPID", "hostIPC"])
    assert all(
        set(v) == {"name", "emptyDir"} and v["emptyDir"]["sizeLimit"] for v in pod["volumes"]
    )
    for container in pod["containers"] + pod["initContainers"]:
        security = container["securityContext"]
        assert security["readOnlyRootFilesystem"] is True
        assert security["allowPrivilegeEscalation"] is False
        assert not security.get("privileged", False)
        assert security["capabilities"] == {"drop": ["ALL"]}
        assert container["imagePullPolicy"] == "Never"
        assert container["image"] == "deliverypulse-pipeline:unbuilt"
        for bound in ["requests", "limits"]:
            assert set(container["resources"][bound]) == {"cpu", "memory", "ephemeral-storage"}
    assert "envFrom" not in pod["initContainers"][0]
    assert pod["containers"][0]["envFrom"][1] == {"secretRef": {"name": "deliverypulse-db"}}
    if name == "cronjob":
        assert obj["spec"]["suspend"] is True
        assert obj["spec"]["concurrencyPolicy"] == "Forbid"
        assert obj["spec"]["timeZone"] == "Etc/UTC"


def test_namespace_node_pin_and_image_security() -> None:
    assert (
        k8s.manifest("namespace")["metadata"]["labels"]["pod-security.kubernetes.io/enforce"]
        == "restricted"
    )
    config = k8s.manifest("kind-config")
    assert config["networking"]["apiServerAddress"] == "127.0.0.1"
    assert len(config["nodes"]) == 1
    assert config["nodes"][0]["image"] == (
        "kindest/node:v1.37.0@sha256:"
        "a1ed56cfb0e7b93589bdf97c8cd566405a265939e3620fc4f5de89adff580ae5"
    )
    dockerfile = (k8s.ROOT / "docker/pipeline/Dockerfile").read_text()
    assert "USER 10001:10001" in dockerfile
    assert "--locked --no-dev --no-install-project" in dockerfile
    for line in dockerfile.splitlines():
        if line.startswith("FROM "):
            assert "@sha256:" in line
        if line.startswith("COPY "):
            assert ".env" not in line and "COPY . " not in line
    ignored = (k8s.ROOT / ".dockerignore").read_text().splitlines()
    assert all(p in ignored for p in [".env", ".env.*", "data/", ".git/"])


@pytest.mark.parametrize(
    "kind_version,kube_version",
    [
        ("kind v0.32.0", "Client Version: v1.37.1"),
        ("kind v0.33.0", "Client Version: v1.36.1"),
    ],
)
def test_preflight_rejects_wrong_tools(kind_version: str, kube_version: str) -> None:
    with pytest.raises(ValueError):
        k8s.check_versions(kind_version, kube_version)


def test_versions_and_explicit_binary(monkeypatch: pytest.MonkeyPatch) -> None:
    k8s.check_versions("kind v0.33.0 go1.27.0 darwin/arm64", "Client Version: v1.37.1")
    monkeypatch.setenv("KUBECTL", "/chosen/kubectl")
    assert k8s.kubectl("get", "pods")[0] == "/chosen/kubectl"
    assert "kind-deliverypulse" in k8s.kubectl("get", "pods")


def test_disk_guard_blocks_build_before_mutation(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "scripts.k8s.shutil.disk_usage", lambda _: SimpleNamespace(free=13 * k8s.GIB)
    )
    with pytest.raises(RuntimeError, match="Disk checkpoint failed"):
        asyncio.run(k8s.stage("build"))


def test_render_requires_verified_current_image(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(k8s, "image_tag", lambda: "deliverypulse-pipeline:abc")
    info = {"tag": "deliverypulse-pipeline:abc", "loaded": False}
    monkeypatch.setattr(k8s, "evidence", lambda _: info)
    with pytest.raises(ValueError, match="Load and verify"):
        k8s.rendered_job("pipeline")
    info["loaded"] = True
    job = k8s.rendered_job("connectivity")
    pod = job["spec"]["template"]["spec"]
    assert "initContainers" not in pod
    assert pod["containers"][0]["image"] == info["tag"]
    assert job["spec"]["backoffLimit"] == 0


def test_replay_detects_changed_content(monkeypatch: pytest.MonkeyPatch) -> None:
    values = iter(
        [
            json.dumps({"orders": {"rows": 1, "sha256": "before"}}),
            "pipeline succeeded",
            json.dumps({"orders": {"rows": 1, "sha256": "after"}}),
        ]
    )

    async def fake_job(*args: Any, **kwargs: Any) -> str:
        return next(values)

    monkeypatch.setattr(k8s, "run_job", fake_job)
    with pytest.raises(ValueError, match="Analytics rows differ"):
        asyncio.run(k8s.pipeline())


def test_dataset_identity_and_tamper_rejection(tmp_path: Path) -> None:
    rows = []
    for sequence in range(1, 4):
        folder = tmp_path / f"batch-{sequence}"
        folder.mkdir()
        (folder / "orders.csv").write_bytes(b"synthetic\n")
        (folder / "batch.json").write_text(
            json.dumps(
                {
                    "batch_sequence": sequence,
                    "batch_id": str(sequence),
                    "files": {"orders.csv": {"sha256": hashlib.sha256(b"synthetic\n").hexdigest()}},
                }
            )
        )
        rows.append((k8s_pipeline.DATASET, sequence, str(sequence)))
    conn = MagicMock()
    conn.execute.return_value.fetchall.return_value = rows
    k8s_pipeline.verify_identity(conn, tmp_path)
    conn.execute.assert_any_call("SET TRANSACTION READ ONLY")
    conn.execute.return_value.fetchall.return_value = rows[:-1]
    with pytest.raises(ValueError, match="identity differs"):
        k8s_pipeline.verify_identity(conn, tmp_path)
    (tmp_path / "batch-1/orders.csv").write_bytes(b"changed\n")
    with pytest.raises(ValueError, match="hash mismatch"):
        k8s_pipeline.verify_identity(conn, tmp_path)


def test_secret_selects_only_etl_credential(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[list[str], Any]] = []
    settings = SimpleNamespace(port=5432, dbname="synthetic", password=object())
    monkeypatch.setattr("src.config.load_database_settings", lambda role: settings)

    async def fake_command(arguments: list[str], document: Any = None, **kwargs: Any) -> str:
        calls.append((arguments, document))
        return ""

    async def fake_job(operation: str) -> str:
        assert operation == "connectivity"
        return "passed"

    monkeypatch.setattr(k8s, "command", fake_command)
    monkeypatch.setattr(k8s, "run_job", fake_job)
    monkeypatch.setattr(k8s, "rendered_job", lambda _: k8s.manifest("job"))
    monkeypatch.setattr(k8s, "record", lambda *args: None)
    asyncio.run(k8s.create_secret())
    secret_calls = [(args, doc) for args, doc in calls if doc["kind"] == "Secret"]
    assert len(secret_calls) == 1
    args, document = secret_calls[0]
    assert "--server-side" in args
    assert document["stringData"] == {"DELIVERYPULSE_ETL_PASSWORD": settings.password}
    assert "-" in args  # credentials travel on stdin, not in process arguments
    config = next(doc for _, doc in calls if doc["kind"] == "ConfigMap")
    assert config["data"]["POSTGRES_HOST"] == "host.docker.internal"


def test_identity_failure_prevents_every_pipeline_write(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("sys.argv", ["pipeline", "pipeline"])
    monkeypatch.setattr(k8s_pipeline, "load_database_settings", lambda _: MagicMock())
    monkeypatch.setattr("scripts.k8s_pipeline.psycopg.connect", lambda *a, **k: MagicMock())

    def wrong_identity(*args: Any) -> None:
        raise ValueError("dataset mismatch")

    monkeypatch.setattr(k8s_pipeline, "verify_identity", wrong_identity)
    writer = MagicMock(side_effect=AssertionError("writes must not run"))
    for module in ["transform", "load_postgres", "metrics", "quality"]:
        monkeypatch.setattr(f"scripts.k8s_pipeline.{module}.main", writer)
    assert k8s_pipeline.main() == 1
    writer.assert_not_called()


def test_live_job_failure_is_not_reported_as_success(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(k8s, "checkpoint", lambda *a, **k: 20.0)
    monkeypatch.setattr(k8s, "rendered_job", lambda *a: {})
    monkeypatch.setattr(k8s, "record", lambda *a: None)
    commands = []

    async def fake_command(args: list[str], document: Any = None, **kwargs: Any) -> str:
        commands.append(args)
        if "create" in args:
            return json.dumps({"metadata": {"name": "deliverypulse-test"}})
        if "get" in args:
            return json.dumps({"status": {"conditions": [{"type": "Failed", "status": "True"}]}})
        return "failed cleanly\n"

    monkeypatch.setattr(k8s, "command", fake_command)
    with pytest.raises(RuntimeError, match="Unexpected Job result"):
        asyncio.run(k8s.run_job("pipeline"))
    assert not any("delete" in args for args in commands)
