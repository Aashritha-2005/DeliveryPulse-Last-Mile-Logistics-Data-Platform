"""Explicit, user-run Kubernetes stages; no implicit build, deploy or cleanup."""

import argparse
import asyncio
import hashlib
import json
import os
import re
import shutil
import signal
import time
import uuid
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
STATE = ROOT / "data/k8s"
GIB = 1024**3
CLUSTER = "deliverypulse"
NODE = "deliverypulse-control-plane"
LIMITS = {"preflight": 13.5, "build": 13.5, "up": 12.8, "run": 12.5, "replay": 12.5}


def checkpoint(stage: str, minimum: float | None = None) -> float:
    """Refuse a stage below its budget; 11 GiB is the cancellation threshold."""
    free = min(shutil.disk_usage(ROOT).free, shutil.disk_usage("/").free) / GIB
    required = LIMITS.get(stage, 11.0) if minimum is None else minimum
    print(f"Disk checkpoint {stage}: {free:.3f} GiB free; require {required:.3f} GiB", flush=True)
    if free < required:
        raise RuntimeError("Disk checkpoint failed; stop and reclaim space; keep volumes")
    return free


def manifest(name: str) -> dict[str, Any]:
    """Read JSON-formatted YAML without an additional parser dependency."""
    value: dict[str, Any] = json.loads((ROOT / "k8s" / f"{name}.yaml").read_text())
    return value


def record(name: str, value: Any) -> None:
    """Save non-secret evidence under the ignored data directory."""
    STATE.mkdir(parents=True, exist_ok=True, mode=0o700)
    (STATE / f"{name}.json").write_text(json.dumps(value, indent=2) + "\n")


def evidence(name: str) -> Any:
    """Read evidence produced by a previous successful stage."""
    return json.loads((STATE / f"{name}.json").read_text())


def kubectl(*arguments: str) -> list[str]:
    """Always use the selected binary and dedicated cluster context."""
    return [
        os.environ.get("KUBECTL", "/opt/homebrew/bin/kubectl"),
        "--kubeconfig",
        str(STATE / "kubeconfig"),
        "--context",
        "kind-deliverypulse",
        "--namespace",
        CLUSTER,
        *arguments,
    ]


def kind(*arguments: str) -> list[str]:
    """Use the overridable kind binary."""
    return [os.environ.get("KIND", "kind"), *arguments]


async def command(
    arguments: list[str], document: Any = None, timeout: float = 900, *, monitor: bool = True
) -> str:
    """Run an argument vector, suppress errors containing credentials, monitor host free space.

    Killing a client does not undo server-side work. On cancellation, the operator must stop
    the active Job/build/cluster and check Docker storage before proceeding.
    """
    process = await asyncio.create_subprocess_exec(
        *arguments,
        cwd=ROOT,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        start_new_session=True,
    )
    payload = json.dumps(document).encode() if document is not None else None
    communication = asyncio.create_task(process.communicate(payload))
    deadline = time.monotonic() + timeout
    try:
        while not communication.done():
            await asyncio.wait({communication}, timeout=1)
            if (
                monitor
                and min(shutil.disk_usage(ROOT).free, shutil.disk_usage("/").free) < 11 * GIB
            ):
                raise RuntimeError("11 GiB cancellation point reached; stop the server-side work")
            if time.monotonic() > deadline:
                raise RuntimeError("Command timed out; inspect status before any next stage")
        stdout, _stderr = await communication
        if process.returncode:
            raise RuntimeError("Command failed; no next stage is authorized (use k8s-status/logs)")
        return stdout.decode()
    finally:
        if process.returncode is None:
            os.killpg(process.pid, signal.SIGTERM)
            try:
                await asyncio.wait_for(process.wait(), 5)
            except TimeoutError:
                os.killpg(process.pid, signal.SIGKILL)
                await process.wait()
        await communication


def check_versions(kind_output: str, kube_output: str) -> None:
    """Reject bundled kubectl and unsupported kind minor versions."""
    if not re.search(r"\bkind v0\.33\.\d+\b", kind_output):
        raise ValueError("kind must be v0.33.x")
    if not re.search(r"Client Version: v1\.37\.\d+\b", kube_output):
        raise ValueError("kubectl must be v1.37.x; select the Homebrew binary")


def image_tag() -> str:
    """Tag the local image by all its build inputs, never by latest."""
    files = [
        ROOT / p
        for p in (
            "pyproject.toml",
            "uv.lock",
            "docker/pipeline/Dockerfile",
            "scripts/k8s_pipeline.py",
            "scripts/__init__.py",
            ".dockerignore",
        )
    ]
    files += list((ROOT / "src").rglob("*.py")) + list((ROOT / "sql").rglob("*.sql"))
    digest = hashlib.sha256()
    for path in sorted(files):
        digest.update(str(path.relative_to(ROOT)).encode() + b"\0" + path.read_bytes())
    return f"deliverypulse-pipeline:{digest.hexdigest()[:20]}"


def verify_image_layers(host: dict[str, Any], node: dict[str, Any]) -> dict[str, Any]:
    """Compare ordered uncompressed layers, not store-specific image identifiers or tags."""
    layers = host.get("RootFS", {}).get("Layers")
    node_layers = node.get("info", {}).get("imageSpec", {}).get("rootfs", {}).get("diff_ids")
    for values in (layers, node_layers):
        if (
            not isinstance(values, list)
            or not values
            or not all(
                isinstance(value, str) and re.fullmatch(r"sha256:[0-9a-f]{64}", value)
                for value in values
            )
        ):
            raise ValueError("Image layers missing or invalid; keep host image")
    if layers != node_layers:
        raise ValueError("Node image layers differ from host build; keep host image")
    host_config = host.get("ConfigDigest")
    node_config = node.get("status", {}).get("id")
    config_checked = host_config is not None and node_config is not None
    if config_checked and (
        not isinstance(host_config, str)
        or not re.fullmatch(r"sha256:[0-9a-f]{64}", host_config)
        or host_config != node_config
    ):
        raise ValueError("Image config digests differ or are invalid; keep host image")
    return {
        "diff_ids": layers,
        "config_digest_checked": config_checked,
        "host_config_digest": host_config,
        "node_config_digest": node_config,
    }


async def inspect_image_pair(tag: str) -> tuple[dict[str, Any], dict[str, Any]]:
    """Read the local host and node image metadata without exporting or changing either."""
    host = json.loads(await command(["docker", "image", "inspect", tag]))[0]
    node = json.loads(await command(["docker", "exec", NODE, "crictl", "inspecti", tag]))
    return host, node


async def verify_loaded_image(info: dict[str, Any]) -> dict[str, Any]:
    """Anchor the host tag to the saved build, then verify matching node content."""
    host, node = await inspect_image_pair(info["tag"])
    if host.get("Id") != info["id"]:
        raise ValueError("Host tag no longer identifies the recorded build; keep host image")
    result = verify_image_layers(host, node)
    print(f"Node image matches host build: {len(result['diff_ids'])} ordered rootfs layers")
    print(
        "Config digest comparison: "
        + ("matched" if result["config_digest_checked"] else "unavailable on one or both sides")
    )
    return result


def rendered_job(operation: str, credential_name: str = "deliverypulse-db") -> dict[str, Any]:
    """Render a unique Job with a verified local image and no plaintext credentials."""
    image = evidence("image")
    if not image.get("loaded") or image["tag"] != image_tag():
        raise ValueError("Load and verify the current build before deploying")
    job = manifest("job")
    pod = job["spec"]["template"]["spec"]
    for container in pod["initContainers"] + pod["containers"]:
        container["image"] = image["tag"]
    pod["containers"][0]["args"] = [operation]
    pod["containers"][0]["envFrom"][1]["secretRef"]["name"] = credential_name
    if operation != "pipeline":
        pod.pop("initContainers")
        job["spec"]["activeDeadlineSeconds"] = 120
        job["spec"]["backoffLimit"] = 0
    return job


async def run_job(
    operation: str, credential_name: str = "deliverypulse-db", expect_failure: bool = False
) -> str:
    """Create, observe and log one Job; retain evidence before deleting temporary pod storage."""
    checkpoint("run")
    job = json.loads(
        await command(
            kubectl("create", "-f", "-", "-o", "json"), rendered_job(operation, credential_name)
        )
    )
    name = job["metadata"]["name"]
    record("last-job", name)
    begun = time.monotonic()
    samples = []
    sampled = 0.0
    try:
        while True:
            job = json.loads(await command(kubectl("get", "job", name, "-o", "json")))
            conditions = job.get("status", {}).get("conditions", [])
            finished = {c["type"] for c in conditions if c["status"] == "True"}
            if finished & {"Complete", "Failed"}:
                break
            if time.monotonic() - begun > 960:
                raise RuntimeError("Job timed out; stop and inspect it")
            if time.monotonic() - sampled > 10:
                samples.append(
                    json.loads(
                        await command(
                            [
                                "docker",
                                "stats",
                                "--no-stream",
                                "--format",
                                "{{json .}}",
                                NODE,
                            ],
                            timeout=30,
                        )
                    )
                )
                sampled = time.monotonic()
            await asyncio.sleep(2)
    except RuntimeError as exc:
        if "cancellation point" in str(exc):
            await command(kubectl("delete", "job", name, "--wait=true"), monitor=False)
        raise
    logs = await command(kubectl("logs", f"job/{name}", "--all-containers=true"))
    print(logs, end="")
    record(
        name,
        {
            "status": job["status"],
            "seconds": time.monotonic() - begun,
            "logs": logs,
            "node_samples": samples,
        },
    )
    if ("Failed" in finished) != expect_failure:
        raise RuntimeError("Unexpected Job result; stop and inspect it")
    # Only successful, evidenced jobs (or the expected negative test) are removed.
    await command(kubectl("delete", "job", name, "--wait=true"), monitor=False)
    return logs


async def pipeline(replay: bool = False) -> None:
    """Require analytics equality before and after a replay of the verified source dataset."""
    before = json.loads(await run_job("snapshot"))
    if not before:
        raise ValueError("No analytics tables found")
    if replay and before != evidence("baseline"):
        raise ValueError("Database changed since first run; stop and investigate")
    await run_job("pipeline")
    after = json.loads(await run_job("snapshot"))
    if before != after:
        raise ValueError("Analytics rows differ after pipeline replay")
    record("baseline", after)
    print("Kubernetes replay: analytics counts and content hashes unchanged")


async def create_secret() -> None:
    """User-only stage: select ETL credentials in memory; never serialize the whole .env."""
    from src.config import load_database_settings

    settings = load_database_settings("etl")
    await command(kubectl("apply", "-f", "-"), manifest("namespace"))
    config = manifest("configmap")
    config["data"].update(POSTGRES_PORT=str(settings.port), POSTGRES_DB=settings.dbname)
    await command(kubectl("apply", "-f", "-"), config)
    secret = {
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {"name": "deliverypulse-db", "namespace": CLUSTER},
        "type": "Opaque",
        "stringData": {"DELIVERYPULSE_ETL_PASSWORD": settings.password},
    }
    await command(kubectl("apply", "--server-side", "-f", "-"), secret)
    cron = manifest("cronjob")
    cron["spec"]["jobTemplate"]["spec"] = rendered_job("pipeline")["spec"]
    await command(kubectl("apply", "-f", "-"), cron)
    await run_job("connectivity")
    record("connectivity", {"passed": True})
    print("ETL Secret created; values not shown; connectivity passed; CronJob suspended")


async def negative_test() -> None:
    """A separate wrong credential must fail without disclosure or modifying the real Secret."""
    checkpoint("wrong-credential-test", 12.5)
    name = "deliverypulse-negative-" + uuid.uuid4().hex[:12]
    value = uuid.uuid4().hex
    secret = {
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {"name": name},
        "stringData": {"DELIVERYPULSE_ETL_PASSWORD": value},
    }
    await command(kubectl("create", "-f", "-"), secret)
    try:
        logs = await run_job("connectivity", name, expect_failure=True)
        if value in logs or "ERROR: Kubernetes pipeline failed" not in logs:
            raise ValueError("Wrong-password failure was not clean")
    finally:
        await command(kubectl("delete", "secret", name))


async def stage(name: str) -> None:
    """Execute exactly one explicit stage, with a fresh free-space check."""
    recovery = name in {"down", "cache-clean", "status", "logs", "abort"}
    checkpoint(name, 0 if recovery else None)
    if name == "preflight":
        k = await command(kind("version"))
        v = await command(
            [os.environ.get("KUBECTL", "/opt/homebrew/bin/kubectl"), "version", "--client"]
        )
        print(k + v, end="")
        check_versions(k, v)
        print(
            await command(
                [
                    "docker",
                    "info",
                    "--format",
                    "Docker architecture={{.Architecture}} memory_bytes={{.MemTotal}}",
                ]
            ),
            end="",
        )
        print("Kubernetes preflight: passed (no cluster created)")
    elif name == "build":
        tag = image_tag()
        print(
            await command(["docker", "build", "-f", "docker/pipeline/Dockerfile", "-t", tag, "."]),
            end="",
        )
        info = json.loads(await command(["docker", "image", "inspect", tag]))[0]
        record("image", {"tag": tag, "id": info["Id"], "bytes": info["Size"], "loaded": False})
        print(json.dumps(evidence("image")))
    elif name == "cache-clean":
        if input("Delete unused Docker build cache (no volumes)? Type DELETE: ") != "DELETE":
            raise ValueError("Cancelled")
        print(await command(["docker", "builder", "prune", "--force"], monitor=False), end="")
    elif name == "up":
        STATE.mkdir(parents=True, exist_ok=True, mode=0o700)
        clusters = (await command(kind("get", "clusters"))).splitlines()
        if CLUSTER in clusters:
            raise ValueError("Cluster already exists; inspect before continuing")
        begun = time.monotonic()
        print(
            await command(
                kind(
                    "create",
                    "cluster",
                    "--config",
                    "k8s/kind-config.yaml",
                    "--kubeconfig",
                    str(STATE / "kubeconfig"),
                    "--wait",
                    "180s",
                )
            ),
            end="",
        )
        (STATE / "kubeconfig").chmod(0o600)
        record("cluster", {"create_seconds": time.monotonic() - begun})
        record("connectivity", {"passed": False})
        print(json.dumps(evidence("cluster")))
        print(await command(["docker", "exec", NODE, "df", "-k", "/var/lib/containerd"]))
    elif name == "image-info":
        tag = evidence("image")["tag"]
        host, node = await inspect_image_pair(tag)
        print(
            json.dumps(
                {
                    "tag": tag,
                    "host": {
                        "Id": host.get("Id"),
                        "RootFS": host.get("RootFS"),
                        "ConfigDigest": host.get("ConfigDigest"),
                        "Identity": host.get("Identity"),
                        "Descriptor": host.get("Descriptor"),
                        "fields": sorted(host),
                    },
                    "node": {
                        "status.id": node.get("status", {}).get("id"),
                        "status.repoTags": node.get("status", {}).get("repoTags"),
                        "rootfs": node.get("info", {}).get("imageSpec", {}).get("rootfs"),
                    },
                },
                indent=2,
            )
        )
    elif name == "load":
        info = evidence("image")
        checkpoint(name, 11 + 2 * info["bytes"] / GIB + 0.3)
        info["loaded"] = False
        record("image", info)
        await command(kind("load", "docker-image", info["tag"], "--name", CLUSTER))
        info["verification"] = await verify_loaded_image(info)
        info["loaded"] = True
        record("image", info)
    elif name == "host-image-clean":
        info = evidence("image")
        if not info["loaded"]:
            raise ValueError("Verify node image first")
        await verify_loaded_image(info)
        print(await command(["docker", "image", "rm", info["tag"]]), end="")
    elif name == "secret":
        checkpoint("secret-probe", 12.5)
        await create_secret()
    elif name in {"run", "replay"}:
        if not evidence("connectivity")["passed"]:
            raise ValueError("Connectivity preflight is required")
        await pipeline(replay=name == "replay")
    elif name == "status":
        print(
            await command(kubectl("get", "jobs,pods,cronjobs", "-o", "wide"), monitor=False), end=""
        )
        print(
            await command(
                kubectl("get", "secret", "deliverypulse-db", "-o", "name"), monitor=False
            ),
            end="",
        )
    elif name == "abort":
        last = evidence("last-job")
        print(await command(kubectl("delete", "job", last, "--wait=true"), monitor=False))
    elif name == "logs":
        last = evidence("last-job")
        saved = STATE / f"{last}.json"
        print(
            json.loads(saved.read_text())["logs"]
            if saved.exists()
            else await command(
                kubectl("logs", f"job/{last}", "--all-containers=true"), monitor=False
            )
        )
    elif name == "stats":
        stats = {
            "image": evidence("image"),
            "cluster": evidence("cluster"),
            "host_free_bytes": shutil.disk_usage(ROOT).free,
            "node_sample": await command(
                ["docker", "stats", "--no-stream", "--format", "{{json .}}", NODE]
            ),
            "node_disk": await command(["docker", "exec", NODE, "df", "-k", "/"]),
            "jobs": {
                p.stem: json.loads(p.read_text())
                for p in sorted(STATE.glob("deliverypulse-*.json"))
            },
        }
        record("measurements", stats)
        print(json.dumps(stats, indent=2))
    elif name == "down":
        if (
            input(
                "Delete deliverypulse kind cluster, Jobs and Secrets; "
                "keep Compose volumes? Type DELETE: "
            )
            != "DELETE"
        ):
            raise ValueError("Cancelled")
        print(await command(kind("delete", "cluster", "--name", CLUSTER), monitor=False), end="")
    checkpoint(name + "-after", 0 if recovery else None)


def main() -> int:
    """Run one stage; any failure ends the command and blocks the next stage."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "stage",
        choices=[
            "preflight",
            "build",
            "cache-clean",
            "up",
            "load",
            "image-info",
            "host-image-clean",
            "secret",
            "run",
            "replay",
            "status",
            "logs",
            "stats",
            "down",
            "abort",
        ],
    )
    try:
        asyncio.run(stage(parser.parse_args().stage))
    except (RuntimeError, ValueError, OSError, KeyError) as exc:
        print(f"STOP: {exc}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
