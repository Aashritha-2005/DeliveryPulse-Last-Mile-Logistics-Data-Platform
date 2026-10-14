"""Unit tests of the Compose layout: core never depends on Airflow or object-store settings.

CI proves the same at run time: its integration job starts `make up` with a .env that has no
AIRFLOW_ and no S3_ variables (.github/workflows/tests.yml).
"""

import re
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

ROOT = Path(__file__).resolve().parents[2]
CORE = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")
AIRFLOW = (ROOT / "docker-compose.airflow.yml").read_text(encoding="utf-8")
REQUIRED = ["AIRFLOW_UID", "AIRFLOW_DB_PASSWORD", "AIRFLOW_FERNET_KEY", "AIRFLOW_JWT_SECRET"]


def test_core_file_references_no_airflow_variable_or_service() -> None:
    assert "AIRFLOW_" not in CORE
    assert re.search(r"^\s+airflow-[a-z-]+:\s*$", CORE, flags=re.MULTILINE) is None


@pytest.mark.parametrize("name", REQUIRED)
def test_airflow_file_requires_each_variable(name: str) -> None:
    uses = re.findall(r"\$\{" + name + r"([^}]*)\}", AIRFLOW)
    assert uses, f"{name} is not used in docker-compose.airflow.yml"
    assert all(use.startswith(":?") for use in uses), f"{name} is not required everywhere"


def test_airflow_file_has_no_optional_airflow_variable() -> None:
    assert re.search(r"\$\{AIRFLOW_[A-Z_]+:-", AIRFLOW) is None


def test_airflow_ports_are_bound_to_localhost_only() -> None:
    ports = re.findall(r'^\s+- "([^"]*:\d+:\d+)"\s*$', AIRFLOW, flags=re.MULTILINE)
    assert ports == ["127.0.0.1:8080:8080"]


S3_FILE = (ROOT / "docker-compose.s3.yml").read_text(encoding="utf-8")
S3_REQUIRED = [
    "S3_API_PORT",
    "S3_BUCKET",
    "S3_ADMIN_ACCESS_KEY",
    "S3_ADMIN_SECRET_KEY",
    "S3_ACCESS_KEY_ID",
    "S3_SECRET_ACCESS_KEY",
]


def test_core_file_references_no_object_store_variable_or_service() -> None:
    assert "S3_" not in CORE
    assert re.search(r"^\s+s3(-[a-z]+)?:\s*$", CORE, flags=re.MULTILINE) is None


@pytest.mark.parametrize("name", S3_REQUIRED)
def test_object_store_file_requires_each_variable(name: str) -> None:
    uses = re.findall(r"\$\{" + name + r"([^}]*)\}", S3_FILE)
    assert uses, f"{name} is not used in docker-compose.s3.yml"
    assert all(use.startswith(":?") for use in uses), f"{name} is not required everywhere"


def test_object_store_publishes_only_its_s3_api_on_localhost() -> None:
    ports = re.findall(r'^\s+- "([^"]*:\d+)"\s*$', S3_FILE, flags=re.MULTILINE)
    assert ports == ["127.0.0.1:${S3_API_PORT:?set S3_API_PORT in .env}:8333"]
    assert 'profiles: ["s3"]' in S3_FILE


def test_object_store_image_is_pinned_by_digest() -> None:
    assert re.search(r"chrislusf/seaweedfs:4\.48@sha256:[0-9a-f]{64}\b", S3_FILE)
    assert ":latest" not in S3_FILE
