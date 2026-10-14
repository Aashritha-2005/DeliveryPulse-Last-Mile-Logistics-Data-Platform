"""Unit tests of the two-file Compose layout: core never depends on Airflow settings.

CI proves the same at run time: its integration job starts `make up` with a .env that has no
AIRFLOW_ variables (.github/workflows/tests.yml).
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
