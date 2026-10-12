"""Unit tests for src.config. They never read the real project .env (tmp_path + monkeypatch)."""

from pathlib import Path

import pytest

from src.config import (
    DEFAULT_ENV_FILE,
    PLACEHOLDER,
    ConfigError,
    load_database_settings,
)

pytestmark = pytest.mark.unit

CONFIG_VARIABLES = (
    "POSTGRES_HOST",
    "POSTGRES_PORT",
    "POSTGRES_DB",
    "DELIVERYPULSE_ETL_PASSWORD",
    "DELIVERYPULSE_READONLY_PASSWORD",
)

# Deliberately low-entropy dummy values: they must never look like real secrets.
ETL_VALUE = "e" * 16
READONLY_VALUE = "r" * 16


@pytest.fixture(autouse=True)
def clean_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    """Remove any real DeliveryPulse variables from the process environment for each test."""
    for name in CONFIG_VARIABLES:
        monkeypatch.delenv(name, raising=False)


def write_env_file(directory: Path, lines: list[str]) -> Path:
    """Write a .env file with the given lines into ``directory`` and return its path."""
    env_file = directory / ".env"
    env_file.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return env_file


def valid_lines() -> list[str]:
    """A complete, valid set of .env lines."""
    return [
        "POSTGRES_HOST=127.0.0.1",
        "POSTGRES_PORT=5432",
        "POSTGRES_DB=deliverypulse_test",
        f"DELIVERYPULSE_ETL_PASSWORD={ETL_VALUE}",
        f"DELIVERYPULSE_READONLY_PASSWORD={READONLY_VALUE}",
    ]


def test_default_env_file_is_in_project_root() -> None:
    """.env is located from the project root, independent of the working directory."""
    project_root = Path(__file__).resolve().parents[2]
    assert project_root / ".env" == DEFAULT_ENV_FILE


def test_reads_settings_from_env_file(tmp_path: Path) -> None:
    env_file = write_env_file(tmp_path, valid_lines())

    settings = load_database_settings("readonly", env_file=env_file)

    assert settings.host == "127.0.0.1"
    assert settings.port == 5432
    assert settings.dbname == "deliverypulse_test"
    assert settings.user == "deliverypulse_readonly"
    assert settings.password == READONLY_VALUE


def test_etl_role_uses_etl_user_and_password(tmp_path: Path) -> None:
    env_file = write_env_file(tmp_path, valid_lines())

    settings = load_database_settings("etl", env_file=env_file)

    assert settings.user == "deliverypulse_etl"
    assert settings.password == ETL_VALUE


def test_real_environment_overrides_env_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    env_file = write_env_file(tmp_path, valid_lines())
    monkeypatch.setenv("POSTGRES_PORT", "6543")

    settings = load_database_settings("readonly", env_file=env_file)

    assert settings.port == 6543


def test_missing_env_file_uses_environment_only(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("POSTGRES_DB", "from_environment")
    monkeypatch.setenv("DELIVERYPULSE_READONLY_PASSWORD", READONLY_VALUE)

    settings = load_database_settings("readonly", env_file=tmp_path / "does-not-exist.env")

    assert settings.dbname == "from_environment"
    assert settings.host == "127.0.0.1"
    assert settings.port == 5432


def test_missing_required_setting_raises(tmp_path: Path) -> None:
    lines = [line for line in valid_lines() if not line.startswith("POSTGRES_DB=")]
    env_file = write_env_file(tmp_path, lines)

    with pytest.raises(ConfigError, match="POSTGRES_DB"):
        load_database_settings("readonly", env_file=env_file)


def test_placeholder_password_is_rejected_without_echoing_value(tmp_path: Path) -> None:
    lines = [line for line in valid_lines() if not line.startswith("DELIVERYPULSE_ETL_PASSWORD=")]
    lines.append(f"DELIVERYPULSE_ETL_PASSWORD={PLACEHOLDER}-{ETL_VALUE}")
    env_file = write_env_file(tmp_path, lines)

    with pytest.raises(ConfigError, match="DELIVERYPULSE_ETL_PASSWORD") as excinfo:
        load_database_settings("etl", env_file=env_file)

    assert ETL_VALUE not in str(excinfo.value)


@pytest.mark.parametrize("bad_port", ["abc", "0", "70000"])
def test_invalid_port_is_rejected(tmp_path: Path, bad_port: str) -> None:
    lines = [line for line in valid_lines() if not line.startswith("POSTGRES_PORT=")]
    lines.append(f"POSTGRES_PORT={bad_port}")
    env_file = write_env_file(tmp_path, lines)

    with pytest.raises(ConfigError, match="POSTGRES_PORT"):
        load_database_settings("readonly", env_file=env_file)


def test_repr_hides_password(tmp_path: Path) -> None:
    env_file = write_env_file(tmp_path, valid_lines())

    settings = load_database_settings("readonly", env_file=env_file)

    assert READONLY_VALUE not in repr(settings)
    assert "deliverypulse_readonly" in repr(settings)


def test_conninfo_targets_configured_database(tmp_path: Path) -> None:
    env_file = write_env_file(tmp_path, valid_lines())

    conninfo = load_database_settings("readonly", env_file=env_file).conninfo()

    assert "dbname=deliverypulse_test" in conninfo
    assert "user=deliverypulse_readonly" in conninfo
    assert "connect_timeout=" in conninfo
