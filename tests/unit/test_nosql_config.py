"""MongoDB configuration and isolation checks require no service or optional client."""

from pathlib import Path

import pytest

from src.config import ConfigError
from src.nosql.config import mongo_settings, validate_environment

pytestmark = pytest.mark.unit
ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture
def settings() -> dict[str, str]:
    return {
        "MONGO_PORT": "27017",
        "MONGO_DB": "deliverypulse",
        "MONGO_ROOT_USER": "bootstrap",
        "MONGO_ROOT_PASSWORD": "r" * 24,
        "MONGO_APP_USER": "loader",
        "MONGO_APP_PASSWORD": "a" * 24,
        "MONGO_READONLY_USER": "reader",
        "MONGO_READONLY_PASSWORD": "q" * 24,
    }


def test_valid_settings_hide_credentials(settings: dict[str, str]) -> None:
    result = mongo_settings(settings, "app")
    assert result.database == "deliverypulse"
    assert result.port == 27017
    assert result.username == "loader"
    assert settings["MONGO_APP_PASSWORD"] not in repr(result)
    assert result.username not in repr(result)


@pytest.mark.parametrize("name", ["MONGO_ROOT_PASSWORD", "MONGO_APP_USER", "MONGO_PORT"])
def test_placeholder_rejected(settings: dict[str, str], name: str) -> None:
    settings[name] = "CHANGE_ME"
    with pytest.raises(ConfigError, match=name):
        validate_environment(settings)


@pytest.mark.parametrize("port", ["0", "1023", "65536", "abc", "２７０１７"])
def test_invalid_port(settings: dict[str, str], port: str) -> None:
    settings["MONGO_PORT"] = port
    with pytest.raises(ConfigError, match="MONGO_PORT"):
        validate_environment(settings)


@pytest.mark.parametrize("database", ["admin", "local", "config", "bad/name", "UPPER"])
def test_invalid_database(settings: dict[str, str], database: str) -> None:
    settings["MONGO_DB"] = database
    with pytest.raises(ConfigError, match="MONGO_DB"):
        validate_environment(settings)


def test_distinct_credentials(settings: dict[str, str]) -> None:
    settings["MONGO_APP_PASSWORD"] = settings["MONGO_ROOT_PASSWORD"]
    with pytest.raises(ConfigError, match="distinct"):
        validate_environment(settings)


def test_invalid_secret_not_echoed(settings: dict[str, str]) -> None:
    settings["MONGO_APP_PASSWORD"] = settings["MONGO_APP_PASSWORD"] + "!"
    with pytest.raises(ConfigError) as exc:
        validate_environment(settings)
    assert settings["MONGO_APP_PASSWORD"] not in str(exc.value)


def test_compose_confines_bootstrap_credentials() -> None:
    core = (ROOT / "docker-compose.yml").read_text()
    assert "MONGO_" not in core
    overlay = (ROOT / "docker-compose.mongo.yml").read_text()
    init, server = overlay.split("\n  mongo:\n", 1)
    assert "environment:" not in server
    assert "MONGO_ROOT_" not in server
    assert overlay.count("user: mongod") == 2
    assert "network_mode: none" in init
    assert "ports:" not in init
    assert '"127.0.0.1:${MONGO_PORT:?set MONGO_PORT in .env}:27017"' in server
    assert "command: [--auth," in server
    assert "--wiredTigerCacheSizeGB" in server
    assert overlay.count("image: *mongo-image") == 2
    assert "@sha256:67fbda8e00b63557d4bb797cb85324f456aa3c001bbb2528acdb969a1261c2a2" in init
    for line in init.splitlines():
        if "${MONGO_" in line:
            assert ":?" in line
    assert "name: deliverypulse_mongo_data" in server
    assert "name: deliverypulse_mongo_config" in server
