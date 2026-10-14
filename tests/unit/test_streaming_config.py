"""Streaming configuration fails closed without requiring a Kafka client or broker."""

from pathlib import Path

import pytest

from src.config import ConfigError
from src.streaming.config import kafka_settings

pytestmark = pytest.mark.unit


@pytest.fixture
def settings() -> dict[str, str]:
    return {
        "KAFKA_PORT": "9092",
        "KAFKA_BOOTSTRAP_SERVERS": "127.0.0.1:9092",
        "KAFKA_CLUSTER_ID": "MkU3OEVBNTcwNTJENDM2Qk",
        "KAFKA_TOPIC_ORDERS": "deliverypulse.orders.v1",
        "KAFKA_TOPIC_PINGS": "deliverypulse.driver_pings.v1",
        "KAFKA_TOPIC_DLQ": "deliverypulse.events.dlq.v1",
    }


def test_local_configuration(settings: dict[str, str]) -> None:
    assert kafka_settings(settings).bootstrap_servers == "127.0.0.1:9092"


@pytest.mark.parametrize("port", ["0", "1023", "65536", "-1", "abc", "９０９２"])
def test_invalid_port(settings: dict[str, str], port: str) -> None:
    settings["KAFKA_PORT"] = port
    with pytest.raises(ConfigError, match="KAFKA_PORT"):
        kafka_settings(settings)


@pytest.mark.parametrize("endpoint", ["remote:9092", "0.0.0.0:9092", "127.0.0.1:9093"])
def test_endpoint_must_match_local_port(settings: dict[str, str], endpoint: str) -> None:
    settings["KAFKA_BOOTSTRAP_SERVERS"] = endpoint
    with pytest.raises(ConfigError, match="KAFKA_BOOTSTRAP_SERVERS"):
        kafka_settings(settings)


def test_missing_settings_report_names_only() -> None:
    with pytest.raises(ConfigError, match="Missing Kafka settings"):
        kafka_settings({})


def test_dlq_cannot_be_an_input_topic(settings: dict[str, str]) -> None:
    settings["KAFKA_TOPIC_DLQ"] = settings["KAFKA_TOPIC_ORDERS"]
    with pytest.raises(ConfigError, match="distinct"):
        kafka_settings(settings)


@pytest.mark.parametrize("topic", ["__consumer_offsets", "UPPERCASE", "bad/topic", "a" * 250])
def test_invalid_topic(settings: dict[str, str], topic: str) -> None:
    settings["KAFKA_TOPIC_ORDERS"] = topic
    with pytest.raises(ConfigError, match="topic names"):
        kafka_settings(settings)


def test_cluster_id_placeholder_is_rejected(settings: dict[str, str]) -> None:
    settings["KAFKA_CLUSTER_ID"] = "CHANGE_ME"
    with pytest.raises(ConfigError, match="Missing Kafka settings: KAFKA_CLUSTER_ID"):
        kafka_settings(settings)


def test_invalid_cluster_id(settings: dict[str, str]) -> None:
    settings["KAFKA_CLUSTER_ID"] = "invalid"
    with pytest.raises(ConfigError, match="KAFKA_CLUSTER_ID"):
        kafka_settings(settings)


def test_core_compose_remains_independent() -> None:
    root = Path(__file__).resolve().parents[2]
    core = (root / "docker-compose.yml").read_text()
    assert "KAFKA_" not in core
    assert "kafka:" not in core
    overlay = (root / "docker-compose.kafka.yml").read_text()
    assert "127.0.0.1:${KAFKA_PORT:" in overlay
    assert 'KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"' in overlay
    assert "--config retention.ms=3600000" in overlay
    assert "--config retention.bytes=268435456" in overlay
