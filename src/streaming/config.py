"""Validate local-demo settings without importing the optional Kafka client."""

import re
from collections.abc import Mapping
from dataclasses import dataclass

from src.config import ConfigError, read_environment


@dataclass(frozen=True)
class KafkaSettings:
    """Local endpoint, persistent cluster identity and three distinct topics."""

    bootstrap_servers: str
    cluster_id: str
    orders_topic: str
    pings_topic: str
    dlq_topic: str


def kafka_settings(values: Mapping[str, str]) -> KafkaSettings:
    """Reject missing settings, non-local endpoints and ambiguous topic names."""
    names = (
        "KAFKA_PORT",
        "KAFKA_BOOTSTRAP_SERVERS",
        "KAFKA_CLUSTER_ID",
        "KAFKA_TOPIC_ORDERS",
        "KAFKA_TOPIC_PINGS",
        "KAFKA_TOPIC_DLQ",
    )
    missing = [name for name in names if not values.get(name) or values[name] == "CHANGE_ME"]
    if missing:
        raise ConfigError("Missing Kafka settings: " + ", ".join(missing))
    port = values["KAFKA_PORT"]
    if not port.isascii() or not port.isdecimal() or not 1024 <= int(port) <= 65535:
        raise ConfigError("KAFKA_PORT must be an unprivileged TCP port (1024-65535)")
    if values["KAFKA_BOOTSTRAP_SERVERS"] != f"127.0.0.1:{int(port)}":
        raise ConfigError("KAFKA_BOOTSTRAP_SERVERS must match 127.0.0.1:KAFKA_PORT")
    if re.fullmatch(r"[A-Za-z0-9_-]{22}", values["KAFKA_CLUSTER_ID"]) is None:
        raise ConfigError("KAFKA_CLUSTER_ID must be a 22-character Kafka cluster identifier")
    topics = [values[name] for name in names[3:]]
    if any(re.fullmatch(r"[a-z][a-z0-9._-]{0,248}", topic) is None for topic in topics):
        raise ConfigError("Invalid Kafka topic names: use lowercase letters, digits, dot, _ or -")
    if len(set(topics)) != 3:
        raise ConfigError("Kafka input and dead-letter topics must be distinct")
    return KafkaSettings(values["KAFKA_BOOTSTRAP_SERVERS"], values["KAFKA_CLUSTER_ID"], *topics)


def load_kafka_settings() -> KafkaSettings:
    """Load settings using the application's normal environment precedence."""
    return kafka_settings(read_environment())


def main() -> int:
    """Report configuration validity without printing environment values."""
    try:
        load_kafka_settings()
    except ConfigError as exc:
        print(f"ERROR: {exc}")
        return 1
    print("Kafka configuration: valid local endpoint, cluster ID and distinct topics")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
