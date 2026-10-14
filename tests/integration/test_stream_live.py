"""Kafka end-to-end replay and durable database recovery on the isolated test database."""

import json
import time
import uuid
from typing import Any

import psycopg
import pytest
from psycopg.rows import TupleRow

from src.streaming.config import load_kafka_settings
from src.streaming.consumer import consume
from src.streaming.demo import run_window, watermarks
from src.streaming.messages import decode, encode
from src.streaming.producer import send
from src.streaming.writer import Delivery, write_batch
from tests.unit.transform_helpers import order

pytestmark = pytest.mark.kafka


def test_topic_layout_and_retention() -> None:
    """The demo assumes one partition and bounded retention on explicitly created topics."""
    from confluent_kafka.admin import AdminClient
    from confluent_kafka.admin._config import ConfigResource
    from confluent_kafka.admin._resource import ResourceType

    settings = load_kafka_settings()
    admin = AdminClient({"bootstrap.servers": settings.bootstrap_servers})
    # The pinned client's C extension populates fields typed as None in its Python wrapper.
    metadata: Any = admin.list_topics(timeout=10)
    assert metadata.cluster_id == settings.cluster_id
    topics = [settings.orders_topic, settings.pings_topic, settings.dlq_topic]
    for topic in topics:
        assert topic in metadata.topics
        assert set(metadata.topics[topic].partitions) == {0}
    resources = [ConfigResource(ResourceType.TOPIC, topic) for topic in topics]
    for future in admin.describe_configs(resources, request_timeout=10).values():
        values = future.result(timeout=15)
        assert values["retention.ms"].value == "3600000"
        assert values["retention.bytes"].value == "268435456"
        assert values["cleanup.policy"].value == "delete"


def test_live_flow_and_republished_replay() -> None:
    from confluent_kafka import Consumer, TopicPartition

    settings = load_kafka_settings()
    dlq_start = watermarks(settings, [settings.dlq_topic])[settings.dlq_topic]
    payload = encode(
        "order", order(order_id="stream-test-" + uuid.uuid4().hex), "live-test", "orders.csv", 1
    )
    first = run_window(
        settings,
        [(settings.orders_topic, payload), (settings.orders_topic, b"{malformed")],
        0,
        True,
    )
    second = run_window(settings, [(settings.orders_topic, payload)], 0, True)
    assert (first["inserted"], first["dead_letter"]) == (1, 1)
    assert (second["inserted"], second["duplicate"]) == (0, 1)
    reader = Consumer(
        {
            "bootstrap.servers": settings.bootstrap_servers,
            "group.id": "dlq-test-" + uuid.uuid4().hex,
            "enable.auto.commit": False,
        }
    )
    try:
        reader.assign([TopicPartition(settings.dlq_topic, 0, dlq_start)])
        message = reader.poll(10)
        assert message is not None and not message.error()
        body, key = message.value(), message.key()
        assert body is not None and key is not None
        value = json.loads(body)
        assert value["reason"] == "malformed_json"
        assert value["payload_base64"] == "e21hbGZvcm1lZA=="
        assert key.decode() == value["origin"]
    finally:
        reader.close()


def test_restart_after_database_commit_before_offset(monkeypatch: pytest.MonkeyPatch) -> None:
    """Fail at the actual acknowledgment boundary, then resume the same Kafka group."""
    from confluent_kafka import Consumer, TopicPartition

    from src.streaming import consumer as module

    settings = load_kafka_settings()
    topics = [settings.orders_topic, settings.pings_topic]
    starts = watermarks(settings, topics)
    group = "restart-test-" + uuid.uuid4().hex
    probe = Consumer(
        {
            "bootstrap.servers": settings.bootstrap_servers,
            "group.id": group,
            "enable.auto.commit": False,
        }
    )
    try:
        probe.assign([TopicPartition(t, 0, starts[t]) for t in topics])
        probe.commit(offsets=[TopicPartition(t, 0, starts[t]) for t in topics], asynchronous=False)
        payload = encode(
            "order", order(order_id="restart-" + uuid.uuid4().hex), "live-test", "orders.csv", 1
        )
        send(settings, [(settings.orders_topic, payload)])
        original = module.process_batch

        def fail_commit() -> None:
            raise RuntimeError("injected crash before offset commit")

        def crash(*args: Any, **kwargs: Any) -> Any:
            return original(args[0], args[1], args[2], args[3], fail_commit)

        with monkeypatch.context() as patch:
            patch.setattr(module, "process_batch", crash)
            with pytest.raises(RuntimeError, match="injected crash"):
                consume(settings, group, 1, test_database=True, timeout=20)
        offsets = probe.committed([TopicPartition(t, 0) for t in topics], timeout=10)
        assert {p.topic: p.offset for p in offsets} == starts
        result = consume(settings, group, 1, test_database=True, timeout=20)
        assert result["inserted"] == 0 and result["duplicate"] == 1
    finally:
        probe.close()


def test_database_commit_survives_lost_offset_ack(
    etl_test_conn: psycopg.Connection[TupleRow],
) -> None:
    """A separately committed insert is harmless when delivered again after a crash."""
    key = "crash-test-" + uuid.uuid4().hex
    record = decode(encode("order", order(order_id=key), "live-test", "orders.csv", 1))
    item = Delivery(record, "test", 0, 1, time.time_ns())
    assert write_batch(etl_test_conn, [item], "test-cluster") == ["inserted"]
    # No offset acknowledgment occurs between the durable transaction and this retry.
    assert write_batch(etl_test_conn, [item], "test-cluster") == ["duplicate"]
    changed = decode(
        encode("order", order(order_id=key, fare_amount="99"), "live-test", "orders.csv", 1)
    )
    assert write_batch(
        etl_test_conn, [Delivery(changed, "test", 0, 2, time.time_ns())], "test-cluster"
    ) == ["conflict"]
