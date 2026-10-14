"""Exercise the actual consumer loop with scripted external boundaries."""

import json
import sys
from types import ModuleType, SimpleNamespace
from typing import Any
from unittest.mock import Mock

import pytest

from src.streaming import consumer
from src.streaming.messages import encode
from tests.unit.test_streaming import SETTINGS
from tests.unit.transform_helpers import order

pytestmark = pytest.mark.unit


def message(payload: bytes, topic: str = "orders", offset: int = 10) -> Mock:
    """Return one transport message with stable latency and provenance."""
    return Mock(
        value=lambda: payload,
        topic=lambda: topic,
        partition=lambda: 0,
        offset=lambda: offset,
        headers=lambda: [("produced_ns", b"1000000")],
        error=lambda: None,
    )


@pytest.fixture
def loop(monkeypatch: pytest.MonkeyPatch) -> Any:
    """Install fake drivers even when the optional Kafka module is absent."""
    actions: list[str] = []
    reader, sender = Mock(), Mock()

    def commit(**kwargs: Any) -> list[Any]:
        actions.append("commit")
        return []

    def flush(timeout: int) -> int:
        actions.append("dlq_ack")
        return 0

    def write(conn: Any, items: Any, cluster: str) -> list[str]:
        actions.append("database")
        return ["inserted"] * len(items)

    reader.commit.side_effect = commit
    sender.flush.side_effect = flush
    sender.produce.side_effect = lambda *a, **kw: kw["on_delivery"](None, None)
    module = ModuleType("confluent_kafka")
    constructor = Mock(return_value=reader)
    monkeypatch.setattr(module, "Consumer", constructor, raising=False)
    monkeypatch.setattr(module, "TopicPartition", lambda t, p, o=None: (t, p, o), raising=False)
    monkeypatch.setitem(sys.modules, "confluent_kafka", module)
    monkeypatch.setattr(consumer, "client", lambda settings: sender)
    connection = Mock()
    connection.__enter__ = Mock(return_value=connection)
    connection.__exit__ = Mock(return_value=False)
    monkeypatch.setattr("src.streaming.consumer.psycopg.connect", Mock(return_value=connection))
    monkeypatch.setattr(consumer, "load_database_settings", lambda *a, **kw: Mock())
    writer = Mock(side_effect=write)
    monkeypatch.setattr(consumer, "write_batch", writer)
    monkeypatch.setattr(
        consumer,
        "time",
        SimpleNamespace(
            monotonic=lambda: 0, perf_counter=Mock(side_effect=[0, 1]), time_ns=lambda: 2000000
        ),
    )
    return SimpleNamespace(
        reader=reader, sender=sender, actions=actions, writer=writer, constructor=constructor
    )


def test_bounded_window_commits_next_offsets_after_database_and_dlq(loop: Any) -> None:
    good = encode("order", order(), "fixture", "orders.csv", 1)
    loop.reader.poll.side_effect = [message(good), message(b"{", "pings", 20)]
    result = consumer.consume(SETTINGS, "test-group", 2, {"orders": 10, "pings": 20})
    assert (result["consumed"], result["inserted"], result["dead_letter"]) == (2, 1, 1)
    assert loop.actions == ["database", "dlq_ack", "commit"]
    loop.reader.commit.assert_called_once_with(
        offsets=[("orders", 0, 11), ("pings", 0, 21)], asynchronous=False
    )
    loop.reader.assign.assert_called_once_with([("orders", 0, 10), ("pings", 0, 20)])
    assert loop.reader.poll.call_count == 2
    assert result["latency_ms_p50"] == result["latency_ms_p95"] == 1
    dlq = loop.sender.produce.call_args
    assert dlq.args == ("dlq",)
    assert json.loads(dlq.kwargs["value"]) == {
        "reason": "malformed_json",
        "origin": "test:pings:0:20",
        "payload_base64": "ew==",
    }
    assert dlq.kwargs["key"] == b"test:pings:0:20"
    loop.reader.close.assert_called_once()


@pytest.mark.parametrize("failure", ["database", "dlq_flush", "dlq_callback", "commit", "poll"])
def test_failures_propagate_and_close_reader(loop: Any, failure: str) -> None:
    good = encode("order", order(), "fixture", "orders.csv", 1)
    msg = message(good if failure == "database" else b"{")
    loop.reader.poll.return_value = msg
    if failure == "database":
        loop.writer.side_effect = RuntimeError("write failed")
    elif failure == "dlq_flush":
        loop.sender.flush.return_value = 1
        loop.sender.flush.side_effect = None
    elif failure == "dlq_callback":
        loop.sender.produce.side_effect = lambda *a, **kw: kw["on_delivery"]("failure", None)
    elif failure == "commit":
        loop.reader.commit.side_effect = lambda **kw: [SimpleNamespace(error="failed")]
    else:
        msg.error = lambda: "broker unavailable"
    with pytest.raises(RuntimeError):
        consumer.consume(SETTINGS, "test-group", 1)
    if failure != "commit":
        loop.reader.commit.assert_not_called()
    loop.reader.close.assert_called_once()


def test_timeout_does_not_acknowledge(loop: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("src.streaming.consumer.time.monotonic", Mock(side_effect=[0, 0, 2]))
    with pytest.raises(TimeoutError, match="expected record count"):
        consumer.consume(SETTINGS, "test-group", 1, timeout=1)
    loop.reader.poll.assert_not_called()
    loop.reader.commit.assert_not_called()
    loop.reader.close.assert_called_once()


def test_all_rejected_window_has_no_database_write_or_latency(loop: Any) -> None:
    loop.reader.poll.return_value = message(b"{")
    result = consumer.consume(SETTINGS, "test-group", 1)
    loop.writer.assert_not_called()
    assert result["dead_letter"] == 1 and result["latency_ms_max"] is None
    loop.reader.assign.assert_called_once_with([("orders", 0, None), ("pings", 0, None)])
    config = loop.constructor.call_args.args[0]
    assert config["enable.auto.commit"] is config["enable.auto.offset.store"] is False


@pytest.mark.parametrize("count", [0, 100006])
def test_invalid_window_never_constructs_reader(loop: Any, count: int) -> None:
    with pytest.raises(ValueError, match="count must"):
        consumer.consume(SETTINGS, "test-group", count)
    loop.constructor.assert_not_called()
