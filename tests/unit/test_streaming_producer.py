"""Delivery guarantees with a scripted producer and no optional client installation."""

import sys
from types import ModuleType, SimpleNamespace
from typing import Any
from unittest.mock import Mock

import pytest

from src.streaming import producer
from tests.unit.test_streaming import SETTINGS

pytestmark = pytest.mark.unit


@pytest.fixture
def broker(monkeypatch: pytest.MonkeyPatch) -> Mock:
    """Replace the client boundary, never the delivery algorithm."""
    fake = Mock()
    fake.flush.return_value = 0
    fake.produce.side_effect = lambda *a, **kw: kw["on_delivery"](None, None)
    module = ModuleType("confluent_kafka")
    constructor = Mock(return_value=fake)
    monkeypatch.setattr(module, "Producer", constructor, raising=False)
    monkeypatch.setitem(sys.modules, "confluent_kafka", module)
    fake.constructor = constructor
    return fake


def test_acknowledgments_and_safe_client_settings(broker: Mock) -> None:
    assert producer.send(SETTINGS, [("orders", b"a"), ("pings", b"b")]) == 2
    config = broker.constructor.call_args.args[0]
    assert config["enable.idempotence"] is True
    assert config["acks"] == "all"
    assert config["allow.auto.create.topics"] is False
    assert [c.kwargs["value"] for c in broker.produce.call_args_list] == [b"a", b"b"]
    broker.flush.assert_called_once_with(30)


@pytest.mark.parametrize("failure", ["callback", "flush", "missing", "late_callback"])
def test_incomplete_delivery_never_reports_success(broker: Mock, failure: str) -> None:
    if failure == "callback":
        broker.produce.side_effect = lambda *a, **kw: kw["on_delivery"]("rejected", None)
    elif failure == "flush":
        broker.flush.return_value = 1
    elif failure == "missing":
        broker.produce.side_effect = None
    else:
        broker.produce.side_effect = None

        def flush(timeout: int) -> int:
            broker.produce.call_args.kwargs["on_delivery"]("late rejection", None)
            return 0

        broker.flush.side_effect = flush
    with pytest.raises(RuntimeError, match="Kafka delivery"):
        producer.send(SETTINGS, [("orders", b"a")])


def test_queue_retry_preserves_payload(broker: Mock) -> None:
    attempts = 0

    def produce(*args: Any, **kwargs: Any) -> None:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise BufferError
        kwargs["on_delivery"](None, None)

    broker.produce.side_effect = produce
    assert producer.send(SETTINGS, [("orders", b"original")]) == 1
    assert attempts == 2
    assert all(c.kwargs["value"] == b"original" for c in broker.produce.call_args_list)
    broker.poll.assert_any_call(0.01)


def test_persistent_queue_saturation_times_out(
    broker: Mock, monkeypatch: pytest.MonkeyPatch
) -> None:
    broker.produce.side_effect = BufferError
    monkeypatch.setattr(
        producer,
        "time",
        SimpleNamespace(
            perf_counter=lambda: 0, monotonic=Mock(side_effect=[0, 31]), time_ns=lambda: 1
        ),
    )
    with pytest.raises(TimeoutError, match="queue remained full"):
        producer.send(SETTINGS, [("orders", b"a")])
    broker.flush.assert_not_called()


def test_rate_limits_scheduled_sends(broker: Mock, monkeypatch: pytest.MonkeyPatch) -> None:
    sleep = Mock()
    monkeypatch.setattr(
        producer,
        "time",
        SimpleNamespace(
            perf_counter=lambda: 10, monotonic=lambda: 10, time_ns=lambda: 1, sleep=sleep
        ),
    )
    assert producer.send(SETTINGS, [("orders", b"a")] * 3, rate=2) == 3
    assert [c.args[0] for c in sleep.call_args_list] == [0, 0.5, 1]


def test_negative_rate_does_not_construct_client(broker: Mock) -> None:
    with pytest.raises(ValueError, match="non-negative"):
        producer.send(SETTINGS, [], rate=-1)
    broker.constructor.assert_not_called()
