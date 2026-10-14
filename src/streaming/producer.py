"""Acknowledged, rate-limited Kafka production; imported only by streaming commands."""

import time
from collections.abc import Iterable
from typing import Any

from src.streaming.config import KafkaSettings


def client(settings: KafkaSettings) -> Any:
    """Create a bounded, idempotent producer using the optional client dependency."""
    from confluent_kafka import Producer

    return Producer(
        {
            "bootstrap.servers": settings.bootstrap_servers,
            "enable.idempotence": True,
            "acks": "all",
            "delivery.timeout.ms": 30000,
            "queue.buffering.max.kbytes": 16384,
            "queue.buffering.max.messages": 20000,
            "linger.ms": 5,
            "allow.auto.create.topics": False,
        }
    )


def send(
    settings: KafkaSettings,
    records: Iterable[tuple[str, bytes]],
    rate: float = 0,
) -> int:
    """Send records at a maximum configured rate (zero means unthrottled), then flush."""
    if rate < 0:
        raise ValueError("rate must be non-negative")
    producer = client(settings)
    failures: list[str] = []
    acknowledged = 0

    def delivered(error: Any, message: Any) -> None:
        nonlocal acknowledged
        if error is not None:
            failures.append(str(error))
        else:
            acknowledged += 1

    started = time.perf_counter()
    total = 0
    for topic, payload in records:
        if rate:
            time.sleep(max(0, started + total / rate - time.perf_counter()))
        deadline = time.monotonic() + 30
        while True:
            try:
                producer.produce(
                    topic,
                    value=payload,
                    headers={
                        "produced_ns": str(time.time_ns()).encode(),
                    },
                    on_delivery=delivered,
                )
                break
            except BufferError:
                if time.monotonic() >= deadline:
                    raise TimeoutError("Producer queue remained full") from None
                producer.poll(0.01)
        total += 1
        producer.poll(0)
        if failures:
            raise RuntimeError("Kafka delivery failed: " + failures[0])
    if producer.flush(30) or failures or acknowledged != total:
        raise RuntimeError("Kafka delivery was not fully acknowledged")
    return acknowledged
