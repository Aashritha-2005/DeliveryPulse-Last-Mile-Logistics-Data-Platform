"""Bounded consumer: database commit, acknowledged DLQ, then synchronous offsets."""

import base64
import json
import time
from collections.abc import Callable
from typing import Any

import psycopg

from src.config import load_database_settings
from src.streaming.config import KafkaSettings
from src.streaming.messages import InvalidMessageError, decode
from src.streaming.producer import client
from src.streaming.writer import Delivery, write_batch


def process_batch(
    messages: list[Any],
    settings: KafkaSettings,
    write: Callable[[list[Delivery]], list[str]],
    dead_letter: Callable[[list[tuple[Any, str]]], None],
    commit: Callable[[], None],
) -> tuple[dict[str, int], list[float]]:
    """Resolve every input before acknowledging; failure at any step prevents commit."""
    accepted: list[Delivery] = []
    accepted_messages: list[Any] = []
    rejected: list[tuple[Any, str]] = []
    for msg in messages:
        try:
            record = decode(msg.value() or b"")
            expected = settings.orders_topic if record.kind == "order" else settings.pings_topic
            if msg.topic() != expected:
                raise InvalidMessageError("topic_kind_mismatch")
            headers = dict(msg.headers() or [])
            try:
                produced_ns = int(headers.get("produced_ns", b""))
            except (ValueError, TypeError):
                raise InvalidMessageError("invalid_produced_ns") from None
            if not 0 < produced_ns <= time.time_ns():
                raise InvalidMessageError("invalid_produced_ns")
            accepted.append(
                Delivery(record, msg.topic(), msg.partition(), msg.offset(), produced_ns)
            )
            accepted_messages.append(msg)
        except InvalidMessageError as exc:
            rejected.append((msg, str(exc)))
    outcomes = write(accepted) if accepted else []
    saved_ns = time.time_ns()
    counts = {"consumed": len(messages), "inserted": 0, "duplicate": 0, "dead_letter": 0}
    latency: list[float] = []
    for item, msg, outcome in zip(accepted, accepted_messages, outcomes, strict=True):
        if outcome == "conflict":
            rejected.append((msg, "conflicting_payload"))
        elif outcome in ("inserted", "duplicate"):
            counts[outcome] += 1
            latency.append((saved_ns - item.produced_ns) / 1e6)
        else:
            raise RuntimeError("Unknown database outcome")
    dead_letter(rejected)
    counts["dead_letter"] = len(rejected)
    if counts["consumed"] != counts["inserted"] + counts["duplicate"] + counts["dead_letter"]:
        raise RuntimeError("Stream reconciliation failed")
    commit()
    return counts, latency


def consume(
    settings: KafkaSettings,
    group: str,
    count: int,
    starts: dict[str, int] | None = None,
    *,
    test_database: bool = False,
    timeout: float = 300,
    ready: Callable[[], None] = lambda: None,
) -> dict[str, Any]:
    """Consume exactly a bounded demo window, resuming committed offsets when starts is None."""
    from confluent_kafka import Consumer, TopicPartition

    if not 0 < count <= 100005:
        raise ValueError("count must be between 1 and 100005")
    consumer = Consumer(
        {
            "bootstrap.servers": settings.bootstrap_servers,
            "group.id": group,
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
            "auto.offset.reset": "earliest",
            "allow.auto.create.topics": False,
            "queued.max.messages.kbytes": 16384,
        }
    )
    producer = client(settings)
    totals = {"consumed": 0, "inserted": 0, "duplicate": 0, "dead_letter": 0}
    latencies: list[float] = []
    begun = time.perf_counter()
    deadline = time.monotonic() + timeout

    def dlq(rejected: list[tuple[Any, str]]) -> None:
        errors: list[str] = []

        def delivered(error: Any, message: Any) -> None:
            if error is not None:
                errors.append(str(error))

        for msg, reason in rejected:
            provenance = f"{settings.cluster_id}:{msg.topic()}:{msg.partition()}:{msg.offset()}"
            value = json.dumps(
                {
                    "reason": reason,
                    "origin": provenance,
                    "payload_base64": base64.b64encode(msg.value() or b"").decode(),
                }
            ).encode()
            producer.produce(
                settings.dlq_topic, key=provenance.encode(), value=value, on_delivery=delivered
            )
        if producer.flush(30) or errors:
            raise RuntimeError("Dead-letter delivery not acknowledged")

    try:
        partitions = [
            TopicPartition(t, 0, starts[t]) if starts is not None else TopicPartition(t, 0)
            for t in (settings.orders_topic, settings.pings_topic)
        ]
        consumer.assign(partitions)
        with psycopg.connect(
            load_database_settings("etl", test_database=test_database).conninfo(), autocommit=True
        ) as conn:
            ready()
            while totals["consumed"] < count:
                batch: list[Any] = []
                flush_at = time.monotonic() + 0.25
                while len(batch) < min(500, count - totals["consumed"]):
                    if time.monotonic() >= deadline:
                        raise TimeoutError("Consumer did not receive the expected record count")
                    msg = consumer.poll(min(0.05, max(0, flush_at - time.monotonic())))
                    if msg is not None:
                        if msg.error():
                            raise RuntimeError(f"Kafka poll failed: {msg.error()}")
                        batch.append(msg)
                    if time.monotonic() >= flush_at:
                        break
                if not batch:
                    continue
                offsets: dict[tuple[str, int], int] = {}
                for msg in batch:
                    offsets[msg.topic(), msg.partition()] = msg.offset() + 1

                def commit(offsets: dict[tuple[str, int], int] = offsets) -> None:
                    result = consumer.commit(
                        offsets=[TopicPartition(t, p, o) for (t, p), o in offsets.items()],
                        asynchronous=False,
                    )
                    if any(part.error for part in result or []):
                        raise RuntimeError("Kafka offset commit failed")

                counts, delays = process_batch(
                    batch,
                    settings,
                    lambda items: write_batch(conn, items, settings.cluster_id),
                    dlq,
                    commit,
                )
                for key, value in counts.items():
                    totals[key] += value
                latencies.extend(delays)
    finally:
        consumer.close()
    duration = time.perf_counter() - begun
    latencies.sort()
    return {
        **totals,
        "seconds": duration,
        "records_per_second": count / duration,
        "latency_ms_p50": latencies[len(latencies) // 2] if latencies else None,
        "latency_ms_p95": latencies[min(len(latencies) - 1, int(len(latencies) * 0.95))]
        if latencies
        else None,
        "latency_ms_max": max(latencies) if latencies else None,
    }
