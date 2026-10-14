"""Replay identity and failure ordering without any external service."""

import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

from src.streaming.config import KafkaSettings
from src.streaming.consumer import process_batch
from src.streaming.demo import source_records
from src.streaming.messages import InvalidMessageError, decode, encode
from tests.unit.transform_helpers import order, ping

pytestmark = pytest.mark.unit
SETTINGS = KafkaSettings("127.0.0.1:9092", "test", "orders", "pings", "dlq")


@dataclass
class Message:
    payload: bytes

    def value(self) -> bytes:
        return self.payload

    def topic(self) -> str:
        return "orders"

    def partition(self) -> int:
        return 0

    def offset(self) -> int:
        return 10

    def headers(self) -> list[tuple[str, bytes]]:
        return [("produced_ns", str(time.time_ns() - 1000000).encode())]


def test_identity_excludes_transport_and_provenance() -> None:
    a = decode(encode("order", order(), "a", "orders.csv", 1))
    b = decode(encode("order", order(), "b", "orders.csv", 99))
    assert (a.key, a.version, a.digest) == (b.key, b.version, b.digest)
    changed = decode(encode("order", order(fare_amount="100"), "a", "orders.csv", 1))
    assert a.digest != changed.digest


@pytest.mark.parametrize("payload", [b"{", b"null", b"[]", b"\xff", b"{}"])
def test_malformed(payload: bytes) -> None:
    with pytest.raises(InvalidMessageError):
        decode(payload)


def test_batch_rules_are_reused() -> None:
    with pytest.raises(InvalidMessageError, match="invalid_status"):
        decode(encode("order", order(status="wrong"), "a", "orders.csv", 1))
    fields = ping("p", "2026-09-10T10:00:00Z", "91", "0")
    with pytest.raises(InvalidMessageError, match="invalid_coordinates"):
        decode(encode("driver_ping", fields, "a", "events.jsonl", 1))


@pytest.mark.parametrize("replacement", [None, [], True, "wrong"])
def test_fields_must_be_an_object(replacement: Any) -> None:
    envelope = json.loads(encode("order", order(), "a", "orders.csv", 1))
    envelope["fields"] = replacement
    with pytest.raises(InvalidMessageError, match="invalid_fields"):
        decode(json.dumps(envelope).encode())


@pytest.mark.parametrize("row", [0, -1, True, "1"])
def test_invalid_source_row(row: Any) -> None:
    envelope = json.loads(encode("order", order(), "a", "orders.csv", 1))
    envelope["row"] = row
    with pytest.raises(InvalidMessageError, match="invalid_provenance"):
        decode(json.dumps(envelope).encode())


def test_source_selection_skips_invalid_and_duplicate_without_rewriting(tmp_path: Path) -> None:
    directory = tmp_path / "dataset" / "batch-1"
    directory.mkdir(parents=True)
    path = directory / "delivery_events.jsonl"
    a = ping("a", "2026-09-10T10:00:00Z", "20", "70")
    b = {**a, "event_id": "b"}
    bad = {**a, "event_id": "bad", "lat": "100"}
    content = "\n".join(json.dumps(row) for row in [a, a, bad, b]) + "\n"
    path.write_text(content)
    selected = source_records(tmp_path, "driver_ping", 1, 1)
    assert decode(selected[0]).key == "b"
    assert decode(selected[0]).row == 4
    assert path.read_text() == content
    with pytest.raises(ValueError, match="Not enough"):
        source_records(tmp_path, "driver_ping", 0, 3)


@pytest.mark.parametrize("outcome", ["inserted", "duplicate", "conflict"])
def test_commit_after_database_and_dlq(outcome: str) -> None:
    actions: list[str] = []

    def write(items: Any) -> list[str]:
        actions.append("database")
        return [outcome]

    def dlq(items: Any) -> None:
        actions.append("dlq")
        assert len(items) == (2 if outcome == "conflict" else 1)

    counts, _ = process_batch(
        [Message(encode("order", order(), "a", "orders.csv", 1)), Message(b"{")],
        SETTINGS,
        write,
        dlq,
        lambda: actions.append("offset"),
    )
    assert actions == ["database", "dlq", "offset"]
    assert counts["consumed"] == sum(counts[k] for k in ("inserted", "duplicate", "dead_letter"))


@pytest.mark.parametrize("failure", ["database", "dlq"])
def test_failure_never_commits_offsets(failure: str) -> None:
    actions: list[str] = []

    def write(items: Any) -> list[str]:
        if failure == "database":
            raise RuntimeError("database")
        return ["inserted"]

    def dlq(items: Any) -> None:
        raise RuntimeError("dlq")

    with pytest.raises(RuntimeError, match=failure):
        process_batch(
            [Message(encode("order", order(), "a", "orders.csv", 1))],
            SETTINGS,
            write,
            dlq,
            lambda: actions.append("offset"),
        )
    assert actions == []
