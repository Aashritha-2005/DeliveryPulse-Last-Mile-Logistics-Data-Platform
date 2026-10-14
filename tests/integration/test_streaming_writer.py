"""Landing transactions and immutable replay without a Kafka broker."""

from dataclasses import replace
from uuid import uuid4

import psycopg
import pytest
from psycopg.rows import TupleRow

from src.streaming.messages import decode, encode
from src.streaming.writer import Delivery, write_batch
from tests.unit.transform_helpers import order

pytestmark = pytest.mark.integration


def delivery(key: str, fare: str = "10") -> Delivery:
    """Create a valid synthetic event with fixed transport provenance."""
    record = decode(
        encode("order", order(order_id=key, fare_amount=fare), "fixture", "orders.csv", 1)
    )
    return Delivery(record, "orders", 0, 1, 1000000000)


def test_same_batch_duplicates_and_changed_replay_preserve_original(
    test_db: psycopg.Connection[TupleRow],
) -> None:
    key = "coverage-" + uuid4().hex
    first = delivery(key)
    assert write_batch(test_db, [first, first], "fixture") == ["inserted", "duplicate"]
    query = "SELECT * FROM raw.stream_events WHERE record_key = %s"
    before = test_db.execute(query, (key,)).fetchall()
    assert len(before) == 1
    assert write_batch(test_db, [replace(first, offset=99)], "other-cluster") == ["duplicate"]
    assert write_batch(test_db, [delivery(key, "99")], "fixture") == ["conflict"]
    assert test_db.execute(query, (key,)).fetchall() == before


def test_database_error_rolls_back_entire_batch(test_db: psycopg.Connection[TupleRow]) -> None:
    key = "coverage-" + uuid4().hex
    good = delivery(key)
    bad = replace(delivery(key + "-bad"), partition=2**31)
    with pytest.raises(psycopg.errors.NumericValueOutOfRange):
        write_batch(test_db, [good, bad], "fixture")
    assert test_db.execute(
        "SELECT count(*) FROM raw.stream_events WHERE record_key = ANY(%s)",
        ([key, key + "-bad"],),
    ).fetchone() == (0,)
