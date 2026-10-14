"""Append-only PostgreSQL landing writes with replay/conflict detection."""

from dataclasses import dataclass
from datetime import UTC, datetime

import psycopg
from psycopg.rows import TupleRow

from src.streaming.messages import Record


@dataclass(frozen=True)
class Delivery:
    """A validated source record and its first-delivery Kafka provenance."""

    record: Record
    topic: str
    partition: int
    offset: int
    produced_ns: int


INSERT = """
INSERT INTO raw.stream_events (
    record_kind, record_key, record_version, content_hash, payload,
    source_dataset, source_file, source_row_number, kafka_cluster_id,
    kafka_topic, kafka_partition, kafka_offset, produced_at
) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
ON CONFLICT (record_kind, record_key, record_version) DO NOTHING
RETURNING content_hash
"""


def write_batch(
    conn: psycopg.Connection[TupleRow], deliveries: list[Delivery], cluster_id: str
) -> list[str]:
    """Commit a bounded batch, returning inserted/duplicate/conflict in input order.

    Never acknowledge Kafka here. The caller may acknowledge only after this transaction
    and every dead-letter delivery have succeeded. Repeated identities inside a batch work
    because conflict hashes are read after every insert has completed.
    """
    outcomes: list[str] = []
    with conn.transaction():
        with conn.pipeline():
            cursors = []
            for item in deliveries:
                r = item.record
                cursors.append(
                    conn.execute(
                        INSERT,
                        (
                            r.kind,
                            r.key,
                            r.version,
                            r.digest,
                            r.payload,
                            r.dataset,
                            r.file,
                            r.row,
                            cluster_id,
                            item.topic,
                            item.partition,
                            item.offset,
                            datetime.fromtimestamp(item.produced_ns / 1e9, UTC),
                        ),
                    )
                )
        for item, cursor in zip(deliveries, cursors, strict=True):
            if cursor.fetchone() is not None:
                outcomes.append("inserted")
                continue
            r = item.record
            stored = conn.execute(
                "SELECT content_hash FROM raw.stream_events "
                "WHERE record_kind=%s AND record_key=%s AND record_version=%s",
                (r.kind, r.key, r.version),
            ).fetchone()
            if stored is None:
                raise RuntimeError("Landing identity disappeared during replay check")
            outcomes.append("duplicate" if stored[0] == r.digest else "conflict")
    return outcomes
