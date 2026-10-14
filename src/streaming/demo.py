"""Concurrent source-to-database demo with a separately measured replay."""

import argparse
import csv
import json
import platform
import resource
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from itertools import islice
from pathlib import Path
from typing import Any

import psycopg

from src.config import load_database_settings
from src.streaming.config import KafkaSettings, load_kafka_settings
from src.streaming.consumer import consume
from src.streaming.messages import InvalidMessageError, decode, encode
from src.streaming.producer import send


def source_records(root: Path, kind: str, skip: int, count: int) -> list[bytes]:
    """Select deterministic valid unique source records, without rewriting source files."""
    filename = "orders.csv" if kind == "order" else "delivery_events.jsonl"
    seen: set[tuple[str, str]] = set()
    selected: list[bytes] = []
    for path in sorted(root.glob(f"*/batch-*/{filename}")):
        with path.open() as stream:
            rows = csv.DictReader(stream) if kind == "order" else (json.loads(s) for s in stream)
            for row, fields in enumerate(rows, 1):
                payload = encode(
                    kind, fields, path.parent.parent.name, f"{path.parent.name}/{path.name}", row
                )
                try:
                    record = decode(payload)
                except InvalidMessageError:
                    continue
                identity = record.key, record.version
                if identity in seen:
                    continue
                seen.add(identity)
                if len(seen) <= skip:
                    continue
                selected.append(payload)
                if len(selected) == count:
                    return selected
    raise ValueError(f"Not enough unique valid {kind} records for requested slice")


def watermarks(settings: KafkaSettings, topics: list[str]) -> dict[str, int]:
    """Snapshot end offsets; demo consumes only records published after this point."""
    from confluent_kafka import Consumer, TopicPartition

    consumer = Consumer(
        {
            "bootstrap.servers": settings.bootstrap_servers,
            "group.id": "deliverypulse-probe",
            "enable.auto.commit": False,
        }
    )
    try:
        return {
            topic: consumer.get_watermark_offsets(TopicPartition(topic, 0), timeout=10)[1]
            for topic in topics
        }
    finally:
        consumer.close()


def run_window(
    settings: KafkaSettings,
    records: list[tuple[str, bytes]],
    rate: float,
    test_database: bool = False,
) -> dict[str, Any]:
    """Start consumption before producing and propagate failures from either worker."""
    starts = watermarks(settings, [settings.orders_topic, settings.pings_topic])
    ready = threading.Event()
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(
            consume,
            settings,
            "deliverypulse-demo-" + uuid.uuid4().hex,
            len(records),
            starts,
            test_database=test_database,
            ready=ready.set,
            timeout=max(60, len(records) / max(rate, 500) + 60),
        )
        if not ready.wait(15):
            future.result(timeout=1)
            raise TimeoutError("Consumer did not initialize")
        start = time.perf_counter()
        acknowledged = send(settings, records, rate)
        producer_seconds = time.perf_counter() - start
        result = future.result()
    return {
        **result,
        "producer_acknowledged": acknowledged,
        "producer_seconds": producer_seconds,
        "requested_rate": rate,
    }


def main() -> None:
    """Run a selected source slice plus malformed inputs, then replay the same valid bytes."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--count", type=int, default=500)
    parser.add_argument("--rate", type=float, default=500)
    parser.add_argument("--trial", type=int, default=0)
    parser.add_argument("--malformed", type=int, default=5)
    parser.add_argument("--raw-dir", type=Path, default=Path("data/raw"))
    args = parser.parse_args()
    if args.count not in (500, 100000) or args.trial not in range(4):
        parser.error("use count 500 or 100000, trial 0..3")
    if args.rate < 0 or not 0 <= args.malformed <= 5:
        parser.error("rate must be non-negative; malformed must be 0..5")
    settings = load_kafka_settings()
    orders = 250 if args.count == 500 else 10000
    pings = args.count - orders
    order_skip = 0 if args.trial == 0 else 250 + (args.trial - 1) * 10000
    ping_skip = 0 if args.trial == 0 else 250 + (args.trial - 1) * 90000
    a = source_records(args.raw_dir, "order", order_skip, orders)
    b = source_records(args.raw_dir, "driver_ping", ping_skip, pings)
    # Interleave bounded chunks so both topics receive data throughout the run.
    records: list[tuple[str, bytes]] = []
    ai, bi = iter(a), iter(b)
    while len(records) < args.count:
        records.extend((settings.orders_topic, p) for p in islice(ai, 10))
        records.extend((settings.pings_topic, p) for p in islice(bi, 90))
    with psycopg.connect(load_database_settings("etl").conninfo()) as conn:
        before = conn.execute("SELECT count(*) FROM raw.stream_events").fetchone()
    malformed = [(settings.orders_topic, b"{malformed") for _ in range(args.malformed)]
    initial = run_window(settings, records + malformed, args.rate)
    replay = run_window(settings, records, args.rate)
    with psycopg.connect(load_database_settings("etl").conninfo()) as conn:
        after = conn.execute("SELECT count(*) FROM raw.stream_events").fetchone()
    if before is None or after is None:
        raise RuntimeError("Missing database count")
    if (
        initial["dead_letter"] != args.malformed
        or replay["inserted"] != 0
        or replay["duplicate"] != args.count
        or after[0] - before[0] != initial["inserted"]
    ):
        raise RuntimeError("Demo reconciliation or replay check failed")
    peak_rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    peak_mib = peak_rss / (1048576 if platform.system() == "Darwin" else 1024)
    print(
        json.dumps(
            {
                "trial": args.trial,
                "host_process_peak_rss_mib": peak_mib,
                "source_orders": orders,
                "source_pings": pings,
                "before_rows": before[0],
                "after_rows": after[0],
                "initial": initial,
                "replay": replay,
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
