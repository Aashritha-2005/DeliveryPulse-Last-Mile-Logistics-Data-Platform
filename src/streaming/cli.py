"""Standalone producer and resumable consumer entry points for the local stream."""

import argparse
import json
from pathlib import Path

from src.streaming.config import load_kafka_settings
from src.streaming.consumer import consume
from src.streaming.demo import source_records
from src.streaming.producer import send


def main() -> None:
    """Publish a repeatable source slice or resume a bounded consumer group."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=("produce", "consume"))
    parser.add_argument("--count", type=int, default=500)
    parser.add_argument("--rate", type=float, default=500)
    parser.add_argument("--group", default="deliverypulse-local-v1")
    parser.add_argument("--raw-dir", type=Path, default=Path("data/raw"))
    args = parser.parse_args()
    if not 2 <= args.count <= 100000 or args.rate < 0:
        parser.error("count must be 2..100000 and rate must be non-negative")
    settings = load_kafka_settings()
    if args.operation == "consume":
        print(json.dumps(consume(settings, args.group, args.count), sort_keys=True))
    else:
        orders = max(1, args.count // 10)
        records = [
            (settings.orders_topic, p) for p in source_records(args.raw_dir, "order", 0, orders)
        ]
        records.extend(
            (settings.pings_topic, p)
            for p in source_records(args.raw_dir, "driver_ping", 0, args.count - orders)
        )
        print(f"Producer acknowledged: {send(settings, records, args.rate)}")


if __name__ == "__main__":
    main()
