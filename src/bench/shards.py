"""Benchmark datasets as shards of 50,000-order generator runs (`make bench-data`).

The generator holds a whole dataset in memory (about 0.6 GiB at 50,000 orders), so large sizes
are built from shards: shard k is a normal generator run with seed 41 + k (shard 1 is the default
seed 42). Each shard is converted to Parquet (orders and delivery events, contract columns as
text, plus ``row_id``) and its raw files are deleted right away. IDs get a shard prefix
(``S0001-``) so they stay unique across shards. A size of n x 50,000 orders is shards 1..n.

Before each shard the disk guard checks the projected bytes; a shard is written to a temporary
folder and renamed into place only when complete.
"""

import argparse
import logging
import sys
from collections.abc import Sequence
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from src.bench.disk import (
    BENCH_ROOT,
    SHARDS_DIR,
    TMP_DIR,
    DiskGuard,
    DiskGuardError,
    ShardSizes,
    budget_bytes,
    remove_tree,
)
from src.bench.spec import EVENTS_FILE, ORDERS_FILE, ROW_ID
from src.contract import SOURCE_COLUMNS, SOURCE_DELIVERY_EVENTS, SOURCE_ORDERS
from src.ingestion.generate_data import generate
from src.ingestion.generator_config import GeneratorConfig
from src.transformations.records import Batch, batch_folders, load_batch

logger = logging.getLogger("deliverypulse.bench.shards")

SHARD_ORDERS = 50_000
FIRST_SEED = 42
ROW_ID_STRIDE = 1_000_000_000  # row_id = shard * stride + position: unique across shards
# Columns that hold IDs, prefixed per shard (empty values stay empty).
PREFIXED = {
    SOURCE_ORDERS: ("order_id", "customer_id", "driver_id"),
    SOURCE_DELIVERY_EVENTS: ("event_id", "order_id", "driver_id"),
}
FILES = {SOURCE_ORDERS: ORDERS_FILE, SOURCE_DELIVERY_EVENTS: EVENTS_FILE}
# Measured on 2026-10-14 with one shard (allocated bytes on disk; see docs/benchmarks.md):
# input 17,301,504 (orders + delivery events Parquet), read copies 5,054,464 (single file
# 2,318,336 plus partitioned 2,736,128; they hold three columns only), raw files of one shard
# 85,183,532 while it is generated, rounded up here for the manifest and the folder overhead.
MEASURED_SHARD_SIZES = ShardSizes(
    input_bytes=17_301_504,
    read_copy_bytes=5_054_464,
    raw_transient_bytes=100_000_000,
)


def shard_seed(index: int) -> int:
    """The generator seed of shard ``index`` (1-based)."""
    return FIRST_SEED + index - 1


def shard_dir(root: Path, index: int) -> Path:
    """The folder of shard ``index``."""
    return root / SHARDS_DIR / f"shard-{index:04d}"


def shard_prefix(index: int) -> str:
    """The ID prefix of shard ``index``."""
    return f"S{index:04d}-"


def source_table(batch: Batch, source: str, index: int, first_row: int) -> pa.Table:
    """One batch's records of ``source`` as text columns, IDs prefixed, plus ``row_id``."""
    records = batch.records[source]
    prefix = shard_prefix(index)
    columns: dict[str, pa.Array] = {}
    for column in SOURCE_COLUMNS[source]:
        values = [record.fields.get(column, "") for record in records]
        if column in PREFIXED[source]:
            values = [f"{prefix}{value}" if value else value for value in values]
        columns[column] = pa.array(values, pa.string())
    start = index * ROW_ID_STRIDE + first_row
    columns[ROW_ID] = pa.array(range(start, start + len(records)), pa.int64())
    return pa.table(columns)


def convert_dataset(dataset_dir: Path, target: Path, index: int) -> dict[str, int]:
    """Write ``dataset_dir``'s orders and delivery events to ``target``; return rows per source."""
    target.mkdir(parents=True, exist_ok=True)
    writers: dict[str, pq.ParquetWriter] = {}
    rows = dict.fromkeys(FILES, 0)
    try:
        for folder in batch_folders(dataset_dir):
            batch = load_batch(folder)
            for source in FILES:
                table = source_table(batch, source, index, rows[source])
                if source not in writers:
                    writers[source] = pq.ParquetWriter(target / FILES[source], table.schema)
                writers[source].write_table(table)
                rows[source] += table.num_rows
            del batch
    finally:
        for writer in writers.values():
            writer.close()
    return rows


def build_shard(root: Path, index: int) -> dict[str, int]:
    """Generate shard ``index``, convert it, delete its raw files; return rows per source."""
    work = root / TMP_DIR / f"gen-{index:04d}"
    partial = shard_dir(root, index).with_name(f"shard-{index:04d}.partial")
    remove_tree(work)
    remove_tree(partial)
    try:
        config = GeneratorConfig.create(orders=SHARD_ORDERS, seed=shard_seed(index))
        result = generate(config, work / "raw", work / "ground_truth")
        rows = convert_dataset(result.raw_dir, partial, index)
        partial.rename(shard_dir(root, index))
    finally:
        remove_tree(work)
        remove_tree(partial)
    return rows


def ensure_shards(
    root: Path, count: int, guard: DiskGuard, sizes: ShardSizes = MEASURED_SHARD_SIZES
) -> list[Path]:
    """Build the missing shards 1..``count``; check the disk guard before and after each."""
    for index in range(1, count + 1):
        if shard_dir(root, index).is_dir():
            continue
        guard.check(sizes.input_bytes + sizes.raw_transient_bytes, f"shard {index}")
        rows = build_shard(root, index)
        guard.check(0, f"after shard {index}")
        logger.info("shard %d (seed %d): %s rows", index, shard_seed(index), rows)
    return [shard_dir(root, index) for index in range(1, count + 1)]


def main(argv: Sequence[str] | None = None) -> int:
    """Command-line entry point (`make bench-data SHARDS=n`)."""
    parser = argparse.ArgumentParser(description="Build benchmark shards (50,000 orders each).")
    parser.add_argument("--shards", type=int, required=True)
    parser.add_argument("--budget-gib", type=float, default=4.0)
    args = parser.parse_args(argv)
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    try:
        guard = DiskGuard(BENCH_ROOT, budget_bytes(args.budget_gib))
        shards = ensure_shards(BENCH_ROOT, args.shards, guard)
    except DiskGuardError as exc:
        print(f"STOPPED: {exc}", file=sys.stderr)
        return 1
    print(f"bench data: {len(shards)} shards, {guard.usage()} bytes under {BENCH_ROOT}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
