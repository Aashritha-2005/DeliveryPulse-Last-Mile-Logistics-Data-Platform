"""Process every batch of one raw dataset, in sequence, from fresh local state.

Usage (from the project root)::

    uv run python -m src.transformations.transform [--dataset NAME]

Reads ``data/raw/<dataset>/batch-<k>/`` (never modified) and writes
``data/processed/<dataset>/`` (batch outputs, local state, cleaned Parquet). The whole output is
built in a hidden temp folder and then swapped into place, so a re-run replaces it completely:
no appended rows, no leftover files.
"""

import argparse
import logging
import shutil
import sys
import tempfile
import time
from collections.abc import Sequence
from pathlib import Path

from src.contract import MAX_PLAUSIBLE_SPEED_KMH
from src.transformations.outputs import write_batch_outputs, write_cleaned
from src.transformations.pipeline import (
    DEFAULT_ORPHAN_MAX_AGE_BATCHES,
    BatchResult,
    TransformConfig,
    process_batch,
)
from src.transformations.records import BatchSchemaError, batch_folders, load_batch
from src.transformations.state import BatchSequenceError, LocalState
from src.utils.logging import configure_logging

logger = logging.getLogger("deliverypulse.transform")

DEFAULT_RAW_DIR = Path("data/raw")
DEFAULT_PROCESSED_DIR = Path("data/processed")
TEMP_PREFIX = ".transform-tmp-"
OLD_PREFIX = ".transform-old-"


class TransformError(RuntimeError):
    """The transform cannot run (for example, no or several datasets to choose from)."""


def resolve_dataset(raw_dir: Path, name: str | None) -> Path:
    """The dataset folder to process: ``name``, or the only dataset in ``raw_dir``."""
    if name:
        if "/" in name or ".." in name or name.startswith("."):
            raise TransformError(f"Invalid dataset name {name!r}.")
        folder = raw_dir / name
        if not folder.is_dir():
            raise TransformError(f"Dataset {folder} not found.")
        return folder
    candidates = sorted(
        path for path in raw_dir.iterdir() if path.is_dir() and not path.name.startswith(".")
    )
    if len(candidates) != 1:
        names = ", ".join(path.name for path in candidates) or "none"
        raise TransformError(
            f"Expected exactly one dataset in {raw_dir} (found: {names}); "
            "choose one with DATASET=<name>."
        )
    return candidates[0]


def _remove_own_leftovers(parent: Path) -> None:
    for entry in sorted(parent.iterdir()):
        own = entry.name.startswith((TEMP_PREFIX, OLD_PREFIX))
        if own and entry.is_dir() and not entry.is_symlink():
            logger.warning("Removing leftover folder from an interrupted transform: %s", entry)
            shutil.rmtree(entry)


def _swap_into_place(staging: Path, target: Path) -> None:
    """Replace ``target`` with ``staging`` (derived output only; raw data is never touched)."""
    if target.exists():
        old = Path(tempfile.mkdtemp(prefix=f"{OLD_PREFIX}{target.name}-", dir=target.parent))
        old.rmdir()
        target.rename(old)
        staging.rename(target)
        shutil.rmtree(old)
    else:
        staging.rename(target)


def run_transform(
    dataset_dir: Path, output_dir: Path, config: TransformConfig
) -> list[BatchResult]:
    """Process all batches of ``dataset_dir`` in order and write ``output_dir``."""
    folders = batch_folders(dataset_dir)
    if not folders:
        raise TransformError(f"No batch folders in {dataset_dir}.")
    output_dir.parent.mkdir(parents=True, exist_ok=True)
    _remove_own_leftovers(output_dir.parent)
    prefix = f"{TEMP_PREFIX}{output_dir.name}-"
    staging = Path(tempfile.mkdtemp(prefix=prefix, dir=output_dir.parent))
    try:
        state = LocalState()
        results: list[BatchResult] = []
        for folder in folders:
            started = time.perf_counter()
            batch = load_batch(folder)
            result = process_batch(batch, state, config)
            write_batch_outputs(staging / "batches" / folder.name, result)
            state.save(staging / "state")
            logger.info(
                "batch %d (%s) done in %.2f s",
                batch.sequence,
                batch.batch_id[:12],
                time.perf_counter() - started,
            )
            results.append(result)
        write_cleaned(staging / "cleaned", state)
        _swap_into_place(staging, output_dir)
    except BaseException:
        shutil.rmtree(staging, ignore_errors=True)
        raise
    return results


def main(argv: Sequence[str] | None = None) -> int:
    """Command-line entry point; returns the process exit code."""
    parser = argparse.ArgumentParser(description="Transform raw batches into cleaned Parquet.")
    parser.add_argument("--raw-dir", type=Path, default=DEFAULT_RAW_DIR)
    parser.add_argument("--processed-dir", type=Path, default=DEFAULT_PROCESSED_DIR)
    parser.add_argument("--dataset", default=None, help="dataset folder name in --raw-dir")
    parser.add_argument("--max-speed-kmh", type=float, default=MAX_PLAUSIBLE_SPEED_KMH)
    parser.add_argument(
        "--orphan-max-age-batches", type=int, default=DEFAULT_ORPHAN_MAX_AGE_BATCHES
    )
    args = parser.parse_args(argv)
    configure_logging()
    raw_dir: Path = args.raw_dir
    processed_dir: Path = args.processed_dir
    config = TransformConfig(
        max_speed_kmh=args.max_speed_kmh, orphan_max_age_batches=args.orphan_max_age_batches
    )
    started = time.perf_counter()
    try:
        dataset_dir = resolve_dataset(raw_dir, args.dataset)
        logger.info("transforming dataset %s", dataset_dir.name)
        results = run_transform(dataset_dir, processed_dir / dataset_dir.name, config)
    except (TransformError, BatchSchemaError, BatchSequenceError) as exc:
        logger.error("%s", exc)
        return 1
    logger.info(
        "transformed %d batches into %s in %.2f s",
        len(results),
        processed_dir / dataset_dir.name,
        time.perf_counter() - started,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
